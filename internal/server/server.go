// Package server wires HTTP, REST, SSE, and the embedded UI together.
// The router uses only the standard library and a small mux helper so the
// dependency surface stays at one (modernc.org/sqlite).
package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cyberanchor/frgmnt/internal/auth"
	"github.com/cyberanchor/frgmnt/internal/checker"
	"github.com/cyberanchor/frgmnt/internal/logbus"
	"github.com/cyberanchor/frgmnt/internal/store"
	"github.com/cyberanchor/frgmnt/internal/version"
	"github.com/cyberanchor/frgmnt/internal/worker"
)

// Server is the assembled HTTP application.
type Server struct {
	addr       string
	allowHosts []string // extra Host / Origin names (a domain in front of the panel)
	mgr        *worker.Manager
	store      *store.Store
	bus        *logbus.Bus
	staticFS   fs.FS
	keeper     *auth.Keeper // nil: no access tokens (tests)
	tlsCert    string       // PEM files for HTTPS; "" = plain HTTP
	tlsKey     string
	mux        *http.ServeMux
	handler    http.Handler // mux behind guard() and requireToken()
}

// Options bundles dependencies needed to assemble a Server.
type Options struct {
	Addr string
	// AllowHosts are host names (no port) the panel answers to besides the
	// loopback names, e.g. the domain of a reverse proxy in front of it.
	AllowHosts []string
	Manager    *worker.Manager
	Store      *store.Store
	Bus        *logbus.Bus
	StaticFS   fs.FS // already rooted at the static files (no "web/" prefix)
	// Auth, when set, requires an access token (a JWT signed with its key)
	// for every request except the login page. The web mode always sets
	// it; nil leaves the panel open (tests).
	Auth *auth.Keeper
	// TLSCert and TLSKey are PEM files; when set, Run serves HTTPS and
	// reads the files again when they change (certificate renewals).
	TLSCert, TLSKey string
}

// New assembles a Server but does NOT start listening.
func New(opt Options) *Server {
	s := &Server{
		addr:       opt.Addr,
		allowHosts: opt.AllowHosts,
		mgr:        opt.Manager,
		store:      opt.Store,
		bus:        opt.Bus,
		staticFS:   opt.StaticFS,
		keeper:     opt.Auth,
		tlsCert:    opt.TLSCert,
		tlsKey:     opt.TLSKey,
		mux:        http.NewServeMux(),
	}
	s.routes()
	s.handler = s.guard(s.requireToken(s.mux))
	return s
}

// -----------------------------------------------------------------------------
// Access tokens
// -----------------------------------------------------------------------------

// cookieName holds the token after a login in the browser.
const cookieName = "frgmnt_token"

// requireToken lets a request through only with a valid access token, from
// "Authorization: Bearer <token>" (scripts, curl) or from the cookie the
// login page sets (the panel, its EventSource streams and downloads).
// Without one the API answers 401 and the UI redirects to /login. The
// panel had no login at all: anyone who could reach it could use it.
func (s *Server) requireToken(next http.Handler) http.Handler {
	if s.keeper == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login":
			s.handleLogin(w, r)
			return
		case "/logout":
			s.handleLogout(w, r)
			return
		}
		if tok := tokenFrom(r); tok != "" {
			if _, err := s.keeper.Verify(tok); err == nil {
				next.ServeHTTP(w, r)
				return
			}
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="frgmnt"`)
			writeError(w, http.StatusUnauthorized, "access token required: log in at /login or send Authorization: Bearer <token>")
			return
		}
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	})
}

func tokenFrom(r *http.Request) string {
	if h := r.Header.Get("Authorization"); len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return strings.TrimSpace(h[7:])
	}
	if c, err := r.Cookie(cookieName); err == nil {
		return c.Value
	}
	return ""
}

// secureRequest reports whether the browser talks HTTPS to the panel,
// directly or through a TLS reverse proxy.
func secureRequest(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

// clientAddr is the peer address for log lines, with X-Forwarded-For when a
// proxy sets it.
func clientAddr(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return r.RemoteAddr + " (X-Forwarded-For " + xff + ")"
	}
	return r.RemoteAddr
}

// minimalLogin is used when the static files have no login.html (tests).
const minimalLogin = `<!DOCTYPE html><html><head><meta charset="UTF-8"><title>access</title></head><body>
<!--error--><form method="post" action="/login"><input type="password" name="token" required>
<button type="submit">Enter</button></form></body></html>`

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if tok := tokenFrom(r); tok != "" {
			if _, err := s.keeper.Verify(tok); err == nil {
				http.Redirect(w, r, "/", http.StatusSeeOther)
				return
			}
		}
		page := []byte(minimalLogin)
		if s.staticFS != nil {
			if data, err := fs.ReadFile(s.staticFS, "login.html"); err == nil {
				page = data
			}
		}
		if r.URL.Query().Get("failed") != "" {
			page = bytes.Replace(page, []byte("<!--error-->"), []byte(`<div class="error">That token is not valid: `+
				`it is mistyped, expired, or its key was rotated.</div>`), 1)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Frame-Options", "DENY")
		_, _ = w.Write(page)
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		tok := ""
		if err := r.ParseForm(); err == nil {
			tok = strings.TrimSpace(r.PostForm.Get("token"))
		}
		c, err := s.keeper.Verify(tok)
		if err != nil {
			s.bus.Publish(logbus.LevelWarn, "", "login refused ("+err.Error()+") from "+clientAddr(r))
			http.Redirect(w, r, "/login?failed=1", http.StatusSeeOther)
			return
		}
		ck := &http.Cookie{Name: cookieName, Value: tok, Path: "/", HttpOnly: true,
			SameSite: http.SameSiteLaxMode, Secure: secureRequest(r)}
		if exp := c.ExpiresAt(); !exp.IsZero() {
			ck.Expires = exp
		} else {
			ck.MaxAge = 365 * 24 * 3600
		}
		http.SetCookie(w, ck)
		name := c.Name
		if name == "" {
			name = c.ID
		}
		s.bus.Publish(logbus.LevelInfo, "", "login with token "+strconv.Quote(name)+" from "+clientAddr(r))
		http.Redirect(w, r, "/", http.StatusSeeOther)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", MaxAge: -1, HttpOnly: true,
		SameSite: http.SameSiteLaxMode, Secure: secureRequest(r)})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// Handler returns the underlying http.Handler (useful for tests).
func (s *Server) Handler() http.Handler { return s.handler }

// guard rejects requests that a web page on another site can make through
// the user's browser. The API has no login, so without it any page open in
// the same browser could create, start or delete workers with a "simple"
// cross-site POST (text/plain needs no CORS preflight), and a full start
// wipes the worker's previous results.
//
//   - State-changing requests must come from the panel itself: a browser
//     marks cross-site requests with Sec-Fetch-Site and Origin. Clients
//     that send neither (curl, scripts) are not affected.
//   - When the panel listens on a loopback address, the Host header must
//     be a loopback name as well. This stops DNS rebinding, where a hostile
//     domain resolves to 127.0.0.1 and its pages become "same-origin".
//   - Names in AllowHosts (-allow-host) pass both checks: behind a reverse
//     proxy the browser's Host and Origin are the proxy's domain, which
//     used to get a 403. With AllowHosts set, the Host check also applies
//     to a panel listening on a public address.
func (s *Server) guard(next http.Handler) http.Handler {
	checkHost := isLoopbackAddr(s.addr) || len(s.allowHosts) > 0
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if checkHost && !s.hostAllowed(r.Host) {
			writeError(w, http.StatusForbidden, "host "+r.Host+" is not allowed (add it with -allow-host)")
			return
		}
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
		default:
			if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
				writeError(w, http.StatusForbidden, "cross-site request blocked")
				return
			}
			if origin := r.Header.Get("Origin"); origin != "" && !sameOrigin(origin, r.Host) && !s.originAllowed(origin) {
				writeError(w, http.StatusForbidden, "cross-origin request blocked")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// hostAllowed reports whether a Host header names the panel: a loopback
// name or one of AllowHosts (any port).
func (s *Server) hostAllowed(hostport string) bool {
	if isLoopbackHost(hostport) {
		return true
	}
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	host = strings.TrimSuffix(host, ".")
	for _, h := range s.allowHosts {
		if strings.EqualFold(h, host) {
			return true
		}
	}
	return false
}

// originAllowed reports whether an Origin is one of AllowHosts. Behind a
// proxy that sends its own Host (nginx does by default), Origin is the
// only place where the panel's domain shows.
func (s *Server) originAllowed(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || isLoopbackHost(u.Host) {
		return false
	}
	return s.hostAllowed(u.Host)
}

// isLoopbackAddr reports whether a listen address only accepts local
// connections ("127.0.0.1:8080", "localhost:8080", "[::1]:8080").
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return isLoopbackName(host)
}

func isLoopbackHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	return isLoopbackName(host)
}

func isLoopbackName(host string) bool {
	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func sameOrigin(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false // includes Origin: null
	}
	return strings.EqualFold(u.Host, host)
}

// Run blocks until ctx is cancelled. It performs a graceful shutdown when
// the context is done.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.addr,
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
		// SSE responses are long-lived; do NOT set a write deadline.
	}
	scheme := "HTTP"
	if s.tlsCert != "" || s.tlsKey != "" {
		reloader, err := newCertReloader(s.tlsCert, s.tlsKey, func(err error) {
			s.bus.Publish(logbus.LevelWarn, "", "TLS certificate not reloaded, the old one stays in use: "+err.Error())
		})
		if err != nil {
			return fmt.Errorf("TLS certificate: %w", err)
		}
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: reloader.getCertificate}
		scheme = "HTTPS"
	} else if !isLoopbackAddr(s.addr) {
		s.bus.Publish(logbus.LevelWarn, "", "listening on "+s.addr+" without TLS: access tokens travel in clear text; "+
			"use -tls-cert/-tls-key or a TLS reverse proxy")
	}
	errCh := make(chan error, 1)
	go func() {
		s.bus.Publish(logbus.LevelInfo, "", scheme+" server listening on "+s.addr)
		var err error
		if srv.TLSConfig != nil {
			err = srv.ListenAndServeTLS("", "")
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	}
}

// -----------------------------------------------------------------------------
// Routing
// -----------------------------------------------------------------------------

func (s *Server) routes() {
	// API
	s.mux.HandleFunc("/api/health", s.handleHealth)
	s.mux.HandleFunc("/api/settings", s.handleSettings)
	s.mux.HandleFunc("/api/workers", s.handleWorkersCollection)
	s.mux.HandleFunc("/api/workers/", s.handleWorkerItem)
	s.mux.HandleFunc("/api/workers-bulk/start", s.handleBulkStart)
	s.mux.HandleFunc("/api/workers-bulk/stop", s.handleBulkStop)
	s.mux.HandleFunc("/api/logs", s.handleLogs)
	s.mux.HandleFunc("/api/logs/stream", s.handleLogStream)
	// Unknown API paths get a JSON error like the rest of the API (they
	// fell through to the UI handler's plain-text 404).
	s.mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "no such API endpoint: "+r.URL.Path)
	})

	// Static UI (embedded). The provided FS is already rooted at the
	// asset directory; no fs.Sub here.
	if s.staticFS != nil {
		fileServer := http.FileServer(noDirFS{http.FS(s.staticFS)})
		s.mux.Handle("/static/", http.StripPrefix("/static/", fileServer))
		s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			// fs.ReadFile (not http.ServeFileFS) keeps the module
			// buildable on Go 1.21, which the README/CI still target.
			data, err := fs.ReadFile(s.staticFS, "index.html")
			if err != nil {
				http.Error(w, "index.html not found", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			_, _ = w.Write(data)
		})
	}
}

// noDirFS serves files only. Opening a directory fails, so /static/ is a
// 404 instead of a listing of the embedded files.
type noDirFS struct{ http.FileSystem }

func (f noDirFS) Open(name string) (http.File, error) {
	file, err := f.FileSystem.Open(name)
	if err != nil {
		return nil, err
	}
	if st, err := file.Stat(); err != nil || st.IsDir() {
		file.Close()
		return nil, fs.ErrNotExist
	}
	return file, nil
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func readJSON(r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 50<<20) // 50 MB cap
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

// -----------------------------------------------------------------------------
// /api/health
// -----------------------------------------------------------------------------

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"time":      time.Now(),
		"workers":   len(s.mgr.List()),
		"log_level": s.bus.Level().String(),
		"version":   version.Version,
	})
}

// -----------------------------------------------------------------------------
// /api/settings
// -----------------------------------------------------------------------------

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.store.GetSettings())
	case http.MethodPut:
		// Decode onto the current settings: a PUT with some fields changes
		// only those. It used to decode onto zero values, so {"theme":
		// "light"} also set default_retries, default_rate_rps, ... to 0.
		v := s.store.GetSettings()
		if err := readJSON(r, &v); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		// "trace" was a log level of its own: every step of every request
		// in the log. That detail is in a worker's trace file now ("Write a
		// trace"), and the old value means debug.
		if v.LogLevel == "trace" {
			v.LogLevel = "debug"
		}
		if err := validateSettings(v); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.store.PutSettings(v); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		// Apply the new log level live.
		s.bus.SetLevel(logbus.ParseLevel(v.LogLevel))
		s.bus.Publish(logbus.LevelInfo, "", "settings updated (log_level="+v.LogLevel+", theme="+v.Theme+")")
		writeJSON(w, http.StatusOK, v)
	default:
		methodNotAllowed(w, "GET, PUT")
	}
}

// validateSettings rejects values a new worker could not use. Without it,
// default_rate_rps -5 was saved, and every later POST /api/workers failed
// with 400 because the defaults it starts from were out of range.
func validateSettings(v store.Settings) error {
	check := func(name string, val, lo, hi int) error {
		if val < lo || val > hi {
			return fmt.Errorf("%s must be between %d and %d, got %d", name, lo, hi, val)
		}
		return nil
	}
	if v.Theme != "light" && v.Theme != "dark" {
		return fmt.Errorf(`theme must be "light" or "dark", got %q`, v.Theme)
	}
	switch v.LogLevel {
	case "error", "warn", "info", "debug":
	default:
		return fmt.Errorf("log_level must be error, warn, info or debug, got %q", v.LogLevel)
	}
	for _, err := range []error{
		check("default_workers", v.DefaultWorkers, 1, checker.MaxWorkers),
		check("default_rate_rps", v.DefaultRateRPS, 0, checker.MaxRateRPS),
		check("default_retries", v.DefaultRetries, 0, checker.MaxRetries),
		check("default_timeout_s", v.DefaultTimeout, 1, 600),
		check("history_limit_max", v.HistoryLimitMax, 1, 100000),
	} {
		if err != nil {
			return err
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// /api/workers (collection)
// -----------------------------------------------------------------------------

type workerView struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Status    string         `json:"status"`
	Config    checker.Config `json:"config"`
	Total     int64          `json:"total"`
	Stats     checker.Stats  `json:"stats"`
	CreatedAt time.Time      `json:"created_at"`
	StartedAt time.Time      `json:"started_at,omitempty"`
}

func toView(w *worker.Worker) workerView {
	return workerView{
		ID:        w.ID(),
		Name:      w.Name(),
		Status:    string(w.Status()),
		Config:    w.Config(),
		Total:     w.Total(),
		Stats:     w.Stats(),
		CreatedAt: w.CreatedAt(),
		StartedAt: w.StartedAt(),
	}
}

func (s *Server) handleWorkersCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		items := s.mgr.List()
		out := make([]workerView, 0, len(items))
		for _, it := range items {
			out = append(out, toView(it))
		}
		writeJSON(w, http.StatusOK, out)
	case http.MethodPost:
		// Decode onto the defaults: fields left out of the request keep
		// the values from Settings (default_retries, default_rate_rps, ...)
		// instead of becoming zero.
		def := defaultConfigFromSettings(s.store.GetSettings())
		body := struct {
			Name   string         `json:"name"`
			Config checker.Config `json:"config"`
		}{Config: def}
		if err := readJSON(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if strings.TrimSpace(body.Name) == "" {
			writeError(w, http.StatusBadRequest, "name is required")
			return
		}
		// Check before the defaults fill in the zeros: mergeConfig used to
		// turn a negative value (workers: -1) into the default silently.
		if err := body.Config.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := checkCacheDir(body.Config.CacheDir, ""); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		body.Config = mergeConfig(body.Config, def)
		w2, err := s.mgr.Create(r.Context(), body.Name, body.Config)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, toView(w2))
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

// defaultConfigFromSettings returns the config a new worker starts from.
// Values out of range (settings.json can be edited by hand) are ignored,
// so they cannot make every worker creation fail.
func defaultConfigFromSettings(s store.Settings) checker.Config {
	def := checker.DefaultConfig()
	if s.DefaultWorkers > 0 && s.DefaultWorkers <= checker.MaxWorkers {
		def.Workers = s.DefaultWorkers
	}
	if s.DefaultRateRPS >= 0 && s.DefaultRateRPS <= checker.MaxRateRPS {
		def.RateRPS = s.DefaultRateRPS
	}
	if s.DefaultRetries >= 0 && s.DefaultRetries <= checker.MaxRetries {
		def.Retries = s.DefaultRetries
	}
	if s.DefaultTimeout > 0 {
		def.Timeout = time.Duration(s.DefaultTimeout) * time.Second
	}
	return def
}

// checkCacheDir refuses a page-cache directory set through the API. The
// directory comes from the panel's -cache-dir flag (default
// <data>/pagecache); a per-worker cache_dir let anyone with access to the
// API make the panel create directories and write files anywhere it may
// write. A value a worker already has (set by an older version) may stay.
func checkCacheDir(dir, current string) error {
	if dir == "" || dir == current {
		return nil
	}
	return errors.New("cache_dir cannot be set through the API; the page cache directory is set for the whole panel with -cache-dir")
}

// mergeConfig fills the fields that are zero ("not set") from def. The
// caller has rejected negative values with Validate first.
func mergeConfig(in, def checker.Config) checker.Config {
	if in.Workers == 0 {
		in.Workers = def.Workers
	}
	if in.MaxConns == 0 {
		in.MaxConns = def.MaxConns
	}
	if in.RetryDelay == 0 {
		in.RetryDelay = def.RetryDelay
	}
	if in.RLWaitMax == 0 {
		in.RLWaitMax = def.RLWaitMax
	}
	if in.Timeout == 0 {
		in.Timeout = def.Timeout
	}
	if in.ReadLimit == 0 {
		in.ReadLimit = def.ReadLimit
	}
	if in.UserAgent == "" {
		in.UserAgent = def.UserAgent
	}
	return in
}

// -----------------------------------------------------------------------------
// /api/workers/{id}/... (item + actions)
// -----------------------------------------------------------------------------

func (s *Server) handleWorkerItem(w http.ResponseWriter, r *http.Request) {
	// /api/workers/{id}[/action]
	rest := strings.TrimPrefix(r.URL.Path, "/api/workers/")
	if rest == "" {
		writeError(w, http.StatusNotFound, "missing worker id")
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	switch action {
	case "":
		s.handleWorkerCRUD(w, r, id)
	case "input":
		s.handleWorkerInput(w, r, id)
	case "input/upload":
		s.handleWorkerInputUpload(w, r, id)
	case "start":
		s.handleWorkerStart(w, r, id)
	case "stop":
		s.handleWorkerStop(w, r, id)
	case "clear":
		s.handleWorkerClear(w, r, id)
	case "results":
		s.handleWorkerResults(w, r, id)
	case "results/download":
		s.handleWorkerResultsDownload(w, r, id)
	case "trace":
		s.handleWorkerTrace(w, r, id)
	case "events":
		s.handleWorkerEvents(w, r, id)
	default:
		writeError(w, http.StatusNotFound, "unknown action: "+action)
	}
}

func (s *Server) handleWorkerCRUD(w http.ResponseWriter, r *http.Request, id string) {
	switch r.Method {
	case http.MethodGet:
		w2, err := s.mgr.Get(id)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, toView(w2))
	case http.MethodPatch, http.MethodPut:
		existing, err := s.mgr.Get(id)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		// A partial update: decode onto the current values, so fields that
		// are not in the request (retries, rate_rps, insecure_tls, cache
		// settings, ...) stay as they are.
		body := struct {
			Name   string         `json:"name"`
			Config checker.Config `json:"config"`
		}{Name: existing.Name(), Config: existing.Config()}
		if err := readJSON(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if strings.TrimSpace(body.Name) == "" {
			body.Name = existing.Name()
		}
		if err := body.Config.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := checkCacheDir(body.Config.CacheDir, existing.Config().CacheDir); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		body.Config = mergeConfig(body.Config, existing.Config())
		w2, err := s.mgr.Update(r.Context(), id, body.Name, body.Config)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, toView(w2))
	case http.MethodDelete:
		if err := s.mgr.Delete(r.Context(), id); err != nil {
			if errors.Is(err, worker.ErrNotFound) {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	default:
		methodNotAllowed(w, "GET, PATCH, DELETE")
	}
}

func (s *Server) handleWorkerInput(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	var body struct {
		Text string `json:"text"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	list, rejected, _ := checker.CollectNames(strings.NewReader(body.Text))
	body.Text = "" // the list holds the names now
	s.storeInput(w, r, id, list, rejected)
}

// storeInput replaces the worker's list with list and answers with the
// counts. The names are parsed before this point, so the database is only
// held for the write itself and never while a slow client is still
// sending the file.
func (s *Server) storeInput(w http.ResponseWriter, r *http.Request, id string, list *checker.NameList, rejected int) {
	n, err := s.mgr.SetInputFrom(r.Context(), id, list.Each)
	if err != nil {
		if errors.Is(err, worker.ErrWorkerBusy) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": n,
		"rejected": rejected,
	})
}

func (s *Server) handleWorkerInputUpload(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)
	// The file part is read straight from the request: ParseMultipartForm
	// copied an upload over 10 MB into a temporary file first, and the
	// names then went into a []string plus a map (about 170 MB per million
	// names); CollectNames keeps them in a compact list instead.
	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "parse multipart: "+err.Error())
		return
	}
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "missing file: no \"file\" field in the upload")
			return
		}
		if err != nil {
			writeUploadError(w, "parse multipart", err)
			return
		}
		if part.FormName() != "file" {
			_ = part.Close() // another form field: skip it
			continue
		}
		list, rejected, err := checker.CollectNames(part)
		_ = part.Close()
		if err != nil {
			writeUploadError(w, "parse file", err)
			return
		}
		s.storeInput(w, r, id, list, rejected)
		return
	}
}

// maxUploadBytes caps a list upload (about 10 million names). A variable
// so tests can lower it.
var maxUploadBytes int64 = 100 << 20

// writeUploadError answers a failed upload: 413 when the file is over
// maxUploadBytes, 400 otherwise.
func writeUploadError(w http.ResponseWriter, what string, err error) {
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("%s: the file is larger than %d MB", what, maxUploadBytes>>20))
		return
	}
	writeError(w, http.StatusBadRequest, what+": "+err.Error())
}

func (s *Server) handleWorkerStart(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	resume := r.URL.Query().Get("resume") == "true" || r.URL.Query().Get("resume") == "1"
	if err := s.mgr.Start(r.Context(), id, worker.StartOptions{Resume: resume}); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, worker.ErrNotFound) {
			status = http.StatusNotFound
		} else if errors.Is(err, worker.ErrNotReady) || errors.Is(err, worker.ErrWorkerBusy) {
			status = http.StatusConflict
		}
		writeError(w, status, err.Error())
		return
	}
	w2, _ := s.mgr.Get(id)
	writeJSON(w, http.StatusOK, toView(w2))
}

// handleBulkStart continues every worker that has unchecked names: a new
// worker starts at its first name, a stopped one where it stopped. It never
// starts a worker from scratch, so no results are deleted, and a finished
// worker is left as it is. It used to start every ready, done and error
// worker fresh, which wiped their results (a stopped worker is "ready").
func (s *Server) handleBulkStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	started := 0
	skipped := 0
	for _, item := range s.mgr.List() {
		switch item.Status() {
		case worker.StatusReady, worker.StatusDone, worker.StatusError:
		default:
			skipped++
			continue
		}
		if n, err := s.mgr.Pending(r.Context(), item.ID()); err != nil || n == 0 {
			skipped++
			continue
		}
		if err := s.mgr.Start(r.Context(), item.ID(), worker.StartOptions{Resume: true}); err != nil {
			skipped++
			continue
		}
		started++
	}
	writeJSON(w, http.StatusOK, map[string]int{"started": started, "skipped": skipped})
}

func (s *Server) handleBulkStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	stopped := 0
	for _, item := range s.mgr.List() {
		if item.Status() != "running" {
			continue
		}
		if err := s.mgr.Stop(item.ID()); err == nil {
			stopped++
		}
	}
	writeJSON(w, http.StatusOK, map[string]int{"stopped": stopped})
}

func (s *Server) handleWorkerStop(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	if err := s.mgr.Stop(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w2, _ := s.mgr.Get(id)
	writeJSON(w, http.StatusOK, toView(w2))
}

func (s *Server) handleWorkerClear(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	if err := s.mgr.ClearResults(r.Context(), id); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, worker.ErrWorkerBusy) {
			status = http.StatusConflict
		} else if errors.Is(err, worker.ErrNotFound) {
			status = http.StatusNotFound
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// maxResultsPage caps ?limit= of the results list: any limit used to be
// read into memory and sent in one response.
const maxResultsPage = 5000

func (s *Server) handleWorkerResults(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 200
	}
	if limit > maxResultsPage {
		limit = maxResultsPage // page with ?after=; the download has everything
	}
	rs, err := s.store.ListResults(r.Context(), id, after, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if rs == nil {
		rs = []store.Result{} // "results": [] rather than null
	}
	counts, _ := s.store.CountByStatus(r.Context(), id)
	writeJSON(w, http.StatusOK, map[string]any{
		"results": rs,
		"counts":  counts,
	})
}

// handleWorkerTrace downloads (GET) or deletes (DELETE) the structured
// trace of a worker's runs, written while the worker's config has trace
// on: gzip JSON Lines, the older segment first when the trace has kept
// only its tail (the two gzip files in a row read as one stream; a trace
// from 2.1.0 is served as the plain file it is). A trace that is being
// written can be downloaded up to its last flush (every second); it
// cannot be deleted while the worker runs.
func (s *Server) handleWorkerTrace(w http.ResponseWriter, r *http.Request, id string) {
	wk, err := s.mgr.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	switch r.Method {
	case http.MethodGet:
		type part struct {
			f    *os.File
			size int64
		}
		var parts []part
		defer func() {
			for _, p := range parts {
				p.f.Close()
			}
		}()
		var total int64
		gz := false
		for _, path := range s.mgr.TraceFiles(id) {
			f, err := os.Open(path)
			if err != nil {
				continue
			}
			fi, err := f.Stat()
			if err != nil {
				f.Close()
				continue
			}
			// The size now: the current file grows while a run writes it,
			// and Content-Length must hold.
			parts = append(parts, part{f, fi.Size()})
			total += fi.Size()
			gz = gz || strings.HasSuffix(path, ".gz")
		}
		if len(parts) == 0 {
			writeError(w, http.StatusNotFound,
				"no trace for this worker: turn on \"Write a trace\" in its settings and run it")
			return
		}
		name, ctype := safeFilename(wk.Name())+"-trace.jsonl", "application/x-ndjson"
		if gz {
			name, ctype = name+".gz", "application/gzip"
		}
		w.Header().Set("Content-Type", ctype)
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, name))
		w.Header().Set("Content-Length", strconv.FormatInt(total, 10))
		for _, p := range parts {
			if _, err := io.CopyN(w, p.f, p.size); err != nil {
				return // the client went away
			}
		}
	case http.MethodDelete:
		if wk.Status() == worker.StatusRunning {
			writeError(w, http.StatusConflict, "worker is running; stop it before deleting its trace")
			return
		}
		if err := s.mgr.RemoveTrace(id); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleWorkerResultsDownload(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	w2, err := s.mgr.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-results.txt"`, safeFilename(w2.Name())))
	const batch = 1000
	var after int64
	for {
		rs, err := s.store.ListResults(r.Context(), id, after, batch)
		if err != nil {
			fmt.Fprintf(w, "# error: %v\n", err)
			return
		}
		if len(rs) == 0 {
			return
		}
		for _, row := range rs {
			if row.Error != "" {
				fmt.Fprintf(w, "@%s | %s (%s)\n", row.Username, row.Status, row.Error)
			} else {
				fmt.Fprintf(w, "@%s | %s\n", row.Username, row.Status)
			}
			if row.ID > after {
				after = row.ID
			}
		}
	}
}

func safeFilename(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_':
			out = append(out, r)
		case r == ' ':
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return "worker"
	}
	return string(out)
}

// -----------------------------------------------------------------------------
// SSE: per-worker events
// -----------------------------------------------------------------------------

func (s *Server) handleWorkerEvents(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if _, err := s.mgr.Get(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	events, cancel := s.bus.Subscribe(id, 256)
	defer cancel()

	// Send a hello so EventSource fires `open`.
	fmt.Fprintf(w, "event: hello\ndata: {\"worker_id\":\"%s\"}\n\n", id)
	flusher.Flush()

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()
	pollTicker := time.NewTicker(1 * time.Second)
	defer pollTicker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-events:
			if !ok {
				return
			}
			payload, _ := json.Marshal(e)
			fmt.Fprintf(w, "event: log\ndata: %s\n\n", payload)
			flusher.Flush()
		case <-pollTicker.C:
			// Periodic stats push so the UI tile is always up-to-date even
			// when no log events are flowing (e.g. between batches).
			if w2, err := s.mgr.Get(id); err == nil {
				stats := w2.Stats()
				payload, _ := json.Marshal(map[string]any{
					"status": string(w2.Status()),
					"total":  w2.Total(),
					"stats":  stats,
				})
				fmt.Fprintf(w, "event: stats\ndata: %s\n\n", payload)
				flusher.Flush()
			}
		case <-keepalive.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// -----------------------------------------------------------------------------
// /api/logs and /api/logs/stream
// -----------------------------------------------------------------------------

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 200
	}
	topic := r.URL.Query().Get("topic")
	entries := s.bus.Recent(limit, topic)
	writeJSON(w, http.StatusOK, entries)
}

func (s *Server) handleLogStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	topic := r.URL.Query().Get("topic")
	events, cancel := s.bus.Subscribe(topic, 256)
	defer cancel()

	fmt.Fprintf(w, "event: hello\ndata: {}\n\n")
	flusher.Flush()

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-events:
			if !ok {
				return
			}
			payload, _ := json.Marshal(e)
			fmt.Fprintf(w, "event: log\ndata: %s\n\n", payload)
			flusher.Flush()
		case <-keepalive.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}
