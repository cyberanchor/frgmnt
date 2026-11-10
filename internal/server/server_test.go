package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/cyberanchor/frgmnt/internal/auth"
	"github.com/cyberanchor/frgmnt/internal/checker"
	"github.com/cyberanchor/frgmnt/internal/logbus"
	"github.com/cyberanchor/frgmnt/internal/store"
	"github.com/cyberanchor/frgmnt/internal/worker"
)

// fakeFragment answers every name with "free" (302 to /?query=) and counts
// the requests per name. Names starting with "slow" are answered after
// delay, so a run can be stopped half way.
type fakeFragment struct {
	mu   sync.Mutex
	hits map[string]int
}

func newFakeFragment(t *testing.T, delay time.Duration) *fakeFragment {
	t.Helper()
	f := &fakeFragment{hits: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		f.mu.Lock()
		f.hits[name]++
		f.mu.Unlock()
		if delay > 0 && strings.HasPrefix(name, "slow") {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	}))
	old := checker.FragmentURLTemplate
	checker.FragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() {
		checker.FragmentURLTemplate = old
		srv.Close()
	})
	return f
}

func (f *fakeFragment) hitsFor(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[name]
}

// seqNames returns prefix000, prefix001, ...
func seqNames(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%03d", prefix, i)
	}
	return out
}

// newWorkerWithInput creates a worker through the API and loads names.
func newWorkerWithInput(t *testing.T, s *Server, name, config string, names []string) string {
	t.Helper()
	rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"`+name+`","config":`+config+`}`, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create %s: %d %s", name, rec.Code, rec.Body.String())
	}
	id := decodeView(t, rec).ID
	body, _ := json.Marshal(map[string]string{"text": strings.Join(names, "\n")})
	if rec := do(t, s, "POST", "/api/workers/"+id+"/input", "application/json", string(body), nil); rec.Code != http.StatusOK {
		t.Fatalf("input %s: %d %s", name, rec.Code, rec.Body.String())
	}
	return id
}

// waitStopped waits until the worker is no longer running.
func waitStopped(t *testing.T, s *Server, id string) worker.Status {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		w, err := s.mgr.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if st := w.Status(); st != worker.StatusRunning {
			return st
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("worker %s still running", id)
	return ""
}

// resultNames returns the usernames of the stored results of a worker.
func resultNames(t *testing.T, st *store.Store, id string) []string {
	t.Helper()
	rs, err := st.ListResults(context.Background(), id, 0, 100000)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Username)
	}
	return out
}

func newTestServer(t *testing.T) (*Server, *store.Store) {
	return newTestServerWith(t, nil)
}

// newTestServerWith builds a test server; edit can change the Options.
func newTestServerWith(t *testing.T, edit func(*Options)) (*Server, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bus := logbus.New(100, logbus.LevelInfo)
	mgr := worker.New(st, bus)
	mgr.ConfigureCache("", 0, 0, true)
	t.Cleanup(func() { mgr.CloseAll(); bus.Close(); _ = st.Close() })
	opt := Options{Addr: "127.0.0.1:0", Manager: mgr, Store: st, Bus: bus}
	if edit != nil {
		edit(&opt)
	}
	return New(opt), st
}

// do sends one request to the server's handler. ctype "" sends no
// Content-Type header.
func do(t *testing.T, s *Server, method, path, ctype, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Host = "127.0.0.1:8080" // the panel's own address, as a browser sends it
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	for k, v := range hdr {
		if k == "Host" {
			req.Host = v
			continue
		}
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func decodeView(t *testing.T, rec *httptest.ResponseRecorder) workerView {
	t.Helper()
	var v workerView
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

// Config values were not range-checked: read_limit=1<<62 from the API
// crashed the process on the first check, workers=100000000 started a
// hundred million goroutines.
func TestRejectsOutOfRangeConfig(t *testing.T) {
	s, _ := newTestServer(t)
	bad := []string{
		`{"read_limit":4611686018427387904}`,
		`{"rate_rps":4611686018427387904}`,
		`{"workers":100000000}`,
		`{"retries":-1}`,
		// Negative values were silently replaced by the defaults
		// (workers -1 became 50) instead of being rejected.
		`{"workers":-1}`,
		`{"max_conns":-3}`,
		`{"read_limit":-1}`,
		`{"timeout_ns":-1}`,
		`{"retry_delay_ns":-1}`,
		`{"timeout_ns":3600000000000}`,
	}
	for _, cfg := range bad {
		rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"w","config":`+cfg+`}`, nil)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("POST config %s -> %d %s, want 400", cfg, rec.Code, rec.Body.String())
		}
	}
	rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"ok","config":{}}`, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	id := decodeView(t, rec).ID
	for _, cfg := range bad {
		rec := do(t, s, "PATCH", "/api/workers/"+id, "application/json", `{"config":`+cfg+`}`, nil)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("PATCH config %s -> %d %s, want 400", cfg, rec.Code, rec.Body.String())
		}
	}
}

// Any page open in the same browser could drive the API: a "simple"
// cross-site POST (text/plain, no CORS preflight) created workers, and
// POST /api/workers-bulk/start restarted every finished worker, which wipes
// its previous results.
func TestCrossSiteRequestsRejected(t *testing.T) {
	s, _ := newTestServer(t)
	cases := []struct {
		path string
		hdr  map[string]string
	}{
		{"/api/workers", map[string]string{"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"}},
		{"/api/workers", map[string]string{"Origin": "https://evil.example"}},
		{"/api/workers", map[string]string{"Sec-Fetch-Site": "cross-site"}},
		{"/api/workers", map[string]string{"Origin": "null"}},
		{"/api/workers-bulk/start", map[string]string{"Origin": "http://127.0.0.1:3000", "Sec-Fetch-Site": "same-site"}},
	}
	for _, c := range cases {
		rec := do(t, s, "POST", c.path, "text/plain;charset=UTF-8", `{"name":"from-another-site"}`, c.hdr)
		if rec.Code != http.StatusForbidden {
			t.Errorf("POST %s with %v -> %d %s, want 403", c.path, c.hdr, rec.Code, rec.Body.String())
		}
	}
	if rec := do(t, s, "GET", "/api/workers", "", "", nil); rec.Body.String() != "[]\n" {
		t.Errorf("workers after the blocked requests: %s", rec.Body.String())
	}
}

func TestSameOriginAndScriptRequestsAllowed(t *testing.T) {
	s, _ := newTestServer(t)
	for _, hdr := range []map[string]string{
		{"Origin": "http://127.0.0.1:8080", "Sec-Fetch-Site": "same-origin"}, // the panel
		nil, // curl / scripts send neither header
		{"Host": "localhost:9000", "Origin": "http://localhost:9000"}, // ssh -L 9000:127.0.0.1:8080
	} {
		rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"w"}`, hdr)
		if rec.Code != http.StatusCreated {
			t.Errorf("POST with %v -> %d %s, want 201", hdr, rec.Code, rec.Body.String())
		}
	}
}

// DNS rebinding: a hostile domain that resolves to 127.0.0.1 is
// "same-origin" with itself, so only the Host header gives it away.
func TestRebindingHostRejected(t *testing.T) {
	s, _ := newTestServer(t)
	rec := do(t, s, "GET", "/api/workers", "", "", map[string]string{"Host": "rebind.evil.example:8080"})
	if rec.Code != http.StatusForbidden {
		t.Errorf("GET with Host rebind.evil.example -> %d, want 403", rec.Code)
	}
	rec = do(t, s, "POST", "/api/workers", "application/json", `{"name":"w"}`,
		map[string]string{"Host": "rebind.evil.example:8080", "Origin": "http://rebind.evil.example:8080"})
	if rec.Code != http.StatusForbidden {
		t.Errorf("POST with Host rebind.evil.example -> %d, want 403", rec.Code)
	}
	for _, host := range []string{"127.0.0.1:8080", "localhost:8080", "[::1]:8080"} {
		if rec := do(t, s, "GET", "/api/workers", "", "", map[string]string{"Host": host}); rec.Code != http.StatusOK {
			t.Errorf("GET with Host %s -> %d, want 200", host, rec.Code)
		}
	}
}

func TestHostCheckOnlyForLoopbackListeners(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:8080": true, "localhost:8080": true, "[::1]:8080": true,
		":8080": false, "0.0.0.0:8080": false, "192.168.1.5:8080": false,
	} {
		if got := isLoopbackAddr(addr); got != want {
			t.Errorf("isLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

// PATCH {"name": ...} is a partial update, but the handler decoded the body
// into a zero config and mergeConfig only backfilled some fields, so a
// rename reset retries, rate_rps, insecure_tls and the cache settings.
func TestPatchRenameKeepsConfig(t *testing.T) {
	s, _ := newTestServer(t)
	rec := do(t, s, "POST", "/api/workers", "application/json",
		`{"name":"w","config":{"workers":5,"retries":5,"rate_rps":7,"insecure_tls":true,"cache_ttl_ns":60000000000}}`, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	created := decodeView(t, rec)
	rec = do(t, s, "PATCH", "/api/workers/"+created.ID, "application/json", `{"name":"renamed"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body.String())
	}
	v := decodeView(t, rec)
	if v.Name != "renamed" {
		t.Errorf("name = %q, want renamed", v.Name)
	}
	if v.Config != created.Config {
		t.Errorf("rename changed the config:\n before %+v\n after  %+v", created.Config, v.Config)
	}

	rec = do(t, s, "PATCH", "/api/workers/"+created.ID, "application/json", `{"config":{"retries":0}}`, nil)
	v = decodeView(t, rec)
	if v.Config.Retries != 0 || v.Config.RateRPS != 7 || v.Name != "renamed" {
		t.Errorf("PATCH retries=0: retries=%d rate_rps=%d name=%q, want 0/7/renamed", v.Config.Retries, v.Config.RateRPS, v.Name)
	}
}

// default_retries and default_rate_rps in Settings are the defaults for new
// workers, but POST /api/workers ignored them for fields left out.
func TestCreateUsesSettingsDefaults(t *testing.T) {
	s, st := newTestServer(t)
	set := st.GetSettings()
	set.DefaultRetries = 4
	set.DefaultRateRPS = 9
	if err := st.PutSettings(set); err != nil {
		t.Fatal(err)
	}
	rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"w","config":{}}`, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	got := decodeView(t, rec).Config
	if got.Retries != 4 || got.RateRPS != 9 {
		t.Errorf("new worker: retries=%d rate_rps=%d, want the settings defaults 4/9", got.Retries, got.RateRPS)
	}
	rec = do(t, s, "POST", "/api/workers", "application/json", `{"name":"w2","config":{"retries":0,"rate_rps":0}}`, nil)
	got = decodeView(t, rec).Config
	if got.Retries != 0 || got.RateRPS != 0 {
		t.Errorf("explicit retries=0 rate_rps=0 became %d/%d", got.Retries, got.RateRPS)
	}
}

// "Start all" started every ready, done and error worker from scratch,
// which deletes its results first: a finished worker lost all of them, and
// a stopped worker (status "ready") started over instead of continuing.
func TestBulkStartContinuesAndKeepsResults(t *testing.T) {
	f := newFakeFragment(t, 100*time.Millisecond)
	s, st := newTestServer(t)
	cfg := `{"workers":2,"retries":0}`

	finished := newWorkerWithInput(t, s, "finished", cfg, seqNames("done", 5))
	stopped := newWorkerWithInput(t, s, "stopped", cfg, seqNames("slow", 40))
	fresh := newWorkerWithInput(t, s, "fresh", cfg, seqNames("fresh", 5))

	if rec := do(t, s, "POST", "/api/workers/"+finished+"/start", "", "", nil); rec.Code != http.StatusOK {
		t.Fatalf("start: %d %s", rec.Code, rec.Body.String())
	}
	if got := waitStopped(t, s, finished); got != worker.StatusDone {
		t.Fatalf("finished worker: status %s, want done", got)
	}

	do(t, s, "POST", "/api/workers/"+stopped+"/start", "", "", nil)
	deadline := time.Now().Add(10 * time.Second)
	for len(resultNames(t, st, stopped)) < 4 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	do(t, s, "POST", "/api/workers/"+stopped+"/stop", "", "", nil)
	waitStopped(t, s, stopped)
	before := resultNames(t, st, stopped)
	if len(before) == 0 || len(before) == 40 {
		t.Fatalf("stopped worker has %d results, want a partial run", len(before))
	}

	rec := do(t, s, "POST", "/api/workers-bulk/start", "", "", nil)
	var res map[string]int
	_ = json.Unmarshal(rec.Body.Bytes(), &res)
	if rec.Code != http.StatusOK || res["started"] != 2 || res["skipped"] != 1 {
		t.Fatalf("bulk start: %d %s, want started=2 skipped=1", rec.Code, rec.Body.String())
	}
	for _, id := range []string{finished, stopped, fresh} {
		waitStopped(t, s, id)
	}

	if got := resultNames(t, st, finished); len(got) != 5 {
		t.Errorf("finished worker: %d results after Start all, want its 5", len(got))
	}
	for _, n := range seqNames("done", 5) {
		if h := f.hitsFor(n); h != 1 {
			t.Errorf("%s was requested %d times, want 1 (finished worker restarted)", n, h)
		}
	}
	after := resultNames(t, st, stopped)
	seen := map[string]bool{}
	for _, n := range after {
		seen[n] = true
	}
	if len(after) != 40 || len(seen) != 40 {
		t.Errorf("stopped worker: %d results (%d unique) after Start all, want 40", len(after), len(seen))
	}
	for _, n := range before {
		if h := f.hitsFor(n); h != 1 {
			t.Errorf("%s was checked before the stop and requested %d times, want 1", n, h)
		}
	}
	if got := resultNames(t, st, fresh); len(got) != 5 {
		t.Errorf("fresh worker: %d results, want 5", len(got))
	}
}

// PUT /api/settings decoded the body onto zero values: {"theme":"light"}
// also set default_retries, default_rate_rps, default_workers and the rest
// to 0 (or "").
func TestSettingsPartialUpdateKeepsOtherFields(t *testing.T) {
	s, st := newTestServer(t)
	before := st.GetSettings()
	rec := do(t, s, "PUT", "/api/settings", "application/json", `{"theme":"light"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT theme: %d %s", rec.Code, rec.Body.String())
	}
	after := st.GetSettings()
	want := before
	want.Theme = "light"
	if after != want {
		t.Errorf("after PUT {theme: light}:\n got  %+v\n want %+v", after, want)
	}
}

// "trace" was a log level of its own (every step of every request in the
// Logs tab). That detail is in a worker's trace file now: an old client
// that still sends "trace" gets debug, not a 400.
func TestSettingsTraceLevelMeansDebug(t *testing.T) {
	s, st := newTestServer(t)
	rec := do(t, s, "PUT", "/api/settings", "application/json", `{"log_level":"trace"}`, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"log_level":"debug"`) {
		t.Fatalf("PUT log_level trace: %d %s", rec.Code, rec.Body.String())
	}
	if got := st.GetSettings().LogLevel; got != "debug" {
		t.Errorf("saved log level %q, want debug", got)
	}
}

// Settings were not validated: default_rate_rps -5 was saved, and from
// then on every POST /api/workers failed with 400.
func TestSettingsRejectsOutOfRange(t *testing.T) {
	s, st := newTestServer(t)
	before := st.GetSettings()
	for _, body := range []string{
		`{"default_rate_rps":-5}`, `{"default_retries":11}`, `{"default_workers":0}`,
		`{"default_workers":100000}`, `{"default_timeout_s":0}`, `{"theme":"blue"}`,
		`{"log_level":"loud"}`, `{"history_limit_max":-1}`,
	} {
		if rec := do(t, s, "PUT", "/api/settings", "application/json", body, nil); rec.Code != http.StatusBadRequest {
			t.Errorf("PUT %s -> %d %s, want 400", body, rec.Code, rec.Body.String())
		}
	}
	if got := st.GetSettings(); got != before {
		t.Errorf("rejected PUTs changed the settings: %+v", got)
	}
	if rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"w"}`, nil); rec.Code != http.StatusCreated {
		t.Errorf("POST /api/workers after the rejected PUTs: %d %s", rec.Code, rec.Body.String())
	}

	// A bad value written into settings.json by hand must not block new workers.
	bad := before
	bad.DefaultRateRPS = -5
	if err := st.PutSettings(bad); err != nil {
		t.Fatal(err)
	}
	if rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"w2"}`, nil); rec.Code != http.StatusCreated {
		t.Errorf("POST /api/workers with default_rate_rps -5 in settings.json: %d %s", rec.Code, rec.Body.String())
	}
}

// 0 still means "use the default".
func TestZeroConfigValuesUseDefaults(t *testing.T) {
	s, _ := newTestServer(t)
	rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"w","config":{"workers":0,"timeout_ns":0}}`, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	cfg := decodeView(t, rec).Config
	if def := checker.DefaultConfig(); cfg.Workers != def.Workers || cfg.Timeout != def.Timeout {
		t.Errorf("workers=0 timeout=0 became %d / %v, want the defaults %d / %v", cfg.Workers, cfg.Timeout, def.Workers, def.Timeout)
	}
}

// Small API rough edges: an empty result list came back as "results":
// null, ?limit had no upper bound, an unknown /api/ path got the UI's
// plain-text 404, and /static/ answered for the directory itself.
func TestAPIRoughEdges(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bus := logbus.New(100, logbus.LevelInfo)
	mgr := worker.New(st, bus)
	mgr.ConfigureCache("", 0, 0, true)
	t.Cleanup(func() { mgr.CloseAll(); bus.Close(); _ = st.Close() })
	static := fstest.MapFS{
		"index.html": {Data: []byte("<html></html>")},
		"app.js":     {Data: []byte("1")},
	}
	s := New(Options{Addr: "127.0.0.1:0", Manager: mgr, Store: st, Bus: bus, StaticFS: static})

	rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"w"}`, nil)
	id := decodeView(t, rec).ID
	rec = do(t, s, "GET", "/api/workers/"+id+"/results", "", "", nil)
	if !strings.Contains(rec.Body.String(), `"results":[]`) {
		t.Errorf("empty results: %s, want \"results\":[]", rec.Body.String())
	}

	names := seqNames("name", maxResultsPage+5)
	if _, err := mgr.SetInput(context.Background(), id, names); err != nil {
		t.Fatal(err)
	}
	batch := make([]store.Result, 0, len(names))
	for _, n := range names {
		batch = append(batch, store.Result{WorkerID: id, Username: n, Status: "free"})
	}
	if err := st.RecordResults(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	rec = do(t, s, "GET", "/api/workers/"+id+"/results?limit=1000000", "", "", nil)
	var page struct {
		Results []store.Result `json:"results"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if len(page.Results) != maxResultsPage {
		t.Errorf("?limit=1000000 returned %d results, want the cap %d", len(page.Results), maxResultsPage)
	}

	rec = do(t, s, "GET", "/api/nope", "", "", nil)
	if rec.Code != http.StatusNotFound || !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		t.Errorf("GET /api/nope: %d %q %s, want a JSON 404", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
	if rec := do(t, s, "GET", "/static/", "", "", nil); rec.Code != http.StatusNotFound {
		t.Errorf("GET /static/: %d %s, want 404", rec.Code, rec.Body.String())
	}
	if rec := do(t, s, "GET", "/static/app.js", "", "", nil); rec.Code != http.StatusOK {
		t.Errorf("GET /static/app.js: %d", rec.Code)
	}
	if rec := do(t, s, "GET", "/", "", "", nil); rec.Code != http.StatusOK {
		t.Errorf("GET /: %d", rec.Code)
	}
}

// Behind a reverse proxy with a domain and a certificate the browser sends
// Host and Origin of that domain, and the loopback-only Host check
// answered every request with 403. -allow-host names the domain.
func TestAllowHostForReverseProxy(t *testing.T) {
	s, _ := newTestServerWith(t, func(o *Options) { o.AllowHosts = []string{"panel.example.com"} })
	cases := []struct {
		method string
		hdr    map[string]string
		want   int
	}{
		{"GET", map[string]string{"Host": "panel.example.com"}, http.StatusOK},
		{"GET", map[string]string{"Host": "PANEL.example.com:443"}, http.StatusOK},
		{"GET", map[string]string{"Host": "localhost:8080"}, http.StatusOK}, // ssh -L still works
		{"GET", map[string]string{"Host": "evil.example"}, http.StatusForbidden},
		// nginx by default passes its own upstream Host; only Origin shows the domain
		{"POST", map[string]string{"Host": "127.0.0.1:8080", "Origin": "https://panel.example.com"}, http.StatusCreated},
		{"POST", map[string]string{"Host": "panel.example.com", "Origin": "https://panel.example.com"}, http.StatusCreated},
		{"POST", map[string]string{"Host": "panel.example.com", "Origin": "https://evil.example"}, http.StatusForbidden},
		{"POST", map[string]string{"Host": "127.0.0.1:8080", "Origin": "https://evil.example"}, http.StatusForbidden},
	}
	for _, c := range cases {
		path, body, ctype := "/api/workers", "", ""
		if c.method == "POST" {
			body, ctype = `{"name":"w"}`, "application/json"
		}
		if rec := do(t, s, c.method, path, ctype, body, c.hdr); rec.Code != c.want {
			t.Errorf("%s with %v -> %d %s, want %d", c.method, c.hdr, rec.Code, rec.Body.String(), c.want)
		}
	}

	// On a public listener the list applies as well: an IP Host is refused.
	pub, _ := newTestServerWith(t, func(o *Options) { o.Addr = "0.0.0.0:443"; o.AllowHosts = []string{"panel.example.com"} })
	if rec := do(t, pub, "GET", "/api/workers", "", "", map[string]string{"Host": "203.0.113.5"}); rec.Code != http.StatusForbidden {
		t.Errorf("public listener, Host 203.0.113.5 -> %d, want 403", rec.Code)
	}
	if rec := do(t, pub, "GET", "/api/workers", "", "", map[string]string{"Host": "panel.example.com"}); rec.Code != http.StatusOK {
		t.Errorf("public listener, Host panel.example.com -> %d, want 200", rec.Code)
	}
}

// The panel had no login: anyone who could reach it (through ssh -L or a
// domain) could create, start and delete workers and read every result.
// Now every request needs an access token, except the login page.
func TestAccessTokenRequired(t *testing.T) {
	keeper, _, err := auth.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	static := fstest.MapFS{"index.html": {Data: []byte("<html>panel</html>")}, "app.js": {Data: []byte("1")}}
	s, _ := newTestServerWith(t, func(o *Options) { o.Auth = keeper; o.StaticFS = static })
	good, _, _ := keeper.Issue("laptop", time.Hour)
	bearer := func(tok string) map[string]string { return map[string]string{"Authorization": "Bearer " + tok} }

	rec := do(t, s, "GET", "/api/workers", "", "", nil)
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Errorf("API without a token: %d, WWW-Authenticate %q; want 401 with the header", rec.Code, rec.Header().Get("WWW-Authenticate"))
	}
	for _, path := range []string{"/", "/static/app.js"} {
		if rec := do(t, s, "GET", path, "", "", nil); rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/login" {
			t.Errorf("GET %s without a token: %d -> %q, want a redirect to /login", path, rec.Code, rec.Header().Get("Location"))
		}
	}
	if rec := do(t, s, "GET", "/login", "", "", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `name="token"`) {
		t.Errorf("GET /login: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, s, "GET", "/api/workers", "", "", bearer(good)); rec.Code != http.StatusOK {
		t.Errorf("Bearer with a valid token: %d", rec.Code)
	}
	if rec := do(t, s, "GET", "/api/workers", "", "", bearer(good+"x")); rec.Code != http.StatusUnauthorized {
		t.Errorf("Bearer with a forged token: %d, want 401", rec.Code)
	}

	// Login form: a valid token sets an HttpOnly cookie that opens the panel.
	rec = do(t, s, "POST", "/login", "application/x-www-form-urlencoded", "token="+good, nil)
	cookies := rec.Result().Cookies()
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/" || len(cookies) != 1 {
		t.Fatalf("POST /login: %d -> %q, cookies %v", rec.Code, rec.Header().Get("Location"), cookies)
	}
	ck := cookies[0]
	if ck.Name != cookieName || !ck.HttpOnly || ck.SameSite != http.SameSiteLaxMode || ck.Secure || ck.Value != good {
		t.Errorf("cookie %+v, want HttpOnly, SameSite=Lax, not Secure over plain HTTP", ck)
	}
	withCookie := map[string]string{"Cookie": cookieName + "=" + ck.Value}
	if rec := do(t, s, "GET", "/", "", "", withCookie); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "panel") {
		t.Errorf("GET / with the cookie: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, s, "GET", "/login", "", "", withCookie); rec.Code != http.StatusSeeOther {
		t.Errorf("GET /login while logged in: %d, want a redirect to /", rec.Code)
	}
	rec = do(t, s, "POST", "/login", "application/x-www-form-urlencoded", "token="+good, map[string]string{"X-Forwarded-Proto": "https"})
	if c := rec.Result().Cookies(); len(c) != 1 || !c[0].Secure {
		t.Errorf("login behind a TLS proxy: cookie %v, want Secure", c)
	}

	// A wrong token: back to the form with an error, no cookie.
	rec = do(t, s, "POST", "/login", "application/x-www-form-urlencoded", "token=nope", nil)
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/login?failed=1" || len(rec.Result().Cookies()) != 0 {
		t.Errorf("POST /login with a wrong token: %d -> %q, cookies %v", rec.Code, rec.Header().Get("Location"), rec.Result().Cookies())
	}
	if rec := do(t, s, "GET", "/login?failed=1", "", "", nil); !strings.Contains(rec.Body.String(), "not valid") {
		t.Errorf("GET /login?failed=1 shows no error: %s", rec.Body.String())
	}
	// Another site cannot log the browser in (login CSRF).
	rec = do(t, s, "POST", "/login", "application/x-www-form-urlencoded", "token="+good, map[string]string{"Origin": "https://evil.example"})
	if rec.Code != http.StatusForbidden {
		t.Errorf("cross-site POST /login: %d, want 403", rec.Code)
	}

	rec = do(t, s, "POST", "/logout", "", "", withCookie)
	if c := rec.Result().Cookies(); rec.Code != http.StatusSeeOther || len(c) != 1 || c[0].MaxAge >= 0 {
		t.Errorf("POST /logout: %d, cookies %v; want the cookie removed", rec.Code, c)
	}

	// Rotating the key revokes the token.
	if err := keeper.Rotate(); err != nil {
		t.Fatal(err)
	}
	if rec := do(t, s, "GET", "/api/workers", "", "", bearer(good)); rec.Code != http.StatusUnauthorized {
		t.Errorf("token after a key rotation: %d, want 401", rec.Code)
	}
}

// cache_dir of a worker came straight from the API: anyone with access
// could make the panel create directories and write cache files anywhere
// it may write.
func TestCacheDirNotSettableThroughAPI(t *testing.T) {
	s, _ := newTestServer(t)
	rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"w","config":{"cache_dir":"/tmp/elsewhere"}}`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("POST with cache_dir: %d %s, want 400", rec.Code, rec.Body.String())
	}
	rec = do(t, s, "POST", "/api/workers", "application/json", `{"name":"w"}`, nil)
	id := decodeView(t, rec).ID
	if rec := do(t, s, "PATCH", "/api/workers/"+id, "application/json", `{"config":{"cache_dir":"/root/.ssh"}}`, nil); rec.Code != http.StatusBadRequest {
		t.Errorf("PATCH cache_dir: %d %s, want 400", rec.Code, rec.Body.String())
	}

	// A worker that already has one (from an older version) can still be edited.
	cfg := checker.DefaultConfig()
	cfg.CacheDir = t.TempDir()
	old, err := s.mgr.Create(context.Background(), "old", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rec := do(t, s, "PATCH", "/api/workers/"+old.ID(), "application/json", `{"name":"renamed"}`, nil); rec.Code != http.StatusOK {
		t.Errorf("PATCH name of a worker with a cache_dir: %d %s, want 200", rec.Code, rec.Body.String())
	}
}

// The upload read the whole multipart form first (a temporary file for
// anything over 10 MB) and then held the names in a []string and a map.
// Now the "file" part is parsed as it arrives; other fields before it are
// skipped, and an upload over the cap gets 413 instead of a parse error.
func TestUploadStreamsTheFilePart(t *testing.T) {
	s, st := newTestServer(t)
	rec := do(t, s, "POST", "/api/workers", "application/json", `{"name":"up","config":{}}`, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	id := decodeView(t, rec).ID

	upload := func(fields map[string]string, order []string) *httptest.ResponseRecorder {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		for _, name := range order {
			if name == "file" {
				fw, err := mw.CreateFormFile("file", "names.txt")
				if err != nil {
					t.Fatal(err)
				}
				_, _ = fw.Write([]byte(fields[name]))
				continue
			}
			_ = mw.WriteField(name, fields[name])
		}
		_ = mw.Close()
		return do(t, s, "POST", "/api/workers/"+id+"/input/upload", mw.FormDataContentType(), buf.String(), nil)
	}

	rec = upload(map[string]string{"note": "ignored", "file": "alice\nBobby, alice;bad-name\n@carol | free\ndave"},
		[]string{"note", "file"})
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: %d %s", rec.Code, rec.Body.String())
	}
	var got struct{ Accepted, Rejected int }
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Accepted != 4 || got.Rejected != 1 {
		t.Errorf("accepted %d, rejected %d; want 4 and 1", got.Accepted, got.Rejected)
	}
	var names []string
	if err := st.IterInputs(context.Background(), id, func(n string) error { names = append(names, n); return nil }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "alice,bobby,carol,dave" {
		t.Errorf("stored list %v, want alice,bobby,carol,dave in order", names)
	}

	if rec := upload(map[string]string{"note": "x"}, []string{"note"}); rec.Code != http.StatusBadRequest {
		t.Errorf("upload without a file field: %d, want 400", rec.Code)
	}

	old := maxUploadBytes
	maxUploadBytes = 1 << 10
	t.Cleanup(func() { maxUploadBytes = old })
	if rec := upload(map[string]string{"file": strings.Repeat("abcdefgh\n", 500)}, []string{"file"}); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("upload over the cap: %d %s, want 413", rec.Code, rec.Body.String())
	}
	// The list stored before the refused upload is still there.
	if n, _ := st.InputCount(context.Background(), id); n != 4 {
		t.Errorf("%d names after a refused upload, want the previous 4", n)
	}
}

// GET /api/workers/{id}/trace downloads the structured trace of a worker
// (JSON Lines) and DELETE removes it; without a trace GET says how to get
// one, and a running worker's trace cannot be deleted.
func TestTraceDownloadAPI(t *testing.T) {
	fake := newFakeFragment(t, 300*time.Millisecond)
	s, _ := newTestServer(t)
	id := newWorkerWithInput(t, s, "traced", `{"workers":2,"retries":0,"trace":true}`, []string{"alpha001", "slowname"})
	if rec := do(t, s, "GET", "/api/workers/"+id+"/trace", "", "", nil); rec.Code != http.StatusNotFound ||
		!strings.Contains(rec.Body.String(), "Write a trace") {
		t.Errorf("before a run: %d %s, want 404 with a hint", rec.Code, rec.Body.String())
	}
	if rec := do(t, s, "POST", "/api/workers/"+id+"/start", "", "", nil); rec.Code >= 300 {
		t.Fatalf("start: %d %s", rec.Code, rec.Body.String())
	}
	for deadline := time.Now().Add(5 * time.Second); fake.hitsFor("slowname") == 0 && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	if rec := do(t, s, "DELETE", "/api/workers/"+id+"/trace", "", "", nil); rec.Code != http.StatusConflict {
		t.Errorf("DELETE while running: %d, want 409", rec.Code)
	}
	if st := waitStopped(t, s, id); st != worker.StatusDone {
		t.Fatalf("status %s", st)
	}
	rec := do(t, s, "GET", "/api/workers/"+id+"/trace", "", "", nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/x-ndjson" ||
		!strings.Contains(rec.Header().Get("Content-Disposition"), "traced-trace.jsonl") {
		t.Fatalf("GET: %d %v", rec.Code, rec.Header())
	}
	kinds := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(rec.Body.String()), "\n") {
		var e checker.TraceEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("not JSON: %q", line)
		}
		kinds[e.Kind]++
	}
	if kinds["run_start"] != 1 || kinds["result"] != 2 || kinds["run_end"] != 1 {
		t.Errorf("trace kinds %v", kinds)
	}
	if rec := do(t, s, "DELETE", "/api/workers/"+id+"/trace", "", "", nil); rec.Code != http.StatusNoContent {
		t.Errorf("DELETE: %d", rec.Code)
	}
	if rec := do(t, s, "GET", "/api/workers/"+id+"/trace", "", "", nil); rec.Code != http.StatusNotFound {
		t.Errorf("GET after DELETE: %d, want 404", rec.Code)
	}
	if rec := do(t, s, "GET", "/api/workers/nosuchworker/trace", "", "", nil); rec.Code != http.StatusNotFound {
		t.Errorf("unknown worker: %d, want 404", rec.Code)
	}
}
