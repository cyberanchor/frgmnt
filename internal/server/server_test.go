package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cyberanchor/frgmnt/internal/logbus"
	"github.com/cyberanchor/frgmnt/internal/store"
	"github.com/cyberanchor/frgmnt/internal/worker"
)

func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bus := logbus.New(100, logbus.LevelInfo)
	mgr := worker.New(st, bus)
	mgr.ConfigureCache("", 0, 0, true)
	t.Cleanup(func() { mgr.CloseAll(); bus.Close(); _ = st.Close() })
	return New(Options{Addr: "127.0.0.1:0", Manager: mgr, Store: st, Bus: bus}), st
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
