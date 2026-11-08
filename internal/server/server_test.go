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
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	for k, v := range hdr {
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
