package profiling

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHandlerServesTheProfiles(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()
	for path, want := range map[string]string{
		"/debug/pprof/":                  "goroutine",
		"/debug/pprof/heap?debug=1":      "heap profile",
		"/debug/pprof/goroutine?debug=1": "goroutine profile",
		"/debug/pprof/cmdline":           "",
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(body), want) {
			t.Errorf("%s: %d %.80q", path, resp.StatusCode, body)
		}
	}
}

func TestServeOnLoopback(t *testing.T) {
	addr, stop, err := Serve("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	resp, err := http.Get("http://" + addr.String() + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status %d", resp.StatusCode)
	}
	for addr, want := range map[string]bool{"127.0.0.1:6060": true, "localhost:6060": true, "[::1]:1": true,
		"0.0.0.0:6060": false, ":6060": false, "192.168.1.5:6060": false} {
		if got := IsLoopback(addr); got != want {
			t.Errorf("IsLoopback(%q) = %v", addr, got)
		}
	}
}

// pprof files are gzip-compressed protocol buffers.
func isGzip(t *testing.T, path string) bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	return err == nil && len(raw) > 2 && raw[0] == 0x1f && raw[1] == 0x8b
}

func TestCPUAndHeapProfiles(t *testing.T) {
	dir := t.TempDir()
	stop, err := StartCPU(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(50 * time.Millisecond); time.Now().Before(end); {
		_ = bytes.Repeat([]byte("x"), 1024)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if err := WriteHeap(filepath.Join(dir, "heap.pprof")); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"cpu.pprof", "heap.pprof"} {
		if !isGzip(t, filepath.Join(dir, f)) {
			t.Errorf("%s is not a pprof file", f)
		}
	}
}

func TestExecTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exec.trace")
	tr, err := StartExecTrace(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan bool)
	go func() { time.Sleep(5 * time.Millisecond); done <- true }()
	<-done
	n, early, err := tr.Stop()
	if err != nil || n == 0 || early != "" {
		t.Fatalf("Stop = %d, %q, %v", n, early, err)
	}
	raw, _ := os.ReadFile(path)
	if !bytes.HasPrefix(raw, []byte("go 1.")) || int64(len(raw)) != n {
		t.Errorf("trace file: %d bytes, starts %.10q", len(raw), raw)
	}
}

// Over its limit the execution trace stops by itself; the file stays
// readable (the stream is not cut, so it ends a little past the limit).
func TestExecTraceStopsAtItsLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exec.trace")
	tr, err := StartExecTrace(path, 64<<10, 0)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !tr.stopping.Load() && time.Now().Before(deadline) {
		var wg [8]chan bool
		for i := range wg {
			wg[i] = make(chan bool)
			go func(c chan bool) { c <- true }(wg[i])
		}
		for i := range wg {
			<-wg[i]
		}
	}
	n, early, err := tr.Stop()
	if err != nil || !strings.Contains(early, "limit") || n < 64<<10 {
		t.Fatalf("Stop = %d, %q, %v", n, early, err)
	}
}
