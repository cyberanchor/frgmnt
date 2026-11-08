package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cyberanchor/frgmnt/internal/auth"
	"github.com/cyberanchor/frgmnt/internal/checker"
	"github.com/cyberanchor/frgmnt/internal/store"
	"github.com/cyberanchor/frgmnt/internal/webapp"
)

// freeAddr returns a loopback address with a port that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// call sends one JSON request to the panel with the access token (none when
// token is "") and decodes the answer into out.
func call(t *testing.T, token, method, url, body string, out any) int {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, url, raw, err)
		}
	}
	return resp.StatusCode
}

// TestWebMainEndToEnd runs the web mode the way the binary does
// (webapp.Main with flags, here -base-url pointing at a fake Fragment),
// drives it over HTTP like the panel with an access token, stops it with
// SIGINT and checks that the results stay in the data directory.
func TestWebMainEndToEnd(t *testing.T) {
	sold := fixture(t, "sold.html")
	frag := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		if name == "soldname" {
			_, _ = w.Write(sold)
			return
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	}))
	defer frag.Close()
	defer func(old string) { checker.FragmentURLTemplate = old }(checker.FragmentURLTemplate)

	addr := freeAddr(t)
	data := t.TempDir()
	keeper, _, err := auth.Open(data) // what "frgmnt token -data <data>" does
	if err != nil {
		t.Fatal(err)
	}
	tok, _, err := keeper.Issue("e2e", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() {
		done <- webapp.Main([]string{"-addr", addr, "-data", data, "-base-url", frag.URL, "-no-cache"})
	}()
	base := "http://" + addr
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(base + "/api/health")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the panel did not come up: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if code := call(t, "", "GET", base+"/api/workers", "", nil); code != http.StatusUnauthorized {
		t.Fatalf("API without a token: %d, want 401", code)
	}

	var w struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Stats  struct {
			Checked int64 `json:"checked"`
			Free    int64 `json:"free"`
			Sold    int64 `json:"sold"`
		} `json:"stats"`
	}
	if code := call(t, tok, "POST", base+"/api/workers", `{"name":"e2e","config":{"workers":2}}`, &w); code != http.StatusCreated {
		t.Fatalf("create worker: %d", code)
	}
	var in map[string]int
	call(t, tok, "POST", base+"/api/workers/"+w.ID+"/input", `{"text":"soldname, freeone\nfreetwo ab"}`, &in)
	if in["accepted"] != 3 || in["rejected"] != 1 {
		t.Errorf("input: %v, want 3 accepted and 1 rejected (ab is not a possible name)", in)
	}
	if code := call(t, tok, "POST", base+"/api/workers/"+w.ID+"/start", "", nil); code != http.StatusOK {
		t.Fatalf("start: %d", code)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		call(t, tok, "GET", base+"/api/workers/"+w.ID, "", &w)
		if w.Status == "done" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker status %q after 10 s", w.Status)
		}
	}
	if w.Stats.Checked != 3 || w.Stats.Free != 2 || w.Stats.Sold != 1 {
		t.Errorf("tile stats %+v, want 3 checked: 2 free, 1 sold", w.Stats)
	}
	req, _ := http.NewRequest("GET", base+"/api/workers/"+w.ID+"/results/download", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, line := range []string{"@soldname | sold", "@freeone | free", "@freetwo | free"} {
		if !strings.Contains(string(text), line+"\n") {
			t.Errorf("download lacks %q:\n%s", line, text)
		}
	}

	_ = syscall.Kill(os.Getpid(), syscall.SIGINT)
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("webapp.Main exit code %d after SIGINT", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("webapp.Main did not stop after SIGINT")
	}

	st, err := store.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	counts, err := st.CountByStatus(context.Background(), w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(counts); got != "map[free:2 sold:1]" {
		t.Errorf("results in the data directory after the stop: %s", got)
	}
}
