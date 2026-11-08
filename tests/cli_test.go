package tests

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cyberanchor/frgmnt/fragment_checker_v4/internal/cli"
)

// TestCLIEndToEnd drives the real console entry point against a local fake
// Fragment. It covers flag parsing, the worker pool, parsing, and the output
// file without touching the real site.
func TestCLIEndToEnd(t *testing.T) {
	soldBody := fixture(t, "sold.html")
	saleBody := fixture(t, "for_sale.html")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.URL.Path, "/username/") {
		case "soldname":
			_, _ = w.Write(soldBody)
		case "sale_name":
			_, _ = w.Write(saleBody)
		case "freename":
			http.Redirect(w, r, "/?query=freename", http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	if err := os.WriteFile(in, []byte("soldname\nsale_name\nfreename\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	code := cli.Main([]string{
		"-input", in,
		"-output", out,
		"-workers", "2",
		"-base-url", srv.URL,
		"-no-cache",
		"-quiet",
	})
	if code != 0 {
		t.Fatalf("cli.Main exit code = %d, want 0", code)
	}

	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if parts := strings.SplitN(line, "|", 2); len(parts) == 2 {
			got[strings.TrimSpace(strings.TrimPrefix(parts[0], "@"))] = strings.TrimSpace(parts[1])
		}
	}
	want := map[string]string{
		"soldname":  "sold",
		"sale_name": "available",
		"freename":  "free",
	}
	for name, status := range want {
		if got[name] != status {
			t.Errorf("%s: got %q, want %q (all: %v)", name, got[name], status, got)
		}
	}
}
