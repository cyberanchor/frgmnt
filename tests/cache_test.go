package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
)

func TestPageCacheHitAvoidsSecondRequest(t *testing.T) {
	var calls int32
	body := fixture(t, "for_sale.html")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	useFragmentServer(t, srv)

	c := testChecker(t, checker.Config{CacheDir: t.TempDir()})
	first := c.CheckOne(context.Background(), "forsale_name")
	if first.Status != checker.StatusAvailable || first.Cached {
		t.Fatalf("first = %+v, want fresh available", first)
	}
	second := c.CheckOne(context.Background(), "forsale_name")
	if second.Status != checker.StatusAvailable || !second.Cached {
		t.Fatalf("second = %+v, want cached available", second)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("server calls = %d, want 1", got)
	}
	if c.Snapshot().CacheHits != 1 {
		t.Fatalf("cache_hits = %d, want 1", c.Snapshot().CacheHits)
	}
}

func TestPageCacheDisabled(t *testing.T) {
	var calls int32
	body := fixture(t, "for_sale.html")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	useFragmentServer(t, srv)

	c := testChecker(t, checker.Config{}) // CacheDir empty => disabled
	_ = c.CheckOne(context.Background(), "forsale_name")
	_ = c.CheckOne(context.Background(), "forsale_name")
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("server calls = %d, want 2 (cache disabled)", got)
	}
}

func TestVolatileCacheExpires(t *testing.T) {
	var calls int32
	body := fixture(t, "for_sale.html")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	useFragmentServer(t, srv)

	c := testChecker(t, checker.Config{CacheDir: t.TempDir(), CacheTTL: time.Nanosecond})
	_ = c.CheckOne(context.Background(), "forsale_name")
	res := c.CheckOne(context.Background(), "forsale_name")
	if res.Cached {
		t.Fatal("volatile entry with 1ns TTL must not be reused")
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("server calls = %d, want 2", got)
	}
	if c.Snapshot().CacheStale == 0 {
		t.Fatal("cache_stale was not incremented")
	}
}

// A sold entry is kept for a long TTL by default, but when it does expire
// and the network is down, the checker serves it as an explicitly stale
// fallback instead of losing the result.
func TestSoldCacheServedStaleOnNetworkFailure(t *testing.T) {
	body := fixture(t, "sold.html")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	useFragmentServer(t, srv)

	c := testChecker(t, checker.Config{
		CacheDir:     t.TempDir(),
		CacheSoldTTL: time.Nanosecond,
	})
	first := c.CheckOne(context.Background(), "adam")
	if first.Status != checker.StatusSold || first.Cached {
		t.Fatalf("first = %+v, want fresh sold", first)
	}

	srv.Close()

	second := c.CheckOne(context.Background(), "adam")
	if second.Status != checker.StatusSold {
		t.Fatalf("second status = %q, want sold", second.Status)
	}
	if !second.Cached || !second.Stale {
		t.Fatalf("second = %+v, want cached+stale", second)
	}
	if second.Error == "" {
		t.Fatal("stale fallback should explain the network failure")
	}
}
