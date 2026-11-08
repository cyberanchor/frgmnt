package checker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const soldPageFixture = `<html><body>` +
	`<h2><span class="tm-section-header-status tm-status-unavail">Sold</span></h2>` +
	`<div class="tm-bid-info-text">Purchased on 19 Nov 2022</div>` +
	`</body></html>`

const forSalePageFixture = `<html><body>` +
	`<h2><span class="tm-section-header-status tm-status-avail">For sale</span></h2>` +
	`<time class="tm-countdown-timer">361d</time>` +
	`<button class="js-buy-now-btn">Buy for 7</button>` +
	`</body></html>`

func TestPageCacheRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c := newPageCache(dir, 0, 0)
	if c == nil {
		t.Fatal("newPageCache returned nil for a non-empty dir")
	}

	info := ParseInfo{Status: StatusSold, Confidence: 99, Evidence: `text="sold"`}
	body := []byte(soldPageFixture)
	if err := c.put("adam", "https://fragment.com/username/adam", info, body); err != nil {
		t.Fatalf("put: %v", err)
	}

	ent, got, ok := c.get("adam")
	if !ok {
		t.Fatal("get: cache miss after put")
	}
	if string(got) != soldPageFixture {
		t.Fatalf("body mismatch: %q", string(got))
	}
	if ent.Status != StatusSold || ent.Confidence != 99 || ent.Username != "adam" {
		t.Fatalf("unexpected metadata: %+v", ent)
	}
	if !c.fresh(ent.Status, ent.FetchedAt) {
		t.Fatal("fresh sold entry reported stale")
	}
	if c.fresh(StatusSold, time.Now().Add(-8*24*time.Hour)) {
		t.Fatal("sold entry older than 7 days reported fresh")
	}
}

func TestPageCacheDisabled(t *testing.T) {
	if c := newPageCache("", 0, 0); c != nil {
		t.Fatal("empty dir must disable the cache")
	}
}

func TestPageCacheVolatileDisabled(t *testing.T) {
	c := newPageCache(t.TempDir(), -1, 0)
	info := ParseInfo{Status: StatusAvailable, Confidence: 99}
	if err := c.put("x", "u", info, []byte("body")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, _, ok := c.get("x"); ok {
		t.Fatal("volatile status was cached despite CacheTTL<0")
	}
}

func TestPageCacheTTLPolicy(t *testing.T) {
	c := newPageCache(t.TempDir(), 2*time.Minute, 7*24*time.Hour)
	now := time.Now()
	cases := []struct {
		name string
		st   Status
		at   time.Time
		want bool
	}{
		{"sold_recent", StatusSold, now.Add(-time.Hour), true},
		{"sold_expired", StatusSold, now.Add(-8 * 24 * time.Hour), false},
		{"taken_recent", StatusTaken, now.Add(-time.Hour), true},
		{"taken_expired", StatusTaken, now.Add(-7 * time.Hour), false},
		{"reserved_recent", StatusReserved, now.Add(-time.Hour), true},
		{"reserved_expired", StatusReserved, now.Add(-25 * time.Hour), false},
		{"available_recent", StatusAvailable, now.Add(-time.Minute), true},
		{"available_expired", StatusAvailable, now.Add(-3 * time.Minute), false},
		{"auction_recent", StatusAuction, now.Add(-time.Minute), true},
		{"unknown_never", StatusUnknown, now, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.fresh(tc.st, tc.at); got != tc.want {
				t.Fatalf("fresh(%s, %v) = %v, want %v", tc.st, tc.at, got, tc.want)
			}
		})
	}
}

func TestCheckerCacheAvoidsSecondRequest(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		fmt.Fprint(w, forSalePageFixture)
	}))
	defer srv.Close()

	oldTemplate := FragmentURLTemplate
	FragmentURLTemplate = srv.URL + "/username/%s"
	defer func() { FragmentURLTemplate = oldTemplate }()

	c := New(Config{Workers: 1, Retries: 0, Timeout: 3 * time.Second, CacheDir: t.TempDir()})
	defer c.Close()

	first := c.CheckOne(context.Background(), "forsale")
	if first.Status != StatusAvailable || first.Cached {
		t.Fatalf("first check = %+v, want fresh available", first)
	}
	if first.Confidence < 95 || first.Evidence == "" {
		t.Fatalf("first check lacks confidence/evidence: %+v", first)
	}

	second := c.CheckOne(context.Background(), "forsale")
	if second.Status != StatusAvailable || !second.Cached {
		t.Fatalf("second check = %+v, want cached available", second)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("server requests = %d, want 1 (cache hit)", got)
	}
	if c.Snapshot().CacheHits != 1 {
		t.Fatalf("cache_hits = %d, want 1", c.Snapshot().CacheHits)
	}
}

func TestCheckerServesStaleCacheOnNetworkFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, soldPageFixture)
	}))

	oldTemplate := FragmentURLTemplate
	FragmentURLTemplate = srv.URL + "/username/%s"
	defer func() { FragmentURLTemplate = oldTemplate }()

	c := New(Config{
		Workers: 1, Retries: 0, Timeout: 2 * time.Second,
		CacheDir:     t.TempDir(),
		CacheSoldTTL: time.Nanosecond, // force the entry to be stale immediately
	})
	defer c.Close()

	first := c.CheckOne(context.Background(), "adam")
	if first.Status != StatusSold || first.Cached {
		t.Fatalf("first check = %+v, want fresh sold", first)
	}

	srv.Close()

	second := c.CheckOne(context.Background(), "adam")
	if !second.Cached || !second.Stale {
		t.Fatalf("second check = %+v, want stale cached", second)
	}
	if second.Status != StatusSold {
		t.Fatalf("second status = %q, want sold", second.Status)
	}
	if second.Error == "" {
		t.Fatal("stale fallback should explain the network failure in Error")
	}
	if c.Snapshot().CacheStale == 0 {
		t.Fatal("cache_stale counter was not incremented")
	}
}
