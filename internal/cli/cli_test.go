package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func cliPage(label, class, extra string) string {
	return `<html><body><h2><span class="tm-section-header-status tm-status-` + class + `">` +
		label + `</span></h2>` + extra + `</body></html>`
}

func TestParseStatusInfo(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		want      Status
		wantFound bool
		minConf   int
	}{
		{
			name: "auction",
			body: cliPage("On auction", "avail",
				`<time class="tm-countdown-timer">5d</time><button class="js-place-bid-btn">Place bid</button>`),
			want: StatusAuction, wantFound: true, minConf: 95,
		},
		{
			name: "for_sale_is_available",
			body: cliPage("For sale", "avail",
				`<time class="tm-countdown-timer">361d</time><button class="js-buy-now-btn">Buy for 7</button>`),
			want: StatusAvailable, wantFound: true, minConf: 95,
		},
		{
			name: "taken",
			body: cliPage("Taken", "taken", `<button class="js-make-offer-btn">Make an offer</button>`),
			want: StatusTaken, wantFound: true, minConf: 95,
		},
		{
			name: "sold",
			body: cliPage("Sold", "unavail", `<div>Purchased on 19 Nov 2022</div>`),
			want: StatusSold, wantFound: true, minConf: 95,
		},
		{
			name: "timer_alone_is_not_auction",
			body: cliPage("", "avail", `<time class="tm-countdown-timer">2h</time>`),
			want: StatusUnknown, wantFound: false, minConf: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st, _, found, conf, evidence := parseStatusInfo([]byte(c.body))
			if st != c.want || found != c.wantFound {
				t.Fatalf("got (%q, found=%v), want (%q, found=%v); evidence=%s", st, found, c.want, c.wantFound, evidence)
			}
			if conf < c.minConf {
				t.Fatalf("confidence=%d, want >= %d; evidence=%s", conf, c.minConf, evidence)
			}
			if found && !strings.Contains(evidence, "text=") && !strings.Contains(evidence, "buy-now") {
				t.Fatalf("unexpected evidence: %q", evidence)
			}
		})
	}
}

func TestPageCacheRoundTrip(t *testing.T) {
	c := newPageCache(t.TempDir(), 0, 0)
	if c == nil {
		t.Fatal("cache must be enabled for a non-empty dir")
	}
	body := []byte(cliPage("Sold", "unavail", `<div>Purchased on 19 Nov 2022</div>`))
	st, _, found, conf, ev := parseStatusInfo(body)
	if !found {
		t.Fatal("fixture did not parse")
	}
	c.put("adam", "https://fragment.com/username/adam", st, conf, ev, body)

	ent, got, ok := c.get("adam")
	if !ok || string(got) != string(body) {
		t.Fatalf("cache round-trip failed (ok=%v)", ok)
	}
	if ent.Status != StatusSold {
		t.Fatalf("cached status = %q, want sold", ent.Status)
	}
	if !c.fresh(StatusSold, ent.FetchedAt) {
		t.Fatal("fresh sold entry reported stale")
	}
	if c.fresh(StatusSold, time.Now().Add(-8*24*time.Hour)) {
		t.Fatal("8-day-old sold entry must be stale with the default TTL")
	}
}

func TestReservedRedirectRetriesWithCookie(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			http.SetCookie(w, &http.Cookie{Name: "stel_ssid", Value: "abc", Path: "/"})
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		if c, err := r.Cookie("stel_ssid"); err != nil || c.Value != "abc" {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		_, _ = w.Write([]byte(cliPage("Sold", "unavail", `<div>Purchased on 19 Nov 2022</div>`)))
	}))
	defer srv.Close()

	old := fragmentURLTemplate
	fragmentURLTemplate = srv.URL + "/username/%s"
	defer func() { fragmentURLTemplate = old }()

	client := newHTTPClient(httpConfig{maxConns: 2, timeout: 2 * time.Second,
		dialTimeout: time.Second, tlsTimeout: time.Second,
		respHdrTimeout: time.Second, idleTimeout: time.Second})
	stats := newStats()
	res := checkOne(context.Background(), "adam", client, stats, checkConfig{
		retries: 2, retryDelay: time.Millisecond, blockCooldown: time.Millisecond,
	})
	if res.Status != StatusSold {
		t.Fatalf("status = %q, want sold", res.Status)
	}
	if calls < 2 {
		t.Fatalf("calls = %d, want >= 2", calls)
	}
}

func TestReservedRedirectPersistent(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Redirect(w, r, "/", http.StatusFound)
	}))
	defer srv.Close()

	old := fragmentURLTemplate
	fragmentURLTemplate = srv.URL + "/username/%s"
	defer func() { fragmentURLTemplate = old }()

	client := newHTTPClient(httpConfig{maxConns: 2, timeout: 2 * time.Second,
		dialTimeout: time.Second, tlsTimeout: time.Second,
		respHdrTimeout: time.Second, idleTimeout: time.Second})
	res := checkOne(context.Background(), "tg", client, newStats(), checkConfig{
		retries: 2, retryDelay: time.Millisecond, blockCooldown: time.Millisecond,
	})
	if res.Status != StatusError {
		t.Fatalf("status = %q, want error (throttled)", res.Status)
	}
	if calls != 4 {
		t.Fatalf("calls = %d, want 4 (3 attempts + probe)", calls)
	}
}

func TestReservedGenuineWithHealthyProbe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/username/zzprobe") {
			_, _ = w.Write([]byte(cliPage("Sold", "unavail", `<div>Purchased on 1 Jan 2026</div>`)))
			return
		}
		http.Redirect(w, r, "/", http.StatusFound)
	}))
	defer srv.Close()

	old := fragmentURLTemplate
	fragmentURLTemplate = srv.URL + "/username/%s"
	defer func() { fragmentURLTemplate = old }()

	client := newHTTPClient(httpConfig{maxConns: 2, timeout: 2 * time.Second,
		dialTimeout: time.Second, tlsTimeout: time.Second,
		respHdrTimeout: time.Second, idleTimeout: time.Second})
	res := checkOne(context.Background(), "tg", client, newStats(), checkConfig{
		retries: 2, retryDelay: time.Millisecond, blockCooldown: time.Millisecond,
	})
	if res.Status != StatusReserved {
		t.Fatalf("status = %q, want reserved", res.Status)
	}
}

func TestParseBannedAndSold(t *testing.T) {
	body := `<h2><span class="tm-section-header-status tm-status-banned">BANNED</span>` +
		`<span class="tm-section-header-status tm-status-unavail">Sold</span></h2>` +
		`<div>Purchased on 7 Feb 2026</div>`
	st, _, found, conf, ev := parseStatusInfo([]byte(body))
	if st != StatusSold || !found || conf < 95 {
		t.Fatalf("got (%q, found=%v, conf=%d), want sold/true/>=95; %s", st, found, conf, ev)
	}
	if !strings.Contains(ev, "badges=") {
		t.Fatalf("evidence does not mention multiple badges: %s", ev)
	}
}

func TestParseAuctionWithBuyNow(t *testing.T) {
	body := cliPage("On auction", "avail",
		`<time class="tm-countdown-timer">2d</time>`+
			`<button class="js-place-bid-btn">Place bid</button>`+
			`<button class="js-buy-now-btn">Buy now</button>`)
	st, _, found, conf, ev := parseStatusInfo([]byte(body))
	if st != StatusAuction || !found || conf < 95 {
		t.Fatalf("got (%q, found=%v, conf=%d), want auction/true/>=95; %s", st, found, conf, ev)
	}
}
