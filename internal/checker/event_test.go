package checker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// eventLog collects the structured trace of a checker.
type eventLog struct {
	mu     sync.Mutex
	events []TraceEvent
}

func (l *eventLog) add(e TraceEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) all() []TraceEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]TraceEvent(nil), l.events...)
}

func (l *eventLog) of(kind string) []TraceEvent {
	var out []TraceEvent
	for _, e := range l.all() {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

// jsonLines marshals every event as the hosts do and fails on a cookie
// value in any of them.
func (l *eventLog) jsonLines(t *testing.T, secret string) string {
	t.Helper()
	var b strings.Builder
	for _, e := range l.all() {
		line, err := json.Marshal(e)
		if err != nil {
			t.Fatalf("marshal %+v: %v", e, err)
		}
		if secret != "" && strings.Contains(string(line), secret) {
			t.Fatalf("a cookie value reached the trace: %s", line)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// The structured trace has a record for every HTTP try of a name, with
// the request, the waits, every phase and what the answer said, and one
// for the end of the name with its times split.
func TestTraceEventsOfARetriedName(t *testing.T) {
	var n atomic.Int32
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		if n.Add(1) == 1 {
			http.SetCookie(w, &http.Cookie{Name: "stel_ssid", Value: "SECRET123", Path: "/"})
			w.Header().Set("Cf-Ray", "8f00test-AMS")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	})
	var log eventLog
	c := New(Config{Workers: 1, Retries: 1, RetryDelay: 50 * time.Millisecond, Timeout: 2 * time.Second})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	if res := c.CheckOne(context.Background(), "evname"); res.Status != StatusFree {
		t.Fatalf("%s %s", res.Status, res.Error)
	}
	tries := log.of("attempt")
	if len(tries) != 2 {
		t.Fatalf("%d attempt events, want 2: %+v", len(tries), log.all())
	}
	first, second := tries[0], tries[1]
	if first.Try != 1 || first.Code != 503 || !first.Retry || first.Error != "HTTP 503" || first.CFRay != "8f00test-AMS" ||
		len(first.SetCookies) != 1 || first.SetCookies[0] != "stel_ssid" || !strings.HasSuffix(first.URL, "/username/evname") {
		t.Errorf("first try: %+v", first)
	}
	if second.Try != 2 || second.Code != 302 || second.Status != StatusFree || second.Retry ||
		second.Location != "/?query=evname" || len(second.CookiesSent) != 1 || !second.ConnReused {
		t.Errorf("second try: %+v", second)
	}
	for _, e := range tries {
		if e.NetMs <= 0 || e.TTFBMs <= 0 || e.Proto != "HTTP/1.1" || e.Remote == "" || e.Time.IsZero() {
			t.Errorf("try %d lacks timings or connection: %+v", e.Try, e)
		}
	}
	results := log.of("result")
	if len(results) != 1 {
		t.Fatalf("%d result events, want 1", len(results))
	}
	if r := results[0]; r.Name != "evname" || r.Status != StatusFree || r.Attempts != 2 || r.WaitMs < 50 ||
		r.TookMs < r.NetMs || r.NetMs <= 0 {
		t.Errorf("result: %+v", r)
	}
	if out := log.jsonLines(t, "SECRET123"); !strings.Contains(out, `"kind":"attempt"`) {
		t.Errorf("JSON lines: %s", out)
	}
}

// A throttled spell shows up step by step: the "302 /" try, the probe that
// is redirected to "/" too, the pause with its round and length, the gate
// closing and opening again, the end of the spell, and the name's verdict.
func TestTraceEventsOfAThrottleSpell(t *testing.T) {
	var throttled atomic.Bool
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		if throttled.Load() {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	})
	var log eventLog
	c := New(Config{Workers: 1, Retries: 0, BlockCooldown: 50 * time.Millisecond, ThrottleMax: 10 * time.Second,
		Timeout: 2 * time.Second})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	if res := c.CheckOne(context.Background(), "firstname"); res.Status != StatusFree {
		t.Fatalf("firstname: %s", res.Status)
	}
	throttled.Store(true)
	time.AfterFunc(150*time.Millisecond, func() { throttled.Store(false) })
	if res := c.CheckOne(context.Background(), "thrname"); res.Status != StatusFree {
		t.Fatalf("thrname: %s %s", res.Status, res.Error)
	}
	pauses := log.of("pause")
	if len(pauses) == 0 || pauses[0].Round != 1 || pauses[0].Reason != "throttle" || pauses[0].PauseMs != 50 {
		t.Errorf("pause events: %+v", pauses)
	}
	var gates []string
	for _, e := range log.of("gate") {
		gates = append(gates, e.Gate)
	}
	if got := strings.Join(gates, ","); !strings.HasPrefix(got, "open,closed") || !strings.HasSuffix(got, "open") {
		t.Errorf("gate events %q, want open (first answer), closed (pause), ..., open", got)
	}
	unhealthy := false
	for _, e := range log.of("probe") {
		if e.Healthy != nil && !*e.Healthy && e.Code == 302 && e.Location == "/" {
			unhealthy = true
		}
	}
	if !unhealthy {
		t.Errorf("no probe event redirected to /: %+v", log.of("probe"))
	}
	if over := log.of("throttle_over"); len(over) != 1 || over[0].Round != len(pauses) {
		t.Errorf("throttle_over events %+v after %d pauses", over, len(pauses))
	}
	_ = log.jsonLines(t, "")
}

// The stop of a run is in the trace, with the reason.
func TestTraceEventBlocked(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cf-Mitigated", "challenge")
		w.WriteHeader(http.StatusForbidden)
	})
	var log eventLog
	c := New(Config{Workers: 2, Retries: 0, Timeout: 2 * time.Second})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	in := make(chan string, 30)
	for i := 0; i < 30; i++ {
		in <- fmt.Sprintf("blockname%02d", i)
	}
	close(in)
	out := make(chan Result, 30)
	c.Run(context.Background(), in, out)
	blocked := log.of("blocked")
	if len(blocked) != 1 || blocked[0].Block != "challenge" || !strings.Contains(blocked[0].Message, "Cloudflare") {
		t.Fatalf("blocked events: %+v", blocked)
	}
	if tries := log.of("attempt"); len(tries) == 0 || tries[0].CFMitigated != "challenge" || tries[0].Code != 403 {
		t.Errorf("attempt events: %+v", tries)
	}
}

// Cache lookups are in the trace: a miss before the first request, a hit
// with the age of the entry and no request for the second check.
func TestTraceEventsOfCacheLookups(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, forSalePageFixture) })
	var log eventLog
	c := New(Config{Workers: 1, Retries: 0, Timeout: 2 * time.Second, CacheDir: t.TempDir()})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	_ = c.CheckOne(context.Background(), "forsale")
	_ = c.CheckOne(context.Background(), "forsale")
	var lookups []string
	for _, e := range log.of("cache") {
		lookups = append(lookups, e.Cache)
	}
	if strings.Join(lookups, ",") != "miss,hit" {
		t.Errorf("cache lookups %v, want miss then hit", lookups)
	}
	if hit := log.of("cache"); len(hit) == 2 && (hit[1].Status != StatusAvailable || hit[1].AgeMs < 0) {
		t.Errorf("hit event: %+v", hit[1])
	}
	if n := len(log.of("attempt")); n != 1 {
		t.Errorf("%d attempt events, want 1 (the second check is a cache hit)", n)
	}
	if rs := log.of("result"); len(rs) != 2 || !rs[1].Cached || rs[1].Attempts != 0 {
		t.Errorf("result events: %+v", rs)
	}
}
