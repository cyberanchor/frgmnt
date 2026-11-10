package checker

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The events of a name had only its name to go by: two checks of the same
// name, or a probe made for one name, could not be told apart. Every
// event of one check carries its nid now, every request an aid of its
// own, and the events of a run the worker that made them (w).
func TestTraceIDsTieEventsTogether(t *testing.T) {
	var tries sync.Map
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		n, _ := tries.LoadOrStore(name, new(atomic.Int32))
		if name == "retryme1" && n.(*atomic.Int32).Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		freeRedirect(w, r)
	})
	var log eventLog
	c := New(Config{Workers: 2, Retries: 1, RetryDelay: 30 * time.Millisecond, Timeout: 3 * time.Second})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	in := make(chan string, 3)
	for _, n := range []string{"plain001", "retryme1", "plain002"} {
		in <- n
	}
	close(in)
	out := make(chan Result, 3)
	c.Run(context.Background(), in, out)

	nidOf := map[string]int64{}
	for _, e := range log.of("result") {
		if e.NID == 0 || e.Worker < 1 || e.Worker > 2 {
			t.Errorf("result %s: nid %d, w %d", e.Name, e.NID, e.Worker)
		}
		nidOf[e.Name] = e.NID
	}
	aids := map[int64]bool{}
	for _, e := range log.of("attempt") {
		if e.NID != nidOf[e.Name] || e.AID == 0 || aids[e.AID] || e.Worker == 0 {
			t.Errorf("attempt %s #%d: nid %d (result %d), aid %d, w %d", e.Name, e.Try, e.NID, nidOf[e.Name], e.AID, e.Worker)
		}
		aids[e.AID] = true
		if e.Name == "retryme1" && e.Try == 2 {
			if e.RetryReason != "HTTP 503" || e.RetryDelayMs < 25 {
				t.Errorf("the retry says why and how long it waited: %q after %.1f ms", e.RetryReason, e.RetryDelayMs)
			}
		}
	}
	if len(aids) != 4 {
		t.Errorf("%d attempts, want 4 (one name tried twice)", len(aids))
	}
	if nidOf["plain001"] == nidOf["plain002"] {
		t.Error("two names share a nid")
	}
	// Outside Run there is no worker number.
	_ = c.CheckOne(context.Background(), "plain003")
	if r := log.of("result"); r[len(r)-1].Worker != 0 {
		t.Errorf("CheckOne outside Run: w %d, want 0", r[len(r)-1].Worker)
	}
}

// A reserved name asks the throttle probe; a second ask within a second
// takes the verdict of the probe already made. That reuse left no trace:
// a "healthy" verdict appeared with no probe behind it. It is a probe
// event with reused and the age now, and the try after it tells how long
// it waited for the probe.
func TestProbeReuseIsTraced(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		if strings.HasPrefix(name, "rsv") {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		freeRedirect(w, r)
	})
	var log eventLog
	c := New(Config{Workers: 1, Retries: 1, RetryDelay: time.Millisecond, Timeout: 3 * time.Second})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	if res := c.CheckOne(context.Background(), "rsvname1"); res.Status != StatusReserved {
		t.Fatalf("rsvname1: %s %s", res.Status, res.Error)
	}
	var made, reused int
	for _, e := range log.of("probe") {
		if e.Reused {
			reused++
			if e.Healthy == nil || !*e.Healthy || e.AgeMs <= 0 || e.NID == 0 {
				t.Errorf("reused probe %+v", e)
			}
		} else {
			made++
		}
	}
	if made == 0 || reused == 0 {
		t.Errorf("%d probes made, %d reused; want both", made, reused)
	}
	for _, e := range log.of("attempt") {
		if e.Try == 2 && (e.ProbeWaitMs <= 0 || e.RetryReason != "redirect to /") {
			t.Errorf("second try: probe wait %.2f ms, reason %q", e.ProbeWaitMs, e.RetryReason)
		}
	}
}

// Stats.WaitProbe counted the worker that made the probe as well, which
// was also in InFlight, so the progress line showed it twice. It counts
// the workers waiting for another one's probe only.
func TestWaitProbeCountsOnlyWaiters(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	t.Cleanup(free)
	var probing atomic.Bool
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		switch {
		case strings.HasPrefix(name, "rsv"):
			http.Redirect(w, r, "/", http.StatusFound)
		case strings.HasPrefix(name, "zzprobe") && !probing.Swap(true):
			<-release
			freeRedirect(w, r)
		default:
			freeRedirect(w, r)
		}
	})
	c := New(Config{Workers: 2, MaxConns: 4, Retries: 0, Timeout: 5 * time.Second})
	defer c.Close()
	if res := c.CheckOne(context.Background(), "opener1"); res.Status != StatusFree {
		t.Fatalf("opener: %s", res.Status)
	}
	done := make(chan struct{}, 2)
	go func() { _ = c.CheckOne(context.Background(), "rsvhold1"); done <- struct{}{} }()
	waitFor(t, "the probe at the server", probing.Load)
	go func() { _ = c.CheckOne(context.Background(), "rsvwait1"); done <- struct{}{} }()
	waitFor(t, "a worker waiting for the probe", func() bool { return c.Snapshot().WaitProbe == 1 })
	if st := c.Snapshot(); st.InFlight != 1 {
		t.Errorf("in flight %d, want 1 (the probe); wait_probe %d", st.InFlight, st.WaitProbe)
	}
	free()
	<-done
	<-done
	if st := c.Snapshot(); st.WaitProbe != 0 || st.InFlight != 0 {
		t.Errorf("after: wait_probe %d, in flight %d", st.WaitProbe, st.InFlight)
	}
}

// A challenge in the trace says how many came in a row and how many stop
// the run.
func TestChallengeRunInTheTrace(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cf-Mitigated", "challenge")
		w.WriteHeader(http.StatusForbidden)
	})
	var log eventLog
	c := New(Config{Workers: 1, Retries: 0, Timeout: 3 * time.Second, BlockCooldown: time.Millisecond})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	_ = c.CheckOne(context.Background(), "chal0001")
	_ = c.CheckOne(context.Background(), "chal0002")
	a := log.of("attempt")
	if len(a) != 2 || a[0].ChallengeRun != 1 || a[1].ChallengeRun != 2 || a[1].ChallengeLimit != ChallengeLimit {
		t.Errorf("challenge runs: %+v", a)
	}
}
