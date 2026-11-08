// Regression tests for bugs found in the v4 review. Each test describes the
// old behaviour it guards against.
package checker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// withServer points FragmentURLTemplate at a local handler for one test.
func withServer(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	old := FragmentURLTemplate
	FragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() { FragmentURLTemplate = old; srv.Close() })
	return srv
}

// After a cancel, the checks that were in flight came back as
// Status=error/Error="cancelled", were counted in Stats.Errors and were
// still sent on out (select picked randomly between ctx.Done and out<-r).
// The web worker then stored them as results and flagged the names done.
func TestCancelledChecksAreNotEmitted(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
			return
		}
		http.Redirect(w, r, "/?query=x", http.StatusFound)
	})
	emitted, errors := 0, int64(0)
	for round := 0; round < 5; round++ {
		c := New(Config{Workers: 20, Retries: 0, Timeout: 5 * time.Second})
		in := make(chan string, 64)
		out := make(chan Result, 64)
		for i := 0; i < 40; i++ {
			in <- fmt.Sprintf("name%02d", i)
		}
		close(in)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(150 * time.Millisecond); cancel() }()
		c.Run(ctx, in, out)
		close(out)
		for r := range out {
			if r.Status == StatusError {
				emitted++
			}
		}
		errors += c.Snapshot().Errors
		c.Close()
	}
	if emitted > 0 || errors > 0 {
		t.Errorf("over 5 cancelled runs: %d interrupted checks were emitted as errors, Stats.Errors=%d", emitted, errors)
	}
}

// read_limit came straight from the REST API into make([]byte, n). A huge
// value panicked inside a worker goroutine and took the whole web process
// down, with every other worker.
func TestHugeReadLimitDoesNotPanic(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<span class="tm-section-header-status tm-status-unavail">Sold</span>`)
	})
	c := New(Config{ReadLimit: 1 << 62, Retries: 0})
	defer c.Close()
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("CheckOne panicked: %v", p)
		}
	}()
	if r := c.CheckOne(context.Background(), "adam"); r.Status != StatusSold {
		t.Errorf("status = %s (%s), want sold", r.Status, r.Error)
	}
}

func TestNormalizeConfigClampsUpperBounds(t *testing.T) {
	c := normalizeConfig(Config{Workers: 1 << 30, MaxConns: 1 << 30, RateRPS: 1 << 62, Retries: 1 << 20, ReadLimit: 1 << 62})
	if c.Workers != MaxWorkers || c.MaxConns != MaxConnLimit || c.RateRPS != MaxRateRPS ||
		c.Retries != MaxRetries || c.ReadLimit != MaxReadLimit {
		t.Errorf("not clamped: %+v", c)
	}
	if err := (Config{ReadLimit: 1 << 62}).Validate(); err == nil {
		t.Error("Validate accepted read_limit=1<<62")
	}
	if err := DefaultConfig().Validate(); err != nil {
		t.Errorf("Validate rejected the default config: %v", err)
	}
}

// waitBlock had a loop meant to re-read the shared deadline, but both
// select branches returned, so a cooldown extended by another worker while
// we slept was ignored and the request went out during the pause.
func TestWaitBlockHonoursExtendedCooldown(t *testing.T) {
	c := New(Config{BlockCooldown: 50 * time.Millisecond})
	defer c.Close()
	start := time.Now()
	c.triggerBlock() // deadline = start + 50ms
	done := make(chan time.Duration, 1)
	go func() {
		_ = c.waitBlock(context.Background())
		done <- time.Since(start)
	}()
	time.Sleep(10 * time.Millisecond)
	c.cfg.BlockCooldown = 300 * time.Millisecond
	c.triggerBlock() // another worker saw "302 /": deadline = start + ~310ms
	if got := <-done; got < 250*time.Millisecond {
		t.Errorf("waitBlock returned after %v, but the shared cooldown was extended to ~310ms", got.Round(time.Millisecond))
	}
}
