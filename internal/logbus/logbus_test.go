package logbus

import (
	"sync"
	"testing"
	"time"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]Level{
		"error": LevelError, "err": LevelError, "ERROR": LevelError,
		"warn": LevelWarn, "warning": LevelWarn,
		"info":  LevelInfo,
		"debug": LevelDebug,
		"trace": LevelTrace,
		"":      LevelInfo, // unknown -> info
		"bogus": LevelInfo,
	}
	for in, want := range cases {
		if got := ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestLevelFilter(t *testing.T) {
	b := New(16, LevelWarn)
	b.Publish(LevelError, "", "err")
	b.Publish(LevelWarn, "", "warn")
	b.Publish(LevelInfo, "", "info")   // dropped
	b.Publish(LevelDebug, "", "debug") // dropped
	got := b.Recent(0, "")
	if len(got) != 2 {
		t.Fatalf("Recent len = %d, want 2 (only error+warn)", len(got))
	}
	if got[0].Message != "err" || got[1].Message != "warn" {
		t.Fatalf("unexpected entries: %+v", got)
	}
}

func TestRingBufferEviction(t *testing.T) {
	const cap = 16
	b := New(cap, LevelTrace)
	for i := 0; i < cap+5; i++ {
		b.Publish(LevelInfo, "", "msg")
	}
	got := b.Recent(0, "")
	if len(got) != cap {
		t.Fatalf("Recent len = %d, want %d (ring should evict)", len(got), cap)
	}
}

func TestSubscribeTopicFilter(t *testing.T) {
	b := New(16, LevelTrace)
	chA, cancelA := b.Subscribe("A", 8)
	chB, cancelB := b.Subscribe("B", 8)
	chAll, cancelAll := b.Subscribe("", 8)
	defer cancelA()
	defer cancelB()
	defer cancelAll()

	b.Publish(LevelInfo, "A", "for-a")
	b.Publish(LevelInfo, "B", "for-b")
	b.Publish(LevelInfo, "", "global")

	receive := func(ch <-chan Entry, want int) []Entry {
		var out []Entry
		deadline := time.After(500 * time.Millisecond)
		for len(out) < want {
			select {
			case e := <-ch:
				out = append(out, e)
			case <-deadline:
				return out
			}
		}
		return out
	}
	if got := receive(chA, 1); len(got) != 1 || got[0].Topic != "A" {
		t.Errorf("chA got %+v, want one entry for A", got)
	}
	if got := receive(chB, 1); len(got) != 1 || got[0].Topic != "B" {
		t.Errorf("chB got %+v, want one entry for B", got)
	}
	if got := receive(chAll, 3); len(got) != 3 {
		t.Errorf("chAll got %d entries, want 3", len(got))
	}
}

// TestCloseDoesNotPanicConcurrentPublish exercises the previously-fragile
// shutdown path: many publishers + a Close racing against them must not
// panic on send-to-closed-channel.
func TestCloseDoesNotPanicConcurrentPublish(t *testing.T) {
	for trial := 0; trial < 20; trial++ {
		b := New(64, LevelTrace)
		_, _ = b.Subscribe("", 32)
		_, _ = b.Subscribe("worker-1", 32)
		var wg sync.WaitGroup
		for p := 0; p < 4; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					b.Publish(LevelInfo, "worker-1", "x")
				}
			}()
		}
		// Close mid-flight.
		time.Sleep(time.Millisecond)
		b.Close()
		wg.Wait()
	}
}

// TestSubscribeAfterClose returns a closed channel and a no-op cancel.
func TestSubscribeAfterClose(t *testing.T) {
	b := New(16, LevelInfo)
	b.Close()
	ch, cancel := b.Subscribe("", 8)
	cancel() // must be a no-op
	if _, ok := <-ch; ok {
		t.Fatalf("expected pre-closed channel")
	}
}
