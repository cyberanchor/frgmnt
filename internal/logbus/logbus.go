// Package logbus is a small in-memory event bus with a bounded ring buffer
// and SSE-friendly fan-out. The web layer uses it for two purposes:
//
//   - a global debug console (all log entries),
//   - per-worker progress/result streams (Topic == workerID).
//
// The implementation is intentionally dependency-free and goroutine-safe.
package logbus

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Level is a syslog-ish severity. Higher values are more verbose.
type Level int

const (
	LevelError Level = iota
	LevelWarn
	LevelInfo
	LevelDebug
	LevelTrace
)

// String returns the short uppercase label used in log output.
func (l Level) String() string {
	switch l {
	case LevelError:
		return "ERROR"
	case LevelWarn:
		return "WARN"
	case LevelInfo:
		return "INFO"
	case LevelDebug:
		return "DEBUG"
	case LevelTrace:
		return "TRACE"
	default:
		return "?"
	}
}

// ParseLevel converts a CLI string to a Level. Unknown values fall back to
// LevelInfo so the binary never refuses to start because of a typo.
func ParseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "error", "err":
		return LevelError
	case "warn", "warning":
		return LevelWarn
	case "info":
		return LevelInfo
	case "debug":
		return LevelDebug
	case "trace":
		return LevelTrace
	default:
		return LevelInfo
	}
}

// Entry is one event on the bus. Topic is empty for global events and
// equals the worker ID for per-worker events.
type Entry struct {
	ID      int64     `json:"id"`
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Topic   string    `json:"topic,omitempty"`
	Message string    `json:"message"`
}

// Bus is the event bus. The zero value is not usable — call New.
//
// Locking:
//   - mu  guards the ring buffer and the current level.
//   - subsMu guards the subscribers map AND the closed flag. The flag
//     and the map mutate together so they live under the same lock to
//     avoid Subscribe-after-Close from leaking goroutines.
type Bus struct {
	level Level

	mu     sync.RWMutex
	ring   []Entry
	head   int // next write position
	size   int // current number of entries
	cap    int
	seq    atomic.Int64
	subs   map[*subscriber]struct{}
	subsMu sync.RWMutex
	closed bool
}

// New returns a Bus that keeps the most recent `capacity` entries in memory
// and filters anything more verbose than `level`.
func New(capacity int, level Level) *Bus {
	if capacity < 16 {
		capacity = 16
	}
	return &Bus{
		level: level,
		ring:  make([]Entry, capacity),
		cap:   capacity,
		subs:  make(map[*subscriber]struct{}),
	}
}

// SetLevel updates the active filter level at runtime.
func (b *Bus) SetLevel(l Level) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.level = l
}

// Level returns the current filter level.
func (b *Bus) Level() Level {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.level
}

// Publish appends a log entry. Entries below the current level are dropped.
func (b *Bus) Publish(level Level, topic, msg string) {
	b.mu.RLock()
	if level > b.level {
		b.mu.RUnlock()
		return
	}
	b.mu.RUnlock()

	e := Entry{
		ID:      b.seq.Add(1),
		Time:    time.Now(),
		Level:   level.String(),
		Topic:   topic,
		Message: msg,
	}

	b.mu.Lock()
	b.ring[b.head] = e
	b.head = (b.head + 1) % b.cap
	if b.size < b.cap {
		b.size++
	}
	b.mu.Unlock()

	b.fanout(e)
}

// Recent returns up to n most recent entries, oldest first. If topic is
// non-empty only entries with that topic are returned.
func (b *Bus) Recent(n int, topic string) []Entry {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if n <= 0 || n > b.size {
		n = b.size
	}
	out := make([]Entry, 0, n)
	// Walk newest -> oldest, collect up to n matches, then reverse.
	idx := (b.head - 1 + b.cap) % b.cap
	for i := 0; i < b.size && len(out) < n; i++ {
		e := b.ring[idx]
		if topic == "" || e.Topic == topic {
			out = append(out, e)
		}
		idx = (idx - 1 + b.cap) % b.cap
	}
	// Reverse so callers see oldest first.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// -----------------------------------------------------------------------------
// Subscriptions
// -----------------------------------------------------------------------------

type subscriber struct {
	ch    chan Entry
	topic string // "" = all topics
}

// Subscribe returns a channel that receives every future entry matching
// topic ("" matches all). The returned cancel func unsubscribes and closes
// the channel. The channel buffer is bounded; slow consumers lose events
// rather than blocking the producer.
//
// Subscribing on a closed bus returns a pre-closed channel and a no-op
// cancel so callers can use the API uniformly during shutdown.
func (b *Bus) Subscribe(topic string, buffer int) (<-chan Entry, func()) {
	if buffer < 16 {
		buffer = 16
	}
	s := &subscriber{ch: make(chan Entry, buffer), topic: topic}
	b.subsMu.Lock()
	if b.closed {
		b.subsMu.Unlock()
		close(s.ch)
		return s.ch, func() {}
	}
	b.subs[s] = struct{}{}
	b.subsMu.Unlock()
	cancel := func() {
		b.subsMu.Lock()
		if _, ok := b.subs[s]; ok {
			delete(b.subs, s)
			close(s.ch)
		}
		b.subsMu.Unlock()
	}
	return s.ch, cancel
}

func (b *Bus) fanout(e Entry) {
	b.subsMu.RLock()
	defer b.subsMu.RUnlock()
	for s := range b.subs {
		if s.topic != "" && s.topic != e.Topic {
			continue
		}
		select {
		case s.ch <- e:
		default:
			// Slow consumer: drop. The web UI can re-fetch via /recent.
		}
	}
}

// Close releases every subscriber. Safe to call multiple times.
//
// Close serializes with Subscribe and fanout via subsMu, so it cannot
// race with a Publish in flight: fanout holds subsMu.RLock while iterating,
// and Close takes subsMu.Lock before closing any channel.
func (b *Bus) Close() {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for s := range b.subs {
		delete(b.subs, s)
		close(s.ch)
	}
}
