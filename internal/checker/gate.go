package checker

import (
	"context"
	"sync"
)

// gate lets requests through one at a time until it is opened. A run
// starts with it closed: until the first outcome that is not a Cloudflare
// challenge, one request goes out at a time, so a client that is already
// blocked learns it after ChallengeLimit requests instead of sending one
// per worker at once.
//
// The gate used to be a flag plus a one-slot channel. A request that saw
// the flag unset queued for the slot and stayed in that queue after the
// flag was set, so the requests that had queued at the start still went
// one by one: with the second request slow (2 s), 30 names took 4.0 s
// instead of 2.2 s on the local stand-in. Waiters now wake up as soon as
// the gate opens.
type gate struct {
	mu     sync.Mutex
	open   bool
	opened chan struct{} // closed when the gate opens; a new one when it closes again
	slot   chan struct{} // capacity 1: the request holding it goes alone
}

func newGate() *gate {
	return &gate{opened: make(chan struct{}), slot: make(chan struct{}, 1)}
}

func noRelease() {}

// pass returns when the caller may send its request: at once when the
// gate is open, otherwise when the caller gets the slot (it goes alone) or
// the gate opens. Call release when the request is done.
func (g *gate) pass(ctx context.Context) (release func(), err error) {
	g.mu.Lock()
	if g.open {
		g.mu.Unlock()
		return noRelease, nil
	}
	opened := g.opened
	g.mu.Unlock()
	select {
	case g.slot <- struct{}{}:
		return func() { <-g.slot }, nil
	case <-opened:
		return noRelease, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Open lets every request through and wakes the ones that wait.
func (g *gate) Open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open {
		g.open = true
		close(g.opened)
	}
}

// Close makes requests go one at a time again until the next Open.
func (g *gate) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.open {
		g.open = false
		g.opened = make(chan struct{})
	}
}

// IsOpen reports whether requests go through without waiting.
func (g *gate) IsOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.open
}
