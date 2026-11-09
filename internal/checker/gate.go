package checker

import (
	"context"
	"sync"
)

// gate lets requests through one at a time until it is opened. A run
// starts with it closed: until the first outcome that is not a Cloudflare
// challenge, one request goes out at a time, so a client that is already
// blocked learns it after ChallengeLimit requests instead of sending one
// per worker at once. A throttle pause closes it again, so that after the
// pause one request tests whether Fragment answers normally before the
// others follow.
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
	epoch  uint64        // how many times the gate has been closed again
	opened chan struct{} // closed when the gate opens; a new one when it closes again
	slot   chan struct{} // capacity 1: the request holding it goes alone
}

func newGate() *gate {
	return &gate{opened: make(chan struct{}), slot: make(chan struct{}, 1)}
}

// ticket is a pass through the gate. Release it once, when the request
// is done.
type ticket struct {
	g     *gate
	epoch uint64
	scout bool // holds the slot: the request goes alone
}

func (t ticket) release() {
	if t.scout {
		<-t.g.slot
	}
}

// pass returns when the caller may send its request: at once when the
// gate is open, otherwise when the caller gets the slot (it goes alone) or
// the gate opens. It gives up when ctx is done or stop is closed.
func (g *gate) pass(ctx context.Context, stop <-chan struct{}) (ticket, error) {
	g.mu.Lock()
	t := ticket{g: g, epoch: g.epoch}
	if g.open {
		g.mu.Unlock()
		return t, nil
	}
	opened := g.opened
	g.mu.Unlock()
	select {
	case g.slot <- struct{}{}:
		t.scout = true
		return t, nil
	case <-opened:
		return t, nil
	case <-stop:
		return t, errBlocked
	case <-ctx.Done():
		return t, ctx.Err()
	}
}

// closedSince reports whether the gate was closed again after t was
// issued: the request that holds t must go through the gate once more.
func (g *gate) closedSince(t ticket) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.epoch != t.epoch
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
		g.epoch++
		g.opened = make(chan struct{})
	}
}

// IsOpen reports whether requests go through without waiting.
func (g *gate) IsOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.open
}
