package checker

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Logger receives optional detail about the checks. Any field may be nil.
// The console sets Debugf from -debug and Event from -trace, independently
// of each other; the web panel sends the lines to the tile's log and the
// events to the worker's trace file.
type Logger struct {
	Warnf  func(format string, args ...any) // notable events: throttle pauses, HTTP 429, the stop
	Debugf func(format string, args ...any) // one line per HTTP try with its timings, codes, redirects, verdicts, cache use

	// Event receives the structured trace: every HTTP request with all it
	// sent and got, every cache lookup and name, and the pauses, the gate
	// and the stop. nil = off. There used to be a Tracef level of text
	// lines (DNS, TCP, TLS, headers) that also turned the debug lines on;
	// at 200 workers it was unreadable, and its detail is in the events.
	Event func(TraceEvent)

	// Level, when set, is asked before every Debugf line: the panel's log
	// level can change during a run, and a line nobody will see is then
	// not even formatted. nil: Debugf always runs.
	Level func() LogLevel
}

// LogLevel is what Logger.Level answers.
type LogLevel int

// Levels for Logger.Level.
const (
	LogInfo  LogLevel = iota // no Debugf lines
	LogDebug                 // Debugf lines
)

// SetLogger installs l. Call it before the first check.
func (c *Checker) SetLogger(l Logger) { c.log = l }

// debugOn tells whether debug lines are wanted, so callers can skip work
// that only feeds them.
func (c *Checker) debugOn() bool {
	return c.log.Debugf != nil && (c.log.Level == nil || c.log.Level() >= LogDebug)
}

func (c *Checker) warnf(format string, args ...any) {
	if c.log.Warnf != nil {
		c.log.Warnf(format, args...)
	}
}

func (c *Checker) debugf(format string, args ...any) {
	if c.debugOn() {
		c.log.Debugf(format, args...)
	}
}

// Kinds of HTTP request.
const (
	kindAttempt = "attempt" // a try of a name
	kindProbe   = "probe"   // the shared throttle probe (a random free name)
)

// attempt is the record of one HTTP request, a try of a name or a probe.
// The httptrace hooks fill in the timings for every request (a few clock
// reads), and the request reports itself once at the end: a -debug line,
// and the structured trace.
type attempt struct {
	c     *Checker
	kind  string
	name  string
	try   int // HTTP try of the name, from 1; 0 for a probe
	url   string
	waits waits // before the request: the gate, a shared pause, a -rate start

	// Set on the goroutine of the request.
	start, end time.Time // request sent .. body drained
	bodyDone   time.Time
	inFlight   bool // counted in Stats.InFlight until done
	cookies    []string
	code       int
	proto      string
	header     http.Header
	bytes      int   // body bytes read for the parser (decoded)
	rawBytes   int64 // body bytes received, as sent (compressed), drained rest included
	// The bytes on the connection for this request, taken when its body
	// hit EOF: right after that an HTTP/1.1 connection goes back to the
	// pool and the next request starts on it.
	wireIn, wireOut int64
	wireSet         bool

	// What the httptrace hooks record. The dial hooks run on the
	// transport's dial goroutine, which can go on after the request has
	// returned (it was cancelled, or another connection came first), and
	// the write hooks on its write loop; mu guards h.
	mu sync.Mutex
	h  hooks
}

// hooks is what the httptrace hooks of a request record.
type hooks struct {
	getConn, gotConn          time.Time
	dnsStart, dnsDone         time.Time
	dnsAddrs                  []string
	dnsCoalesced              bool
	connectStart, connectDone time.Time
	tlsStart, tlsDone         time.Time
	wroteHeaders, wrote       time.Time
	firstByte                 time.Time
	got1xx                    []int
	waitingConn               bool // counted in Stats.WaitConn
	reused                    bool
	idle                      time.Duration
	remote, local             string
	tlsVersion, alpn          string
	k                         *conn // the connection, when it is ours
	wireIn, wireOut           int64 // its counters when the request got it
}

// waits is the time a request spent before it was sent.
type waits struct {
	gate, pause, rate time.Duration
	scout             bool // went alone through the closed gate
}

func (w waits) total() time.Duration { return w.gate + w.pause + w.rate }

func (c *Checker) newAttempt(kind, name string, try int, url string) *attempt {
	return &attempt{c: c, kind: kind, name: name, try: try, url: url}
}

// who names the request in log lines: "@name #2", or "probe zzprobe…".
func (a *attempt) who() string {
	if a.kind == kindProbe {
		return "probe " + a.name
	}
	return fmt.Sprintf("@%s #%d", a.name, a.try)
}

// begin marks the request as sent and notes the cookies the jar will add
// (names only).
func (a *attempt) begin(req *http.Request) {
	if jar := a.c.client.Jar; jar != nil {
		for _, ck := range jar.Cookies(req.URL) {
			a.cookies = append(a.cookies, ck.Name)
		}
	}
	a.start = time.Now()
	atomic.AddInt64(&a.c.stats.inFlight, 1)
	a.inFlight = true
}

// response keeps what the answer says.
func (a *attempt) response(resp *http.Response) {
	a.code = resp.StatusCode
	a.proto = resp.Proto
	a.header = resp.Header
}

// snapshot copies what the hooks recorded.
func (a *attempt) snapshot() hooks {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.h
}

// clientTrace returns the httptrace hooks of the request: they record the
// timings, the connection and its bytes, and count new and reused
// connections and the requests waiting for one.
func (a *attempt) clientTrace() *httptrace.ClientTrace {
	c := a.c
	set := func(f func(h *hooks)) {
		a.mu.Lock()
		f(&a.h)
		a.mu.Unlock()
	}
	return &httptrace.ClientTrace{
		GetConn: func(string) {
			now := time.Now()
			set(func(h *hooks) {
				h.getConn = now
				if !h.waitingConn {
					h.waitingConn = true
					atomic.AddInt64(&c.stats.waitConn, 1)
				}
			})
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			now := time.Now()
			set(func(h *hooks) { h.dnsStart = now })
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			now := time.Now()
			addrs := make([]string, len(info.Addrs))
			for i, ad := range info.Addrs {
				addrs[i] = ad.String()
			}
			set(func(h *hooks) { h.dnsDone, h.dnsAddrs, h.dnsCoalesced = now, addrs, info.Coalesced })
		},
		ConnectStart: func(network, addr string) {
			now := time.Now()
			set(func(h *hooks) {
				if h.connectStart.IsZero() {
					h.connectStart = now
				}
			})
		},
		ConnectDone: func(network, addr string, err error) {
			now := time.Now()
			set(func(h *hooks) { h.connectDone = now })
		},
		TLSHandshakeStart: func() {
			now := time.Now()
			set(func(h *hooks) { h.tlsStart = now })
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			now := time.Now()
			set(func(h *hooks) {
				h.tlsDone = now
				if err == nil {
					h.tlsVersion = tlsVersionName(state.Version)
					h.alpn = state.NegotiatedProtocol
				}
			})
		},
		GotConn: func(info httptrace.GotConnInfo) {
			now := time.Now()
			k := ourConn(info.Conn)
			var handshake time.Duration
			set(func(h *hooks) {
				h.gotConn = now
				if h.waitingConn {
					h.waitingConn = false
					atomic.AddInt64(&c.stats.waitConn, -1)
				}
				h.reused, h.idle = info.Reused, info.IdleTime
				if info.Conn != nil {
					h.remote, h.local = info.Conn.RemoteAddr().String(), info.Conn.LocalAddr().String()
				}
				h.k = k
				if k != nil {
					h.wireIn, h.wireOut = k.in.Load(), k.out.Load()
				}
				if !h.tlsStart.IsZero() && !h.tlsDone.IsZero() {
					handshake = h.tlsDone.Sub(h.tlsStart)
				}
			})
			if info.Reused {
				atomic.AddInt64(&c.stats.connReuse, 1)
			} else {
				atomic.AddInt64(&c.stats.connNew, 1)
			}
			if k != nil {
				k.requests.Add(1)
				if tc, ok := info.Conn.(*tls.Conn); ok {
					k.reportTLS(tc, handshake)
				}
			}
		},
		PutIdleConn: func(err error) {
			if err == nil {
				return
			}
			msg := "not kept idle: " + err.Error()
			a.mu.Lock()
			k := a.h.k
			a.mu.Unlock()
			if k != nil {
				k.putErr.Store(&msg)
			}
		},
		WroteHeaders: func() {
			now := time.Now()
			set(func(h *hooks) { h.wroteHeaders = now })
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			now := time.Now()
			set(func(h *hooks) { h.wrote = now })
		},
		Got1xxResponse: func(code int, _ textproto.MIMEHeader) error {
			set(func(h *hooks) { h.got1xx = append(h.got1xx, code) })
			return nil
		},
		GotFirstResponseByte: func() {
			now := time.Now()
			set(func(h *hooks) { h.firstByte = now })
		},
	}
}

// done takes the request off Stats.InFlight (and Stats.WaitConn, when it
// ended before it got a connection); it is called once the body is
// drained.
func (a *attempt) done() {
	if a.end.IsZero() {
		a.end = time.Now()
	}
	if a.inFlight {
		a.inFlight = false
		atomic.AddInt64(&a.c.stats.inFlight, -1)
	}
	a.mu.Lock()
	if a.h.waitingConn {
		a.h.waitingConn = false
		atomic.AddInt64(&a.c.stats.waitConn, -1)
	}
	a.mu.Unlock()
}

// timing is how the time of a finished request splits.
type timing struct {
	total    time.Duration // request sent .. body drained
	connWait time.Duration // waiting for a free connection (-conns), not the own dial
	dial     time.Duration // DNS, TCP and TLS of a connection this request opened
	net      time.Duration // total minus connWait: on the network
}

func (a *attempt) timing(h hooks) timing {
	var t timing
	if a.start.IsZero() || a.end.IsZero() {
		return t
	}
	t.total = a.end.Sub(a.start)
	if !h.getConn.IsZero() && !h.gotConn.IsZero() {
		if !h.reused {
			// The dial that opened this request's connection, when it ran
			// for this request (it can also be another request's dial that
			// lost the race and went to the pool).
			from, to := h.dnsStart, h.tlsDone
			if from.IsZero() {
				from = h.connectStart
			}
			if to.IsZero() {
				to = h.connectDone
			}
			if !from.IsZero() && !to.IsZero() && !from.Before(h.getConn) && !to.After(h.gotConn) {
				t.dial = to.Sub(from)
			}
		}
		t.connWait = max(h.gotConn.Sub(h.getConn)-t.dial, 0)
	}
	t.net = max(t.total-t.connWait, 0)
	return t
}

// netTime is the time of the request on the network: from sending it to
// draining the body, without the wait for a free connection.
func (a *attempt) netTime() time.Duration { return a.timing(a.snapshot()).net }

// line is the -debug summary of a finished request: what came back, the
// waits before it and every phase of it.
func (a *attempt) line(outcome string) string {
	h := a.snapshot()
	tm := a.timing(h)
	parts := []string{a.who(), "-> " + outcome}
	if a.code != 0 {
		parts = append(parts, fmt.Sprintf("%d %s", a.code, a.proto))
	}
	// Waits under a millisecond are left out: they are only the cost of
	// the checks themselves.
	var ws []string
	switch {
	case a.waits.scout && a.waits.gate >= time.Millisecond:
		ws = append(ws, "gate="+msStr(a.waits.gate)+" (went alone)")
	case a.waits.scout:
		ws = append(ws, "gate: went alone")
	case a.waits.gate >= time.Millisecond:
		ws = append(ws, "gate="+msStr(a.waits.gate))
	}
	if a.waits.pause >= time.Millisecond {
		ws = append(ws, "pause="+msStr(a.waits.pause))
	}
	if a.waits.rate >= time.Millisecond {
		ws = append(ws, "rate="+msStr(a.waits.rate))
	}
	if tm.connWait >= time.Millisecond {
		ws = append(ws, "conn="+msStr(tm.connWait))
	}
	if len(ws) > 0 {
		parts = append(parts, "wait "+strings.Join(ws, " "))
	}
	if !a.start.IsZero() {
		phases := []string{"net=" + msStr(tm.net)}
		span := func(label string, from, to time.Time) {
			if !from.IsZero() && !to.IsZero() {
				phases = append(phases, label+"="+msStr(to.Sub(from)))
			}
		}
		span("dns", h.dnsStart, h.dnsDone)
		span("tcp", h.connectStart, h.connectDone)
		span("tls", h.tlsStart, h.tlsDone)
		span("ttfb", a.start, h.firstByte)
		span("body", h.firstByte, a.bodyDone)
		parts = append(parts, strings.Join(phases, " "))
		conn := "conn=new"
		if h.reused {
			conn = "conn=reused (idle " + msStr(h.idle) + ")"
		}
		if h.k != nil {
			conn += fmt.Sprintf(" #%d", h.k.id)
		}
		parts = append(parts, conn)
		if a.bytes > 0 {
			parts = append(parts, fmt.Sprintf("body=%dB", a.bytes))
		}
		if len(a.cookies) > 0 {
			parts = append(parts, fmt.Sprintf("cookies=%d", len(a.cookies)))
		}
		if h.remote != "" {
			parts = append(parts, h.remote)
		}
	}
	return strings.Join(parts, " | ")
}

// finish closes the record of a try of a name: its event in the
// structured trace and its -debug line.
func (a *attempt) finish(res Result, retry bool) {
	a.done()
	if a.c.eventsOn() {
		a.c.event(a.event(res, retry))
	}
	if !a.c.debugOn() {
		return
	}
	outcome := string(res.Status)
	switch {
	case res.notSent:
		outcome = "not sent"
	case res.throttleWait > 0:
		outcome = "429, all wait"
	case retry && res.Status == StatusReserved:
		outcome = "redirect to / (reserved or throttled)"
	case retry:
		outcome += " (will retry)"
	}
	if res.Error != "" && !res.notSent {
		outcome += ": " + res.Error
	}
	a.c.debugf("%s", a.line(outcome))
}

// bodyEOF notes the bytes the connection carried for this request; it is
// called on the request's goroutine when the body hits EOF.
func (a *attempt) bodyEOF() {
	h := a.snapshot()
	if h.k != nil {
		a.wireIn, a.wireOut, a.wireSet = h.k.in.Load()-h.wireIn, h.k.out.Load()-h.wireOut, true
	}
}

// countReader counts the bytes read through it and calls eof once at the
// end of the stream.
type countReader struct {
	r   io.Reader
	n   int64
	eof func()
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	if err == io.EOF && c.eof != nil {
		c.eof()
		c.eof = nil
	}
	return n, err
}

// maskSetCookies hides the values of Set-Cookie headers and keeps the
// names and attributes: "stel_ssid=…; path=/; HttpOnly".
func maskSetCookies(vals []string) []string {
	out := make([]string, len(vals))
	for i, v := range vals {
		head, rest, hasRest := strings.Cut(v, ";")
		if name, _, ok := strings.Cut(head, "="); ok {
			head = strings.TrimSpace(name) + "=…"
		}
		if hasRest {
			head += ";" + rest
		}
		out[i] = head
	}
	return out
}

func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "TLS1.3"
	case tls.VersionTLS12:
		return "TLS1.2"
	}
	return fmt.Sprintf("0x%04x", v)
}

// msStr prints a duration in milliseconds: one decimal under 10 ms.
func msStr(d time.Duration) string {
	if d < 10*time.Millisecond {
		return fmt.Sprintf("%.1fms", ms(d))
	}
	return fmt.Sprintf("%.0fms", ms(d))
}

// ms converts a duration to milliseconds for log lines.
func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
