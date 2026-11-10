package checker

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"strings"
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
// reads; they used to be taken only at -trace), and the request reports
// itself once at the end: a -debug line, and the structured trace.
type attempt struct {
	c     *Checker
	kind  string
	name  string
	try   int // HTTP try of the name, from 1; 0 for a probe
	url   string
	waits waits // before the request: the gate, a shared pause, a -rate start

	start, end                time.Time // request sent .. body drained
	dnsStart, dnsDone         time.Time
	connectStart, connectDone time.Time
	tlsStart, tlsDone         time.Time
	wrote, firstByte          time.Time
	bodyDone                  time.Time
	inFlight                  bool // counted in Stats.InFlight until done

	connReused bool
	connIdle   time.Duration
	remote     string
	tlsVersion string
	alpn       string
	cookies    []string // names of the cookies sent; values are never shown

	code   int
	proto  string
	header http.Header
	bytes  int // body bytes read (decoded)
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

// clientTrace returns the httptrace hooks of the request: they record the
// timings and count new and reused connections.
func (a *attempt) clientTrace() *httptrace.ClientTrace {
	c := a.c
	return &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) {
			a.dnsStart = time.Now()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			a.dnsDone = time.Now()
		},
		ConnectStart: func(network, addr string) {
			if a.connectStart.IsZero() {
				a.connectStart = time.Now()
			}
		},
		ConnectDone: func(network, addr string, err error) {
			a.connectDone = time.Now()
		},
		TLSHandshakeStart: func() {
			a.tlsStart = time.Now()
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			a.tlsDone = time.Now()
			if err != nil {
				return
			}
			a.tlsVersion = tlsVersionName(state.Version)
			a.alpn = state.NegotiatedProtocol
		},
		GotConn: func(info httptrace.GotConnInfo) {
			a.connReused = info.Reused
			a.connIdle = info.IdleTime
			if info.Conn != nil {
				a.remote = info.Conn.RemoteAddr().String()
			}
			if info.Reused {
				atomic.AddInt64(&c.stats.connReuse, 1)
			} else {
				atomic.AddInt64(&c.stats.connNew, 1)
			}
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			a.wrote = time.Now()
		},
		GotFirstResponseByte: func() {
			a.firstByte = time.Now()
		},
	}
}

// done takes the request off Stats.InFlight; it is called once the body
// is drained.
func (a *attempt) done() {
	if a.end.IsZero() {
		a.end = time.Now()
	}
	if a.inFlight {
		a.inFlight = false
		atomic.AddInt64(&a.c.stats.inFlight, -1)
	}
}

// netTime is the time from sending the request to draining the body.
func (a *attempt) netTime() time.Duration {
	if a.start.IsZero() || a.end.IsZero() {
		return 0
	}
	return a.end.Sub(a.start)
}

// line is the -debug summary of a finished request: what came back, the
// waits before it and every phase of it.
func (a *attempt) line(outcome string) string {
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
	if len(ws) > 0 {
		parts = append(parts, "wait "+strings.Join(ws, " "))
	}
	if !a.start.IsZero() {
		phases := []string{"net=" + msStr(a.netTime())}
		span := func(label string, from, to time.Time) {
			if !from.IsZero() && !to.IsZero() {
				phases = append(phases, label+"="+msStr(to.Sub(from)))
			}
		}
		span("dns", a.dnsStart, a.dnsDone)
		span("tcp", a.connectStart, a.connectDone)
		span("tls", a.tlsStart, a.tlsDone)
		span("ttfb", a.start, a.firstByte)
		span("body", a.firstByte, a.bodyDone)
		parts = append(parts, strings.Join(phases, " "))
		conn := "conn=new"
		if a.connReused {
			conn = "conn=reused (idle " + msStr(a.connIdle) + ")"
		}
		parts = append(parts, conn)
		if a.bytes > 0 {
			parts = append(parts, fmt.Sprintf("body=%dB", a.bytes))
		}
		if len(a.cookies) > 0 {
			parts = append(parts, fmt.Sprintf("cookies=%d", len(a.cookies)))
		}
		if a.remote != "" {
			parts = append(parts, a.remote)
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
