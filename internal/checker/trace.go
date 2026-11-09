package checker

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Logger receives optional detail about the checks. Any field may be nil.
// The console sets them from -debug and -trace; the web panel sends them
// to the tile's log.
type Logger struct {
	Warnf  func(format string, args ...any) // notable events: throttle pauses, HTTP 429, the stop
	Debugf func(format string, args ...any) // one line per HTTP try with its timings, codes, redirects, verdicts, cache use
	Tracef func(format string, args ...any) // every step: request headers, DNS/TCP/TLS, headers of the answer, body previews

	// Event receives the structured trace: one TraceEvent per HTTP request,
	// cache lookup and name, and the pauses, the gate and the stop. nil = off.
	Event func(TraceEvent)
}

// SetLogger installs l. Call it before the first check.
func (c *Checker) SetLogger(l Logger) { c.log = l }

// debugOn and traceOn tell whether the detail of that level is wanted, so
// callers can skip work that only feeds it.
func (c *Checker) debugOn() bool { return c.log.Debugf != nil }
func (c *Checker) traceOn() bool { return c.log.Tracef != nil }

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

func (c *Checker) tracef(format string, args ...any) {
	if c.traceOn() {
		c.log.Tracef(format, args...)
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

// begin marks the request as sent, notes the cookies the jar will add
// (names only) and, at -trace, prints the request.
func (a *attempt) begin(req *http.Request) {
	if jar := a.c.client.Jar; jar != nil {
		for _, ck := range jar.Cookies(req.URL) {
			a.cookies = append(a.cookies, ck.Name)
		}
	}
	a.start = time.Now()
	if a.c.traceOn() {
		var hdr []string
		for _, k := range sortedKeys(req.Header) {
			hdr = append(hdr, k+": "+strings.Join(req.Header[k], ", "))
		}
		cookies := "none"
		if len(a.cookies) > 0 {
			cookies = strings.Join(a.cookies, ", ") + " (values hidden)"
		}
		a.c.tracef("%s -> %s %s | %s | cookies: %s", a.who(), req.Method, req.URL, strings.Join(hdr, " | "), cookies)
	}
}

// response keeps what the answer says and, at -trace, prints its headers.
// Set-Cookie values are hidden: they are session tokens.
func (a *attempt) response(resp *http.Response) {
	a.code = resp.StatusCode
	a.proto = resp.Proto
	a.header = resp.Header
	if !a.c.traceOn() {
		return
	}
	for _, k := range sortedKeys(resp.Header) {
		vals := resp.Header[k]
		if k == "Set-Cookie" {
			vals = maskSetCookies(vals)
		}
		a.c.tracef("%s  header  %s: %s", a.who(), k, strings.Join(vals, "; "))
	}
}

// clientTrace returns the httptrace hooks of the request: they record the
// timings, count new and reused connections, and print every step at
// -trace.
func (a *attempt) clientTrace() *httptrace.ClientTrace {
	c := a.c
	return &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			a.dnsStart = time.Now()
			c.tracef("%s DNS start: %s", a.who(), info.Host)
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			a.dnsDone = time.Now()
			if info.Err != nil {
				c.tracef("%s DNS FAILED: %v", a.who(), info.Err)
				return
			}
			if c.traceOn() {
				addrs := make([]string, len(info.Addrs))
				for i, ad := range info.Addrs {
					addrs[i] = ad.String()
				}
				c.tracef("%s DNS -> %s (%.1fms)", a.who(), strings.Join(addrs, ", "), ms(a.dnsDone.Sub(a.dnsStart)))
			}
		},
		ConnectStart: func(network, addr string) {
			if a.connectStart.IsZero() {
				a.connectStart = time.Now()
			}
			c.tracef("%s TCP start -> %s %s", a.who(), network, addr)
		},
		ConnectDone: func(network, addr string, err error) {
			a.connectDone = time.Now()
			if err != nil {
				c.tracef("%s TCP FAILED -> %s: %v", a.who(), addr, err)
				return
			}
			c.tracef("%s TCP connected -> %s (%.1fms)", a.who(), addr, ms(a.connectDone.Sub(a.connectStart)))
		},
		TLSHandshakeStart: func() {
			a.tlsStart = time.Now()
			c.tracef("%s TLS start", a.who())
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			a.tlsDone = time.Now()
			if err != nil {
				c.tracef("%s TLS FAILED: %v", a.who(), err)
				return
			}
			a.tlsVersion = tlsVersionName(state.Version)
			a.alpn = state.NegotiatedProtocol
			c.tracef("%s TLS done: %s proto=%s (%.1fms)", a.who(), a.tlsVersion, a.alpn, ms(a.tlsDone.Sub(a.tlsStart)))
		},
		GotConn: func(info httptrace.GotConnInfo) {
			a.connReused = info.Reused
			a.connIdle = info.IdleTime
			if info.Conn != nil {
				a.remote = info.Conn.RemoteAddr().String()
			}
			if info.Reused {
				atomic.AddInt64(&c.stats.connReuse, 1)
				c.tracef("%s conn REUSED %s (idle %v)", a.who(), a.remote, info.IdleTime)
			} else {
				atomic.AddInt64(&c.stats.connNew, 1)
				c.tracef("%s conn NEW %s", a.who(), a.remote)
			}
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			a.wrote = time.Now()
			if info.Err != nil {
				c.tracef("%s writing the request FAILED: %v", a.who(), info.Err)
			}
		},
		GotFirstResponseByte: func() {
			a.firstByte = time.Now()
			c.tracef("%s TTFB=%.1fms", a.who(), ms(a.firstByte.Sub(a.start)))
		},
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
	if a.end.IsZero() {
		a.end = time.Now()
	}
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

func sortedKeys(h http.Header) []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
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
