package checker

import (
	"crypto/tls"
	"fmt"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"time"
)

// Logger receives optional detail about the checks. Any field may be nil.
// The console sets them from -debug and -trace; the web panel leaves them
// nil and shows Stats instead.
type Logger struct {
	Warnf  func(format string, args ...any) // notable events, e.g. HTTP 429 waits
	Debugf func(format string, args ...any) // HTTP codes, redirects, verdicts, cache use
	Tracef func(format string, args ...any) // DNS/TCP/TLS events, headers, body previews
}

// SetLogger installs l. Call it before the first check.
func (c *Checker) SetLogger(l Logger) { c.log = l }

func (c *Checker) warnf(format string, args ...any) {
	if c.log.Warnf != nil {
		c.log.Warnf(format, args...)
	}
}

func (c *Checker) debugf(format string, args ...any) {
	if c.log.Debugf != nil {
		c.log.Debugf(format, args...)
	}
}

func (c *Checker) tracef(format string, args ...any) {
	if c.log.Tracef != nil {
		c.log.Tracef(format, args...)
	}
}

// requestTrace records the steps of one HTTP attempt when Logger.Tracef is
// set. It also does the connection counting of the plain trace.
type requestTrace struct {
	c            *Checker
	username     string
	requestStart time.Time
	dnsStart     time.Time
	dnsDone      time.Time
	connectStart time.Time
	connectDone  time.Time
	tlsStart     time.Time
	tlsDone      time.Time
	gotFirstByte time.Time
	connReused   bool
	remoteAddr   string
}

// clientTrace returns the counting trace, or the full one with Tracef set.
func (c *Checker) clientTrace(name string, start time.Time) (*httptrace.ClientTrace, *requestTrace) {
	if c.log.Tracef == nil {
		return &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				if info.Reused {
					atomic.AddInt64(&c.stats.connReuse, 1)
				} else {
					atomic.AddInt64(&c.stats.connNew, 1)
				}
			},
		}, nil
	}
	t := &requestTrace{c: c, username: name, requestStart: start}
	return &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			t.dnsStart = time.Now()
			c.tracef("@%s DNS start: %s", name, info.Host)
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			t.dnsDone = time.Now()
			if info.Err != nil {
				c.tracef("@%s DNS FAILED: %v", name, info.Err)
				return
			}
			addrs := make([]string, len(info.Addrs))
			for i, a := range info.Addrs {
				addrs[i] = a.String()
			}
			c.tracef("@%s DNS -> %s (%.1fms)", name, strings.Join(addrs, ", "), ms(t.dnsDone.Sub(t.dnsStart)))
		},
		ConnectStart: func(network, addr string) {
			t.connectStart = time.Now()
			c.tracef("@%s TCP start -> %s %s", name, network, addr)
		},
		ConnectDone: func(network, addr string, err error) {
			t.connectDone = time.Now()
			t.remoteAddr = addr
			if err != nil {
				c.tracef("@%s TCP FAILED: %v", name, err)
				return
			}
			c.tracef("@%s TCP connected -> %s (%.1fms)", name, addr, ms(t.connectDone.Sub(t.connectStart)))
		},
		TLSHandshakeStart: func() {
			t.tlsStart = time.Now()
			c.tracef("@%s TLS start", name)
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			t.tlsDone = time.Now()
			if err != nil {
				c.tracef("@%s TLS FAILED: %v", name, err)
				return
			}
			version := fmt.Sprintf("0x%04x", state.Version)
			switch state.Version {
			case tls.VersionTLS13:
				version = "TLS1.3"
			case tls.VersionTLS12:
				version = "TLS1.2"
			}
			c.tracef("@%s TLS done: %s proto=%s (%.1fms)", name, version, state.NegotiatedProtocol,
				ms(t.tlsDone.Sub(t.tlsStart)))
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.connReused = info.Reused
			if info.Reused {
				atomic.AddInt64(&c.stats.connReuse, 1)
				c.tracef("@%s conn REUSED (idle %v)", name, info.IdleTime)
			} else {
				atomic.AddInt64(&c.stats.connNew, 1)
				c.tracef("@%s conn NEW", name)
			}
		},
		GotFirstResponseByte: func() {
			t.gotFirstByte = time.Now()
			c.tracef("@%s TTFB=%.1fms", name, ms(t.gotFirstByte.Sub(t.requestStart)))
		},
	}, t
}

// summary prints one debug line describing a finished attempt. It is a
// no-op without a full trace.
func (t *requestTrace) summary(status Status, bodySize int) {
	if t == nil {
		return
	}
	parts := []string{
		fmt.Sprintf("@%-20s", t.username),
		fmt.Sprintf("-> %-9s", status),
		fmt.Sprintf("body=%5dB", bodySize),
		fmt.Sprintf("total=%6.0fms", ms(time.Since(t.requestStart))),
	}
	if t.connReused {
		parts = append(parts, "conn=reused")
	} else {
		parts = append(parts, "conn=new")
		if !t.dnsStart.IsZero() && !t.dnsDone.IsZero() {
			parts = append(parts, fmt.Sprintf("dns=%.0fms", ms(t.dnsDone.Sub(t.dnsStart))))
		}
		if !t.connectStart.IsZero() && !t.connectDone.IsZero() {
			parts = append(parts, fmt.Sprintf("tcp=%.0fms", ms(t.connectDone.Sub(t.connectStart))))
		}
		if !t.tlsStart.IsZero() && !t.tlsDone.IsZero() {
			parts = append(parts, fmt.Sprintf("tls=%.0fms", ms(t.tlsDone.Sub(t.tlsStart))))
		}
	}
	if !t.gotFirstByte.IsZero() {
		parts = append(parts, fmt.Sprintf("ttfb=%.0fms", ms(t.gotFirstByte.Sub(t.requestStart))))
	}
	if t.remoteAddr != "" {
		parts = append(parts, t.remoteAddr)
	}
	t.c.debugf("%s", strings.Join(parts, " | "))
}

// ms converts a duration to milliseconds for log lines.
func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
