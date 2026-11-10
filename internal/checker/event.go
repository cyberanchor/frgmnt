package checker

import (
	"net/http"
	"strings"
	"time"
)

// TraceEvent is one record of the structured trace (Logger.Event): an HTTP
// request (a try of a name, or the throttle probe), a cache lookup, the
// end of a name, or an event of the engine (a pause, the gate, the end of
// throttling, the stop). It is JSON-ready, and the hosts write one per
// line (JSON Lines), so a run can be read back with jq or a notebook:
// where the time went, which tries were retried and why, what each answer
// said. Durations are milliseconds; fields that do not apply are left out.
// Cookie values never appear, only their names.
type TraceEvent struct {
	// Seq numbers the lines of a trace file from 1, across its segments
	// (set by the writer): a gap means lines were lost.
	Seq  int64     `json:"seq,omitempty"`
	Time time.Time `json:"ts"`
	Kind string    `json:"kind"` // attempt, probe, cache, result, pause, gate, throttle_over, blocked; run_start, run_end, pipeline, segment, trace_stopped from the hosts and the writer
	Name string    `json:"name,omitempty"`
	Try  int       `json:"try,omitempty"` // HTTP try of the name, from 1; throttled tries count

	// The request (attempt, probe).
	URL         string   `json:"url,omitempty"`
	CookiesSent []string `json:"cookies_sent,omitempty"`
	WaitGateMs  float64  `json:"wait_gate_ms,omitempty"`  // waiting for the gate (one request at a time)
	WaitPauseMs float64  `json:"wait_pause_ms,omitempty"` // waiting out a shared pause
	WaitRateMs  float64  `json:"wait_rate_ms,omitempty"`  // waiting for the -rate start
	Alone       bool     `json:"alone,omitempty"`         // went alone through the closed gate
	DNSMs       float64  `json:"dns_ms,omitempty"`
	ConnectMs   float64  `json:"connect_ms,omitempty"`
	TLSMs       float64  `json:"tls_ms,omitempty"`
	WroteMs     float64  `json:"wrote_ms,omitempty"` // the request written, from the start of the request
	TTFBMs      float64  `json:"ttfb_ms,omitempty"`  // first byte of the answer, from the start
	BodyMs      float64  `json:"body_ms,omitempty"`  // reading the body after the first byte
	NetMs       float64  `json:"net_ms,omitempty"`   // the whole request, until the body is drained
	ConnReused  bool     `json:"conn_reused,omitempty"`
	ConnIdleMs  float64  `json:"conn_idle_ms,omitempty"`
	Remote      string   `json:"remote,omitempty"`
	TLS         string   `json:"tls,omitempty"`
	ALPN        string   `json:"alpn,omitempty"`

	// The answer.
	Proto           string   `json:"proto,omitempty"`
	Code            int      `json:"code,omitempty"`
	Location        string   `json:"location,omitempty"`
	ContentType     string   `json:"content_type,omitempty"`
	ContentEncoding string   `json:"content_encoding,omitempty"`
	Server          string   `json:"server,omitempty"`
	CFRay           string   `json:"cf_ray,omitempty"`
	CFMitigated     string   `json:"cf_mitigated,omitempty"`
	RetryAfter      string   `json:"retry_after,omitempty"`
	SetCookies      []string `json:"set_cookies,omitempty"` // names
	Bytes           int      `json:"bytes,omitempty"`       // body bytes read, decoded

	// The outcome of a request or of a name.
	Status     Status `json:"status,omitempty"`
	Retry      bool   `json:"retry,omitempty"`   // the try will be repeated (or decided by the probe)
	Healthy    *bool  `json:"healthy,omitempty"` // probe: Fragment answers this client normally
	Confidence int    `json:"confidence,omitempty"`
	Evidence   string `json:"evidence,omitempty"`
	Offset     int    `json:"offset,omitempty"`
	Error      string `json:"error,omitempty"`
	NotSent    bool   `json:"not_sent,omitempty"`  // the run stopped before the request went out
	Cancelled  bool   `json:"cancelled,omitempty"` // Stop or Ctrl+C cut it short

	// The end of a name (result).
	Attempts int     `json:"attempts,omitempty"`
	TookMs   float64 `json:"took_ms,omitempty"`
	WaitMs   float64 `json:"wait_ms,omitempty"`
	Cached   bool    `json:"cached,omitempty"`
	Stale    bool    `json:"stale,omitempty"`

	// A cache lookup (cache): hit, stale or miss, and the age of the entry.
	Cache string  `json:"cache,omitempty"`
	AgeMs float64 `json:"age_ms,omitempty"`

	// Engine events.
	PauseMs     float64 `json:"pause_ms,omitempty"`     // pause: how long every request waits
	Round       int     `json:"round,omitempty"`        // pause: its number in the spell; throttle_over: pauses taken
	Reason      string  `json:"reason,omitempty"`       // pause: "throttle" (302 / for the probe too) or "429"
	ThrottledMs float64 `json:"throttled_ms,omitempty"` // pause: how long the spell has lasted
	Gate        string  `json:"gate,omitempty"`         // gate: "open" or "closed"
	Block       string  `json:"block,omitempty"`        // blocked: "challenge" or "throttle"
	Message     string  `json:"message,omitempty"`

	// Anything a host adds (run_start, run_end, pipeline).
	Info map[string]any `json:"info,omitempty"`
}

// eventsOn tells whether a structured trace is being written.
func (c *Checker) eventsOn() bool { return c.log.Event != nil }

// event hands e to the trace, stamped with the time.
func (c *Checker) event(e TraceEvent) {
	if c.log.Event == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	c.log.Event(e)
}

// spanMs is the time from from to to in milliseconds, 0 when either is
// missing.
func spanMs(from, to time.Time) float64 {
	if from.IsZero() || to.IsZero() {
		return 0
	}
	return ms(to.Sub(from))
}

// event is the trace record of a finished request.
func (a *attempt) event(res Result, retry bool) TraceEvent {
	e := TraceEvent{
		Time:        a.start,
		Kind:        a.kind,
		Name:        a.name,
		Try:         a.try,
		URL:         a.url,
		CookiesSent: a.cookies,
		WaitGateMs:  ms(a.waits.gate),
		WaitPauseMs: ms(a.waits.pause),
		WaitRateMs:  ms(a.waits.rate),
		Alone:       a.waits.scout,
		DNSMs:       spanMs(a.dnsStart, a.dnsDone),
		ConnectMs:   spanMs(a.connectStart, a.connectDone),
		TLSMs:       spanMs(a.tlsStart, a.tlsDone),
		WroteMs:     spanMs(a.start, a.wrote),
		TTFBMs:      spanMs(a.start, a.firstByte),
		BodyMs:      spanMs(a.firstByte, a.bodyDone),
		NetMs:       ms(a.netTime()),
		ConnReused:  a.connReused,
		ConnIdleMs:  ms(a.connIdle),
		Remote:      a.remote,
		TLS:         a.tlsVersion,
		ALPN:        a.alpn,
		Proto:       a.proto,
		Code:        a.code,
		Bytes:       a.bytes,
		Status:      res.Status,
		Retry:       retry,
		Confidence:  res.Confidence,
		Evidence:    res.Evidence,
		Offset:      res.Offset,
		Error:       res.Error,
		NotSent:     res.notSent,
	}
	if e.Time.IsZero() {
		e.Time = time.Now() // not sent: no request start
	}
	if h := a.header; h != nil {
		e.Location = h.Get("Location")
		e.ContentType = h.Get("Content-Type")
		e.ContentEncoding = h.Get("Content-Encoding")
		e.Server = h.Get("Server")
		e.CFRay = h.Get("Cf-Ray")
		e.CFMitigated = h.Get("Cf-Mitigated")
		e.RetryAfter = h.Get("Retry-After")
		e.SetCookies = setCookieNames(h)
	}
	return e
}

// setCookieNames lists the names of the cookies an answer sets.
func setCookieNames(h http.Header) []string {
	var names []string
	for _, v := range h.Values("Set-Cookie") {
		if name, _, ok := strings.Cut(v, "="); ok {
			names = append(names, strings.TrimSpace(name))
		}
	}
	return names
}

func boolPtr(b bool) *bool { return &b }
