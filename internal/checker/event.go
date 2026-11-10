package checker

import (
	"encoding/base64"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
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
	URL            string      `json:"url,omitempty"`
	Method         string      `json:"method,omitempty"`
	ReqHeaders     http.Header `json:"req_headers,omitempty"` // as sent; Cookie by name only ("stel_ssid=…")
	CookiesSent    []string    `json:"cookies_sent,omitempty"`
	WaitGateMs     float64     `json:"wait_gate_ms,omitempty"`  // waiting for the gate (one request at a time)
	WaitPauseMs    float64     `json:"wait_pause_ms,omitempty"` // waiting out a shared pause
	WaitRateMs     float64     `json:"wait_rate_ms,omitempty"`  // waiting for the -rate start
	Alone          bool        `json:"alone,omitempty"`         // went alone through the closed gate
	ConnWaitMs     float64     `json:"conn_wait_ms,omitempty"`  // waiting for a free connection (-conns); not network time
	DNSMs          float64     `json:"dns_ms,omitempty"`
	DNSAddrs       []string    `json:"dns_addrs,omitempty"`
	DNSCoalesced   bool        `json:"dns_coalesced,omitempty"` // shared the lookup of another request
	ConnectMs      float64     `json:"connect_ms,omitempty"`
	TLSMs          float64     `json:"tls_ms,omitempty"`
	WroteHeadersMs float64     `json:"wrote_headers_ms,omitempty"` // the headers written, from the start of the request
	WroteMs        float64     `json:"wrote_ms,omitempty"`         // the request written, from the start of the request
	TTFBMs         float64     `json:"ttfb_ms,omitempty"`          // first byte of the answer, from the start
	BodyMs         float64     `json:"body_ms,omitempty"`          // reading the body after the first byte
	NetMs          float64     `json:"net_ms,omitempty"`           // on the network: until the body is drained, without conn_wait_ms
	Informational  []int       `json:"informational,omitempty"`    // 1xx answers before the final one (e.g. 103)
	ConnReused     bool        `json:"conn_reused,omitempty"`
	ConnIdleMs     float64     `json:"conn_idle_ms,omitempty"`
	Remote         string      `json:"remote,omitempty"`
	Local          string      `json:"local,omitempty"`
	TLS            string      `json:"tls,omitempty"`
	ALPN           string      `json:"alpn,omitempty"`
	WireIn         int64       `json:"wire_in,omitempty"`  // bytes received on the connection for this request (HTTP/1.x; HTTP/2 shares a connection: see conn_close)
	WireOut        int64       `json:"wire_out,omitempty"` // bytes sent, likewise

	// A connection (conn_open, conn_tls, conn_close, conn_error; attempts
	// and probes carry cid too).
	Conn         int64      `json:"cid,omitempty"`
	Addr         string     `json:"addr,omitempty"`    // what was dialed
	DialMs       float64    `json:"dial_ms,omitempty"` // conn_open, conn_error: the TCP dial
	LifetimeMs   float64    `json:"lifetime_ms,omitempty"`
	BytesIn      int64      `json:"bytes_in,omitempty"`  // conn_close: bytes received over its life, TLS included
	BytesOut     int64      `json:"bytes_out,omitempty"` // conn_close: bytes sent
	Requests     int64      `json:"requests,omitempty"`  // conn_close: requests it carried
	Cipher       string     `json:"cipher,omitempty"`
	Resumed      bool       `json:"tls_resumed,omitempty"`
	SNI          string     `json:"sni,omitempty"`
	CertSubject  string     `json:"cert_subject,omitempty"`
	CertIssuer   string     `json:"cert_issuer,omitempty"`
	CertNotAfter *time.Time `json:"cert_not_after,omitempty"`
	CertSANs     int        `json:"cert_sans,omitempty"`
	CertChain    int        `json:"cert_chain,omitempty"`
	OCSPStapled  bool       `json:"ocsp_stapled,omitempty"`
	SCTs         int        `json:"scts,omitempty"`

	// The answer.
	Proto           string      `json:"proto,omitempty"`
	Code            int         `json:"code,omitempty"`
	StatusLine      string      `json:"status_line,omitempty"`  // "302 Found"
	RespHeaders     http.Header `json:"resp_headers,omitempty"` // every header; Set-Cookie values hidden
	ContentLength   *int64      `json:"content_length,omitempty"`
	Trailers        http.Header `json:"trailers,omitempty"`
	Location        string      `json:"location,omitempty"`
	ContentType     string      `json:"content_type,omitempty"`
	ContentEncoding string      `json:"content_encoding,omitempty"`
	Server          string      `json:"server,omitempty"`
	CFRay           string      `json:"cf_ray,omitempty"`
	CFMitigated     string      `json:"cf_mitigated,omitempty"`
	RetryAfter      string      `json:"retry_after,omitempty"`
	SetCookies      []string    `json:"set_cookies,omitempty"`    // names
	Bytes           int         `json:"bytes,omitempty"`          // body bytes read for the parser, decoded
	BodyRawBytes    int64       `json:"body_raw_bytes,omitempty"` // body bytes received as sent (compressed), the drained rest included
	// The page itself, decoded, when Config.TraceBodies keeps it (up to
	// 1 MB; body_b64 when it is not UTF-8).
	Body          string `json:"body,omitempty"`
	BodyB64       string `json:"body_b64,omitempty"`
	BodyTruncated bool   `json:"body_truncated,omitempty"`

	// Work on this machine inside a request: parsing the page, writing it
	// to the page cache.
	ParseUs         float64 `json:"parse_us,omitempty"`
	CacheWriteMs    float64 `json:"cache_write_ms,omitempty"`
	CacheWriteBytes int     `json:"cache_write_bytes,omitempty"`
	CacheError      string  `json:"cache_error,omitempty"`

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
	WorkMs   float64 `json:"work_ms,omitempty"` // result, attempt: parsing and the page cache
	Cached   bool    `json:"cached,omitempty"`
	Stale    bool    `json:"stale,omitempty"`

	// A cache lookup (cache): hit, stale, miss or error (an entry that
	// cannot be read; Error says why), the age of the entry and the time
	// the lookup took.
	Cache  string  `json:"cache,omitempty"`
	AgeMs  float64 `json:"age_ms,omitempty"`
	ReadMs float64 `json:"read_ms,omitempty"`

	// Engine events.
	PauseMs     float64 `json:"pause_ms,omitempty"`     // pause: how long every request waits
	Round       int     `json:"round,omitempty"`        // pause: its number in the spell; throttle_over: pauses taken
	Reason      string  `json:"reason,omitempty"`       // pause: "throttle" (302 / for the probe too) or "429"; conn_close: why the transport let it go
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
	h := a.snapshot()
	tm := a.timing(h)
	e := TraceEvent{
		Time:            a.start,
		Kind:            a.kind,
		Name:            a.name,
		Try:             a.try,
		URL:             a.url,
		CookiesSent:     a.cookies,
		WaitGateMs:      ms(a.waits.gate),
		WaitPauseMs:     ms(a.waits.pause),
		WaitRateMs:      ms(a.waits.rate),
		Alone:           a.waits.scout,
		ConnWaitMs:      ms(tm.connWait),
		DNSMs:           spanMs(h.dnsStart, h.dnsDone),
		DNSAddrs:        h.dnsAddrs,
		DNSCoalesced:    h.dnsCoalesced,
		ConnectMs:       spanMs(h.connectStart, h.connectDone),
		TLSMs:           spanMs(h.tlsStart, h.tlsDone),
		WroteHeadersMs:  spanMs(a.start, h.wroteHeaders),
		WroteMs:         spanMs(a.start, h.wrote),
		TTFBMs:          spanMs(a.start, h.firstByte),
		BodyMs:          spanMs(h.firstByte, a.bodyDone),
		NetMs:           ms(tm.net),
		WorkMs:          ms(tm.work),
		Informational:   h.got1xx,
		ConnReused:      h.reused,
		ConnIdleMs:      ms(h.idle),
		Remote:          h.remote,
		Local:           h.local,
		TLS:             h.tlsVersion,
		ALPN:            h.alpn,
		Method:          a.method,
		ReqHeaders:      a.requestHeaders(),
		Proto:           a.proto,
		Code:            a.code,
		StatusLine:      a.status,
		RespHeaders:     maskedHeaders(a.header),
		Trailers:        a.trailer,
		Bytes:           a.bytes,
		BodyRawBytes:    a.rawBytes,
		ParseUs:         float64(a.parse.Nanoseconds()) / 1000,
		CacheWriteMs:    ms(a.cacheWrite),
		CacheWriteBytes: a.cacheWriteBytes,
		CacheError:      a.cacheErr,
		Status:          res.Status,
		Retry:           retry,
		Confidence:      res.Confidence,
		Evidence:        res.Evidence,
		Offset:          res.Offset,
		Error:           res.Error,
		NotSent:         res.notSent,
	}
	if e.Time.IsZero() {
		e.Time = time.Now() // not sent: no request start
	}
	if a.code != 0 && a.contentLength >= 0 {
		n := a.contentLength
		e.ContentLength = &n
	}
	if len(e.Trailers) == 0 {
		e.Trailers = nil
	}
	if a.bodyKept {
		if utf8.Valid(a.body) {
			e.Body = string(a.body)
		} else {
			e.BodyB64 = base64.StdEncoding.EncodeToString(a.body)
		}
		e.BodyTruncated = a.bodyTruncated
	}
	if h.k != nil {
		e.Conn = h.k.id
	}
	// One request at a time on an HTTP/1.x connection: the change of its
	// counters is this request. HTTP/2 multiplexes, so only the
	// connection's totals (conn_close) are exact there.
	if a.wireSet && strings.HasPrefix(a.proto, "HTTP/1.") {
		e.WireIn, e.WireOut = a.wireIn, a.wireOut
	}
	if hd := a.header; hd != nil {
		e.Location = hd.Get("Location")
		e.ContentType = hd.Get("Content-Type")
		e.ContentEncoding = hd.Get("Content-Encoding")
		e.Server = hd.Get("Server")
		e.CFRay = hd.Get("Cf-Ray")
		e.CFMitigated = hd.Get("Cf-Mitigated")
		e.RetryAfter = hd.Get("Retry-After")
		e.SetCookies = setCookieNames(hd)
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
