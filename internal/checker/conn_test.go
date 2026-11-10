package checker

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor polls until ok or fails after 3 s.
func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); !ok(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// countingListener counts the bytes the server reads and writes.
type countingListener struct {
	net.Listener
	in, out atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: c, l: l}, nil
}

type countingConn struct {
	net.Conn
	l *countingListener
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.l.in.Add(int64(n))
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.l.out.Add(int64(n))
	return n, err
}

func freeRedirect(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/?query="+strings.TrimPrefix(r.URL.Path, "/username/"), http.StatusFound)
}

// Over TLS and HTTP/2 (as with Fragment) the trace tells the connection's
// life: conn_open, one conn_tls with the handshake (version, cipher,
// ALPN, certificate), the requests that shared it (cid), and conn_close
// with its bytes and requests. The tests used plain HTTP/1.1 only, so none
// of the TLS or HTTP/2 fields were ever checked.
func TestConnEventsOverTLSAndHTTP2(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(freeRedirect))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	old := FragmentURLTemplate
	FragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() { FragmentURLTemplate = old })

	var log eventLog
	c := New(Config{Workers: 1, MaxConns: 1, InsecureTLS: true, Timeout: 3 * time.Second})
	c.SetLogger(Logger{Event: log.add})
	for _, name := range []string{"tlsname1", "tlsname2"} {
		if res := c.CheckOne(context.Background(), name); res.Status != StatusFree {
			t.Fatalf("%s: %s %s", name, res.Status, res.Error)
		}
	}
	if st := c.Snapshot(); st.ConnOpen != 1 || st.BytesIn == 0 || st.BytesOut == 0 {
		t.Errorf("stats before Close: open=%d in=%d out=%d", st.ConnOpen, st.BytesIn, st.BytesOut)
	}
	c.Close()
	waitFor(t, "conn_close", func() bool { return len(log.of("conn_close")) == 1 })

	opens, tlss, closes := log.of("conn_open"), log.of("conn_tls"), log.of("conn_close")
	if len(opens) != 1 || opens[0].Conn != 1 || opens[0].Remote == "" || opens[0].Addr == "" {
		t.Fatalf("conn_open %+v", opens)
	}
	if len(tlss) != 1 {
		t.Fatalf("%d conn_tls events, want 1 for one connection", len(tlss))
	}
	tl := tlss[0]
	if tl.Conn != 1 || tl.TLS != "TLS1.3" || tl.ALPN != "h2" || tl.Cipher == "" || tl.CertIssuer == "" ||
		tl.CertNotAfter == nil || tl.CertChain < 1 || tl.TLSMs <= 0 {
		t.Errorf("conn_tls %+v", tl)
	}
	attempts := log.of("attempt")
	if len(attempts) != 2 {
		t.Fatalf("%d attempts", len(attempts))
	}
	for i, a := range attempts {
		if a.Conn != 1 || a.Proto != "HTTP/2.0" || a.ALPN != "h2" && i == 0 || a.ConnReused != (i == 1) {
			t.Errorf("attempt %d: cid=%d proto=%s alpn=%s reused=%v", i, a.Conn, a.Proto, a.ALPN, a.ConnReused)
		}
		if a.WireIn != 0 || a.WireOut != 0 {
			t.Errorf("attempt %d has per-request wire bytes on HTTP/2: %d/%d", i, a.WireIn, a.WireOut)
		}
	}
	cl := closes[0]
	if cl.Conn != 1 || cl.Requests != 2 || cl.BytesIn == 0 || cl.BytesOut == 0 || cl.LifetimeMs <= 0 {
		t.Errorf("conn_close %+v", cl)
	}
	if st := c.Snapshot(); st.ConnOpen != 0 {
		t.Errorf("%d connections open after Close", st.ConnOpen)
	}
}

// On HTTP/1.1 a connection carries one request at a time, so every
// request gets its own bytes on the wire; they add up to what the server
// counted, and to the connection's totals.
func TestWireBytesPerRequestHTTP1(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "page") {
			_, _ = w.Write([]byte(strings.Repeat("<p>some page</p>", 500))) // 8000 bytes, no marker
			return
		}
		freeRedirect(w, r)
	}))
	l := &countingListener{Listener: srv.Listener}
	srv.Listener = l
	srv.Start()
	t.Cleanup(srv.Close)
	old := FragmentURLTemplate
	FragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() { FragmentURLTemplate = old })

	var log eventLog
	c := New(Config{Workers: 1, MaxConns: 1, Timeout: 3 * time.Second, ReadLimit: 1024})
	c.SetLogger(Logger{Event: log.add})
	for _, name := range []string{"wirefree1", "wirepage1", "wirefree2"} {
		_ = c.CheckOne(context.Background(), name)
	}
	c.Close()
	waitFor(t, "conn_close", func() bool { return len(log.of("conn_close")) == 1 })
	var in, out int64
	for _, a := range log.of("attempt") {
		if a.WireIn == 0 || a.WireOut == 0 {
			t.Errorf("%s: wire %d/%d", a.Name, a.WireIn, a.WireOut)
		}
		in += a.WireIn
		out += a.WireOut
		if a.Name == "wirepage1" && (a.Bytes != 1024 || a.BodyRawBytes != 8000) {
			t.Errorf("page: %d bytes for the parser, %d received; want 1024 of 8000 (the rest drained)", a.Bytes, a.BodyRawBytes)
		}
	}
	cl := log.of("conn_close")[0]
	if in != l.out.Load() || out != l.in.Load() || cl.BytesIn != in || cl.BytesOut != out {
		t.Errorf("requests got %d/%d bytes, the connection %d/%d, the server sent %d and read %d",
			in, out, cl.BytesIn, cl.BytesOut, l.out.Load(), l.in.Load())
	}
}

// With every connection busy (-conns 1) a request waits for one. That wait
// was counted as network time; it is conn_wait now, part of Wait, and
// Stats.WaitConn shows the waiting request meanwhile.
func TestConnWaitIsNotNetworkTime(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	t.Cleanup(free)
	var holding atomic.Bool
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/connholder") {
			holding.Store(true)
			<-release
		}
		freeRedirect(w, r)
	})
	var log eventLog
	c := New(Config{Workers: 2, MaxConns: 1, Timeout: 3 * time.Second})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	// The first answer opens the gate (until then requests go one at a
	// time, and the second one would wait there, not for a connection).
	if res := c.CheckOne(context.Background(), "opener"); res.Status != StatusFree {
		t.Fatalf("opener: %s %s", res.Status, res.Error)
	}
	results := make(chan Result, 2)
	go func() { results <- c.CheckOne(context.Background(), "connholder") }()
	waitFor(t, "the holder at the server", holding.Load)
	go func() { results <- c.CheckOne(context.Background(), "connwaiter") }()
	waitFor(t, "a request waiting for the connection", func() bool { return c.Snapshot().WaitConn == 1 })
	time.Sleep(150 * time.Millisecond)
	free()
	var waited Result
	for i := 0; i < 2; i++ {
		if r := <-results; r.Username == "connwaiter" {
			waited = r
		}
	}
	if waited.ConnWait < 100*time.Millisecond || waited.Net > 100*time.Millisecond || waited.Wait < waited.ConnWait {
		t.Errorf("waiter: took %v, net %v, wait %v, conn wait %v", waited.Took, waited.Net, waited.Wait, waited.ConnWait)
	}
	var ev TraceEvent
	for _, e := range log.of("attempt") {
		if e.Name == "connwaiter" {
			ev = e
		}
	}
	if ev.ConnWaitMs < 100 || ev.NetMs > 100 || !ev.ConnReused {
		t.Errorf("attempt event: conn_wait_ms %.1f, net_ms %.1f, reused %v", ev.ConnWaitMs, ev.NetMs, ev.ConnReused)
	}
	if st := c.Snapshot(); st.WaitConn != 0 || st.InFlight != 0 {
		t.Errorf("after the checks: wait_conn=%d in_flight=%d", st.WaitConn, st.InFlight)
	}
}

// slowHandshakeListener holds every new connection's first read, so the
// client's TLS handshake is still going when its request is cancelled.
type slowHandshakeListener struct {
	net.Listener
	delay time.Duration
}

func (l slowHandshakeListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &slowFirstRead{Conn: c, delay: l.delay}, nil
}

type slowFirstRead struct {
	net.Conn
	delay time.Duration
	once  sync.Once
}

func (c *slowFirstRead) Read(p []byte) (int, error) {
	c.once.Do(func() { time.Sleep(c.delay) })
	return c.Conn.Read(p)
}

// A request cancelled while its connection is still being opened returns
// at once, but the transport's dial goroutine goes on and runs the DNS,
// TCP and TLS hooks after it. Those hooks wrote the request's record
// while the request read it (a data race); they are guarded now, and the
// gauges come back to zero.
func TestHooksAfterACancelledDial(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(freeRedirect))
	srv.Listener = slowHandshakeListener{Listener: srv.Listener, delay: 200 * time.Millisecond}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	old := FragmentURLTemplate
	FragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() { FragmentURLTemplate = old })

	var log eventLog
	var debug logCapture
	c := New(Config{Workers: 1, Retries: 0, InsecureTLS: true, Timeout: 3 * time.Second})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add, Debugf: debug.add})
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		res := c.CheckOne(ctx, "slowtls")
		cancel()
		if res.Status != StatusError {
			t.Fatalf("cancelled check: %s", res.Status)
		}
	}
	time.Sleep(300 * time.Millisecond) // the dials finish and run their hooks
	if st := c.Snapshot(); st.WaitConn != 0 || st.InFlight != 0 {
		t.Errorf("after cancelled requests: wait_conn=%d in_flight=%d", st.WaitConn, st.InFlight)
	}
	if n := len(log.of("attempt")); n != 3 {
		t.Errorf("%d attempt events, want 3", n)
	}
}
