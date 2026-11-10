package checker

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"
	"time"
)

// conn wraps a TCP connection to Fragment. It counts the bytes on the wire
// (TLS records included, so the handshake too) and reports its life in the
// trace: conn_open when it is dialed, conn_tls once the handshake is known,
// conn_close with its totals. Requests carry its id (cid), so the trace
// tells which requests shared a connection and what each one cost on it.
type conn struct {
	net.Conn
	c        *Checker
	id       int64
	addr     string
	opened   time.Time
	in, out  atomic.Int64
	requests atomic.Int64
	tlsSeen  atomic.Bool
	closed   atomic.Bool
	putErr   atomic.Pointer[string] // why the transport did not keep it idle
}

func (k *conn) Read(p []byte) (int, error) {
	n, err := k.Conn.Read(p)
	if n > 0 {
		k.in.Add(int64(n))
		atomic.AddInt64(&k.c.stats.bytesIn, int64(n))
	}
	return n, err
}

func (k *conn) Write(p []byte) (int, error) {
	n, err := k.Conn.Write(p)
	if n > 0 {
		k.out.Add(int64(n))
		atomic.AddInt64(&k.c.stats.bytesOut, int64(n))
	}
	return n, err
}

func (k *conn) Close() error {
	err := k.Conn.Close()
	if k.closed.CompareAndSwap(false, true) {
		atomic.AddInt64(&k.c.stats.connOpen, -1)
		if k.c.eventsOn() {
			e := TraceEvent{Kind: "conn_close", Conn: k.id, Remote: k.Conn.RemoteAddr().String(),
				LifetimeMs: ms(time.Since(k.opened)), BytesIn: k.in.Load(), BytesOut: k.out.Load(),
				Requests: k.requests.Load()}
			if p := k.putErr.Load(); p != nil {
				e.Reason = *p
			}
			k.c.event(e)
		}
	}
	return err
}

// dial opens a connection for the transport and wraps it.
func (c *Checker) dial(d *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()
		nc, err := d.DialContext(ctx, network, addr)
		if err != nil {
			c.event(TraceEvent{Kind: "conn_error", Addr: addr, DialMs: ms(time.Since(start)), Error: err.Error()})
			return nil, err
		}
		k := &conn{Conn: nc, c: c, id: c.connSeq.Add(1), addr: addr, opened: time.Now()}
		atomic.AddInt64(&c.stats.connOpen, 1)
		if c.eventsOn() {
			c.event(TraceEvent{Kind: "conn_open", Conn: k.id, Addr: addr, Remote: nc.RemoteAddr().String(),
				Local: nc.LocalAddr().String(), DialMs: ms(k.opened.Sub(start))})
		}
		return k, nil
	}
}

// ourConn finds the wrapper under what the transport hands a request (a
// *tls.Conn for https).
func ourConn(nc net.Conn) *conn {
	if tc, ok := nc.(*tls.Conn); ok {
		nc = tc.NetConn()
	}
	k, _ := nc.(*conn)
	return k
}

// reportTLS puts the handshake of the connection in the trace once.
// handshake is its duration when the request that opened it measured it.
func (k *conn) reportTLS(tc *tls.Conn, handshake time.Duration) {
	if !k.c.eventsOn() || !k.tlsSeen.CompareAndSwap(false, true) {
		return
	}
	st := tc.ConnectionState()
	e := TraceEvent{Kind: "conn_tls", Conn: k.id, Remote: k.Conn.RemoteAddr().String(),
		TLS: tlsVersionName(st.Version), Cipher: tls.CipherSuiteName(st.CipherSuite), ALPN: st.NegotiatedProtocol,
		Resumed: st.DidResume, SNI: st.ServerName, OCSPStapled: len(st.OCSPResponse) > 0,
		SCTs: len(st.SignedCertificateTimestamps), TLSMs: ms(handshake)}
	if len(st.PeerCertificates) > 0 {
		cert := st.PeerCertificates[0]
		notAfter := cert.NotAfter
		e.CertSubject = cert.Subject.CommonName
		e.CertIssuer = cert.Issuer.CommonName
		if e.CertIssuer == "" && len(cert.Issuer.Organization) > 0 {
			e.CertIssuer = cert.Issuer.Organization[0]
		}
		e.CertNotAfter = &notAfter
		e.CertSANs = len(cert.DNSNames)
		e.CertChain = len(st.PeerCertificates)
	}
	k.c.event(e)
}
