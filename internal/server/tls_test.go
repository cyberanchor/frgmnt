package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cyberanchor/frgmnt/internal/auth"
)

// writeCert writes a self-signed certificate for 127.0.0.1 with the given
// serial number to dir/cert.pem and dir/key.pem.
func writeCert(t *testing.T, dir string, serial int64) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "frgmnt test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// For a domain with a certificate the panel needed a TLS reverse proxy;
// it can now serve HTTPS itself (-tls-cert/-tls-key) and picks up a
// renewed certificate without a restart.
func TestHTTPSWithCertificateReload(t *testing.T) {
	defer func(old time.Duration) { certRecheck = old }(certRecheck)
	certRecheck = 50 * time.Millisecond

	dir := t.TempDir()
	certFile, keyFile := writeCert(t, dir, 1)
	keeper, _, err := auth.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	s, _ := newTestServerWith(t, func(o *Options) {
		o.Addr, o.TLSCert, o.TLSKey, o.Auth = addr, certFile, keyFile, keeper
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, // the test certificate changes
			DisableKeepAlives: true,                                  // a new handshake per request
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	serial := func() int64 {
		t.Helper()
		var resp *http.Response
		var err error
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			if resp, err = client.Get("https://" + addr + "/api/health"); err == nil {
				break
			}
		}
		if err != nil {
			t.Fatalf("HTTPS request: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET /api/health without a token over HTTPS: %d, want 401", resp.StatusCode)
		}
		return resp.TLS.PeerCertificates[0].SerialNumber.Int64()
	}
	if got := serial(); got != 1 {
		t.Fatalf("serial %d, want 1", got)
	}

	// The login cookie is Secure over HTTPS.
	tok, _, _ := keeper.Issue("tls", time.Hour)
	resp, err := client.PostForm("https://"+addr+"/login", url.Values{"token": {tok}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if c := resp.Cookies(); len(c) != 1 || !c[0].Secure {
		t.Errorf("login over HTTPS: cookies %v, want one Secure cookie", c)
	}

	writeCert(t, dir, 2) // renewal
	time.Sleep(certRecheck + 50*time.Millisecond)
	if got := serial(); got != 2 {
		t.Errorf("after the files changed: serial %d, want 2", got)
	}

	if err := os.WriteFile(certFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(certRecheck + 50*time.Millisecond)
	if got := serial(); got != 2 {
		t.Errorf("after a broken renewal: serial %d, want the old 2", got)
	}
	warned := 0
	for _, e := range s.bus.Recent(0, "") {
		if strings.Contains(e.Message, "TLS certificate not reloaded") {
			warned++
		}
	}
	if warned != 1 {
		t.Errorf("%d warnings about the broken certificate, want 1", warned)
	}
}
