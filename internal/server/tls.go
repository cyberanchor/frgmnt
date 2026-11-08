package server

import (
	"bytes"
	"crypto/tls"
	"os"
	"sync"
	"time"
)

// certRecheck is how often the certificate files are read again. A
// variable so tests can shorten it.
var certRecheck = 10 * time.Second

// certReloader serves a certificate from two PEM files and reads them again
// every certRecheck, so a renewed certificate (certbot, acme.sh) is used
// without a restart. A new pair that does not load keeps the old one in
// use, and the error is reported once.
type certReloader struct {
	certFile, keyFile string
	onError           func(error)

	mu      sync.Mutex
	cert    *tls.Certificate
	certPEM []byte
	keyPEM  []byte
	checked time.Time
	lastErr string
}

func newCertReloader(certFile, keyFile string, onError func(error)) (*certReloader, error) {
	r := &certReloader{certFile: certFile, keyFile: keyFile, onError: onError}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(); err != nil {
		return nil, err
	}
	r.checked = time.Now()
	return r, nil
}

// loadLocked reads both files and swaps the certificate when they changed
// and form a valid pair.
func (r *certReloader) loadLocked() error {
	certPEM, err := os.ReadFile(r.certFile)
	if err != nil {
		return err
	}
	keyPEM, err := os.ReadFile(r.keyFile)
	if err != nil {
		return err
	}
	if r.cert != nil && bytes.Equal(certPEM, r.certPEM) && bytes.Equal(keyPEM, r.keyPEM) {
		return nil
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return err
	}
	r.cert, r.certPEM, r.keyPEM = &pair, certPEM, keyPEM
	return nil
}

// getCertificate is the tls.Config hook.
func (r *certReloader) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.checked) >= certRecheck {
		r.checked = time.Now()
		if err := r.loadLocked(); err != nil {
			if msg := err.Error(); msg != r.lastErr && r.onError != nil {
				r.lastErr = msg
				r.onError(err)
			}
		} else {
			r.lastErr = ""
		}
	}
	return r.cert, nil
}
