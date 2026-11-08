// Package auth protects the web panel with access tokens: JWTs signed with
// HS256 by a key kept in the data directory (auth.key). There are no users
// and no passwords. Whoever holds a valid token gets in; "frgmnt token"
// issues one, and rotating the key revokes every token at once.
package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Subject is the "sub" claim of every panel token.
const Subject = "frgmnt-panel"

// KeyFile is the name of the signing key in the data directory.
const KeyFile = "auth.key"

const (
	keySize     = 32              // bytes of HMAC-SHA256 key
	minKeySize  = 16              // shortest key accepted from the file
	maxTokenLen = 4096            // longer strings are not tokens of ours
	leeway      = time.Minute     // clock skew allowed for exp and nbf
	recheck     = 1 * time.Second // how often the key file is looked at
)

// Errors returned by Verify.
var (
	ErrInvalid = errors.New("invalid token")
	ErrExpired = errors.New("token expired")
)

// Claims are the fields of a panel token.
type Claims struct {
	Subject   string `json:"sub"`
	Name      string `json:"name,omitempty"`
	IssuedAt  int64  `json:"iat"`
	Expires   int64  `json:"exp,omitempty"` // unix seconds; 0 = never
	NotBefore int64  `json:"nbf,omitempty"`
	ID        string `json:"jti"`
}

// ExpiresAt returns the expiry time, or the zero time for "never".
func (c Claims) ExpiresAt() time.Time {
	if c.Expires == 0 {
		return time.Time{}
	}
	return time.Unix(c.Expires, 0)
}

// Keeper holds the signing key of one data directory. It reads the key file
// again when it changes, so a rotation done by "frgmnt token -rotate" takes
// effect in a running panel without a restart.
type Keeper struct {
	path string

	mu      sync.Mutex
	key     []byte
	checked time.Time
}

// Open returns the Keeper of dataDir. It creates a new random key (file
// mode 0600) when there is none yet and reports that in created.
func Open(dataDir string) (k *Keeper, created bool, err error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, false, err
	}
	k = &Keeper{path: filepath.Join(dataDir, KeyFile)}
	if _, err := os.Stat(k.path); errors.Is(err, os.ErrNotExist) {
		if err := k.writeKey(); err != nil {
			return nil, false, err
		}
		created = true
	}
	if _, err := k.current(); err != nil {
		return nil, false, err
	}
	return k, created, nil
}

// Path returns the key file.
func (k *Keeper) Path() string { return k.path }

func (k *Keeper) writeKey() error {
	key := make([]byte, keySize)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	tmp := k.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(hex.EncodeToString(key)+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, k.path)
}

// Rotate writes a new key: every token issued before it stops working.
func (k *Keeper) Rotate() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.writeKey(); err != nil {
		return err
	}
	k.key = nil // read the new one on the next use
	return nil
}

// current returns the key, read from the file again at most once per
// recheck interval. The file is read rather than compared by time and
// size: a key rotated within the same tick of the file system clock has
// the same size and time, and the old key would have stayed in use. A
// missing or broken key file fails every check (fail closed).
func (k *Keeper) current() ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.key != nil && time.Since(k.checked) < recheck {
		return k.key, nil
	}
	raw, err := os.ReadFile(k.path)
	k.checked = time.Now()
	if err != nil {
		k.key = nil
		return nil, fmt.Errorf("access key: %w", err)
	}
	key, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(key) < minKeySize {
		k.key = nil
		return nil, fmt.Errorf("access key %s: not a hex key of at least %d bytes", k.path, minKeySize)
	}
	k.key = key
	return key, nil
}

// Issue returns a new token named name (shown by the panel's log, nothing
// else). ttl 0 means it never expires.
func (k *Keeper) Issue(name string, ttl time.Duration) (string, Claims, error) {
	key, err := k.current()
	if err != nil {
		return "", Claims{}, err
	}
	id := make([]byte, 8)
	if _, err := rand.Read(id); err != nil {
		return "", Claims{}, err
	}
	now := time.Now()
	c := Claims{Subject: Subject, Name: name, IssuedAt: now.Unix(), ID: hex.EncodeToString(id)}
	if ttl > 0 {
		c.Expires = now.Add(ttl).Unix()
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", Claims{}, err
	}
	unsigned := jwtHeader + "." + b64(payload)
	return unsigned + "." + mac(key, unsigned), c, nil
}

// jwtHeader is the only header this package signs and accepts.
var jwtHeader = b64([]byte(`{"alg":"HS256","typ":"JWT"}`))

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func mac(key []byte, s string) string {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(s))
	return b64(h.Sum(nil))
}

// Verify checks a token: the HS256 signature with the current key first
// (nothing of an unsigned token is parsed), then that the header says
// HS256, the subject, and the expiry and not-before times. Other
// algorithms, "none" included, are rejected.
func (k *Keeper) Verify(token string) (Claims, error) {
	var c Claims
	if len(token) > maxTokenLen {
		return c, ErrInvalid
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return c, ErrInvalid
	}
	key, err := k.current()
	if err != nil {
		return c, err
	}
	if !hmac.Equal([]byte(mac(key, parts[0]+"."+parts[1])), []byte(parts[2])) {
		return c, ErrInvalid
	}
	rawHeader, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return c, ErrInvalid
	}
	var hdr struct {
		Alg string `json:"alg"`
	}
	if json.Unmarshal(rawHeader, &hdr) != nil || hdr.Alg != "HS256" {
		return c, ErrInvalid
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(payload, &c) != nil || c.Subject != Subject {
		return Claims{}, ErrInvalid
	}
	now := time.Now()
	if c.Expires != 0 && now.After(time.Unix(c.Expires, 0).Add(leeway)) {
		return c, ErrExpired
	}
	if c.NotBefore != 0 && now.Add(leeway).Before(time.Unix(c.NotBefore, 0)) {
		return c, ErrInvalid
	}
	return c, nil
}
