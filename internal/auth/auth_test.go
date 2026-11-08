package auth

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func openKeeper(t *testing.T, dir string) *Keeper {
	t.Helper()
	k, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// signClaims signs arbitrary claims with the keeper's key (for tests that
// need a token Issue would not make).
func signClaims(t *testing.T, k *Keeper, header string, c Claims) string {
	t.Helper()
	key, err := k.current()
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(c)
	unsigned := b64([]byte(header)) + "." + b64(payload)
	return unsigned + "." + mac(key, unsigned)
}

func TestIssueAndVerify(t *testing.T) {
	dir := t.TempDir()
	k, created, err := Open(dir)
	if err != nil || !created {
		t.Fatalf("Open: created=%v err=%v", created, err)
	}
	if st, err := os.Stat(k.Path()); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("key file: %v %v, want mode 0600", st.Mode(), err)
	}
	tok, issued, err := k.Issue("laptop", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c, err := k.Verify(tok)
	if err != nil || c.Name != "laptop" || c.Subject != Subject || c.ID != issued.ID {
		t.Fatalf("Verify: %+v %v", c, err)
	}
	if d := time.Until(c.ExpiresAt()); d < 59*time.Minute || d > time.Hour {
		t.Errorf("expires in %v, want about an hour", d)
	}
	if _, created, _ := Open(dir); created {
		t.Error("a second Open created a new key")
	}
	forever, _, _ := k.Issue("", 0)
	if c, err := k.Verify(forever); err != nil || !c.ExpiresAt().IsZero() {
		t.Errorf("ttl 0: %+v %v, want a token without expiry", c, err)
	}
}

func TestVerifyRejectsForgedTokens(t *testing.T) {
	k := openKeeper(t, t.TempDir())
	other := openKeeper(t, t.TempDir())
	good, _, _ := k.Issue("x", time.Hour)
	parts := strings.Split(good, ".")
	now := time.Now().Unix()

	forged, _ := json.Marshal(Claims{Subject: Subject, Name: "admin", IssuedAt: now, ID: "1"})
	fromOther, _, _ := other.Issue("x", time.Hour)
	cases := map[string]string{
		"payload changed, old signature": parts[0] + "." + b64(forged) + "." + parts[2],
		"alg none":                       b64([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + parts[1] + ".",
		"signed with another key":        fromOther,
		"two parts":                      parts[0] + "." + parts[1],
		"empty":                          "",
		"garbage":                        "not.a.token",
		"too long":                       strings.Repeat("a", maxTokenLen+1),
		"HS512 header":                   signClaims(t, k, `{"alg":"HS512","typ":"JWT"}`, Claims{Subject: Subject, IssuedAt: now, ID: "2"}),
		"another subject":                signClaims(t, k, `{"alg":"HS256","typ":"JWT"}`, Claims{Subject: "other", IssuedAt: now, ID: "3"}),
		"not valid yet":                  signClaims(t, k, `{"alg":"HS256","typ":"JWT"}`, Claims{Subject: Subject, IssuedAt: now, NotBefore: now + 3600, ID: "4"}),
	}
	for name, tok := range cases {
		if _, err := k.Verify(tok); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	expired := signClaims(t, k, `{"alg":"HS256","typ":"JWT"}`, Claims{Subject: Subject, IssuedAt: now - 7200, Expires: now - 120, ID: "5"})
	if _, err := k.Verify(expired); !errors.Is(err, ErrExpired) {
		t.Errorf("expired token: %v, want ErrExpired", err)
	}
	if _, err := k.Verify(good); err != nil {
		t.Errorf("the untouched token: %v", err)
	}
}

// "frgmnt token -rotate" runs as another process: the running panel must
// see the new key and refuse the tokens of the old one.
func TestRotateByAnotherKeeperRevokesTokens(t *testing.T) {
	dir := t.TempDir()
	panel := openKeeper(t, dir)
	tok, _, _ := panel.Issue("x", time.Hour)
	if _, err := panel.Verify(tok); err != nil {
		t.Fatal(err)
	}
	if err := openKeeper(t, dir).Rotate(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(recheck + 100*time.Millisecond)
	if _, err := panel.Verify(tok); err == nil {
		t.Error("a token of the old key still works after the rotation")
	}
	fresh, _, _ := openKeeper(t, dir).Issue("y", time.Hour)
	if _, err := panel.Verify(fresh); err != nil {
		t.Errorf("a token of the new key: %v", err)
	}
}

// A deleted or broken key file fails every check instead of letting
// anything through.
func TestBrokenKeyFileFailsClosed(t *testing.T) {
	dir := t.TempDir()
	k := openKeeper(t, dir)
	tok, _, _ := k.Issue("x", time.Hour)
	if err := os.WriteFile(k.Path(), []byte("zz\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(recheck + 100*time.Millisecond)
	if _, err := k.Verify(tok); err == nil {
		t.Error("token accepted with a broken key file")
	}
	if err := os.Remove(k.Path()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(recheck + 100*time.Millisecond)
	if _, err := k.Verify(tok); err == nil {
		t.Error("token accepted without a key file")
	}
}
