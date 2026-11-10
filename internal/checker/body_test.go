package checker

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// unknownPage is a page without a status marker, longer than the 1 KB the
// tests read for the parser.
var unknownPage = "<html><body>" + strings.Repeat("<p>nothing to see</p>", 150) + "</body></html>"

// pageServer answers: unknown* with unknownPage (gzip when asked), sold*
// with a sold page, chal* with a Cloudflare challenge, boom* with HTTP 500,
// big* with a 1.5 MB page without marker, bin* with bytes that are not
// UTF-8, the rest with a free redirect. Every answer sets a cookie.
func pageServer(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		http.SetCookie(w, &http.Cookie{Name: "stel_ssid", Value: "SECRETVALUE", Path: "/"})
		w.Header().Set("X-Test", "yes")
		switch {
		case strings.HasPrefix(name, "unknown"):
			if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				var b bytes.Buffer
				gz := gzip.NewWriter(&b)
				_, _ = gz.Write([]byte(unknownPage))
				_ = gz.Close()
				w.Header().Set("Content-Encoding", "gzip")
				_, _ = w.Write(b.Bytes())
				return
			}
			_, _ = w.Write([]byte(unknownPage))
		case strings.HasPrefix(name, "sold"):
			_, _ = w.Write([]byte(soldPageFixture))
		case strings.HasPrefix(name, "chal"):
			w.Header().Set("Cf-Mitigated", "challenge")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("<html><title>Just a moment...</title></html>"))
		case strings.HasPrefix(name, "boom"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("upstream exploded"))
		case strings.HasPrefix(name, "big"):
			_, _ = w.Write([]byte(strings.Repeat("x", 1536<<10)))
		case strings.HasPrefix(name, "bin"):
			_, _ = w.Write([]byte{0xff, 0xfe, 0x00, 0x80, 'a'})
		default:
			freeRedirect(w, r)
		}
	})
}

func traceOf(t *testing.T, bodies string, names ...string) map[string]TraceEvent {
	t.Helper()
	var log eventLog
	c := New(Config{Workers: 1, Retries: 0, Timeout: 3 * time.Second, ReadLimit: 1024, TraceBodies: bodies})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	for _, n := range names {
		_ = c.CheckOne(context.Background(), n)
	}
	out := map[string]TraceEvent{}
	for _, e := range log.of("attempt") {
		out[e.Name] = e
	}
	return out
}

// The trace kept no page, only its size and what the parser found, so an
// unknown answer could not be looked at afterwards. By default
// ("anomalies") it keeps the whole decoded page of unknown answers,
// challenges, unexpected codes and errors; "all" keeps every page; "none"
// none.
func TestTraceKeepsPages(t *testing.T) {
	pageServer(t)
	names := []string{"unknown1", "sold1", "free1", "boom1", "chal1"} // the challenge last: it closes the gate
	anomalies := traceOf(t, "", names...)
	if e := anomalies["unknown1"]; e.Body != unknownPage || e.BodyTruncated || e.Bytes != 1024 ||
		e.ContentEncoding != "gzip" || e.BodyRawBytes == 0 || e.BodyRawBytes >= int64(len(unknownPage)) {
		t.Errorf("unknown page: body %d bytes (want %d, decoded), read %d, raw %d, encoding %q",
			len(e.Body), len(unknownPage), e.Bytes, e.BodyRawBytes, e.ContentEncoding)
	}
	for _, n := range []string{"chal1", "boom1"} {
		if anomalies[n].Body == "" {
			t.Errorf("%s: no page kept for an anomaly", n)
		}
	}
	if b := anomalies["boom1"].Body; b != "upstream exploded" {
		t.Errorf("HTTP 500 page %q", b)
	}
	for _, n := range []string{"sold1", "free1"} {
		if anomalies[n].Body != "" {
			t.Errorf("%s: page kept with anomalies only", n)
		}
	}
	all := traceOf(t, BodiesAll, names...)
	if all["sold1"].Body != soldPageFixture || !strings.Contains(all["free1"].Body, "Found") {
		t.Errorf("with all: sold %q, free %q", all["sold1"].Body, all["free1"].Body)
	}
	for n, e := range traceOf(t, BodiesNone, names...) {
		if e.Body != "" || e.BodyB64 != "" {
			t.Errorf("%s: page kept with none", n)
		}
	}
}

// A page is kept up to 1 MB (and marked cut); bytes that are not UTF-8 go
// to body_b64 so they come back exactly.
func TestTraceKeptPageLimits(t *testing.T) {
	pageServer(t)
	got := traceOf(t, BodiesAll, "big1", "bin1")
	if e := got["big1"]; len(e.Body) != maxKeptBody || !e.BodyTruncated || e.BodyRawBytes != 1536<<10 {
		t.Errorf("big page: kept %d bytes, truncated %v, received %d", len(e.Body), e.BodyTruncated, e.BodyRawBytes)
	}
	e := got["bin1"]
	raw, err := base64.StdEncoding.DecodeString(e.BodyB64)
	if err != nil || e.Body != "" || !bytes.Equal(raw, []byte{0xff, 0xfe, 0x00, 0x80, 'a'}) {
		t.Errorf("binary page: body %q, b64 %q (%v)", e.Body, e.BodyB64, err)
	}
}

// The trace had a few headers of the answer and none of the request. Now
// every header of both: the request as sent (Host, the cookies of the jar
// by name), the answer with its status line and length; cookie values are
// hidden in both.
func TestTraceHasAllHeaders(t *testing.T) {
	pageServer(t)
	var log eventLog
	c := New(Config{Workers: 1, Timeout: 3 * time.Second, UserAgent: "test-agent"})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	_ = c.CheckOne(context.Background(), "free1")
	_ = c.CheckOne(context.Background(), "free2") // sends the cookie back
	raw := log.jsonLines(t, "SECRETVALUE")
	attempts := log.of("attempt")
	first, second := attempts[0], attempts[1]
	if first.Method != "GET" || first.ReqHeaders.Get("User-Agent") != "test-agent" ||
		first.ReqHeaders.Get("Accept-Encoding") != "gzip" || first.ReqHeaders.Get("Host") == "" ||
		first.ReqHeaders.Get("Cookie") != "" {
		t.Errorf("first request headers %v", first.ReqHeaders)
	}
	if got := second.ReqHeaders.Get("Cookie"); got != "stel_ssid=…" {
		t.Errorf("second request Cookie %q, want the name only", got)
	}
	if first.StatusLine != "302 Found" || first.RespHeaders.Get("X-Test") != "yes" ||
		first.RespHeaders.Get("Set-Cookie") != "stel_ssid=…; Path=/" || first.ContentLength == nil {
		t.Errorf("answer: %q %v len %v", first.StatusLine, first.RespHeaders, first.ContentLength)
	}
	if !strings.Contains(raw, `"resp_headers"`) || !strings.Contains(raw, `"req_headers"`) {
		t.Errorf("trace JSON lacks the headers:\n%s", raw)
	}
}

// Parsing a page and writing it to the page cache happen on this machine
// but were counted as network time. They are Work now: Took = Net + Wait +
// Work, the attempt shows parse_us and the cache write, and a cache hit
// has no network time at all.
func TestWorkIsNotNetworkTime(t *testing.T) {
	pageServer(t)
	var log eventLog
	dir := t.TempDir()
	c := New(Config{Workers: 1, Timeout: 3 * time.Second, CacheDir: dir, CacheTTL: time.Hour})
	defer c.Close()
	c.SetLogger(Logger{Event: log.add})
	first := c.CheckOne(context.Background(), "sold1")
	if first.Status != StatusSold || first.Work <= 0 || first.Net <= 0 || first.Net+first.Wait+first.Work != first.Took {
		t.Errorf("network check: %s took %v = net %v + wait %v + work %v", first.Status, first.Took, first.Net, first.Wait, first.Work)
	}
	a := log.of("attempt")[0]
	if a.ParseUs <= 0 || a.CacheWriteBytes != len(soldPageFixture) || a.CacheWriteMs <= 0 || a.WorkMs <= 0 || a.CacheError != "" {
		t.Errorf("attempt: parse %.1fus, cache write %d bytes in %.3fms, work %.3fms, error %q",
			a.ParseUs, a.CacheWriteBytes, a.CacheWriteMs, a.WorkMs, a.CacheError)
	}
	hit := c.CheckOne(context.Background(), "sold1")
	if !hit.Cached || hit.Net != 0 || hit.Work <= 0 {
		t.Errorf("cache hit: cached %v, net %v, work %v", hit.Cached, hit.Net, hit.Work)
	}
	if e := log.of("cache"); len(e) != 2 || e[1].Cache != "hit" || e[1].ReadMs <= 0 {
		t.Errorf("cache events %+v", e)
	}
}

// A broken page-cache entry was a silent miss. It is a miss that says why
// (a "cache" event with "error", Stats.CacheErrors), and the name is
// checked over the network.
func TestBrokenCacheEntryIsVisible(t *testing.T) {
	pageServer(t)
	dir := t.TempDir()
	c := New(Config{Workers: 1, Timeout: 3 * time.Second, CacheDir: dir, CacheTTL: time.Hour})
	defer c.Close()
	if res := c.CheckOne(context.Background(), "sold2"); res.Status != StatusSold {
		t.Fatalf("first check: %s", res.Status)
	}
	_, meta := c.cache.paths("sold2")
	if err := os.WriteFile(meta, []byte("{broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	var log eventLog
	c.SetLogger(Logger{Event: log.add})
	if res := c.CheckOne(context.Background(), "sold2"); res.Status != StatusSold || res.Cached {
		t.Fatalf("after breaking the entry: %s cached=%v", res.Status, res.Cached)
	}
	e := log.of("cache")
	if len(e) != 1 || e[0].Cache != "error" || !strings.Contains(e[0].Error, filepath.Base(meta)) {
		t.Errorf("cache events %+v", e)
	}
	if st := c.Snapshot(); st.CacheErrors != 1 || st.CacheMisses != 2 {
		t.Errorf("stats: %d errors, %d misses", st.CacheErrors, st.CacheMisses)
	}
}
