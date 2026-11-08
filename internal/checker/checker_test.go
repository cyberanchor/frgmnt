// Tests for the pure functions inside the checker library. Network-bound
// behaviour is covered by the smoke tests run during release; these unit
// tests pin down the parsing/normalisation rules that are easy to break.
package checker

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// ParseStatus
// -----------------------------------------------------------------------------

func TestParseStatus(t *testing.T) {
	cases := []struct {
		name string
		body string
		want Status
	}{
		{
			name: "free_or_unset_marker_absent",
			body: `<html><body>no marker here</body></html>`,
			want: StatusUnknown,
		},
		{
			name: "timer_alone_is_ambiguous",
			body: `<div class="tm-section-header-status tm-status-avail">` +
				`<span class="tm-countdown-timer">2h</span></div>`,
			want: StatusUnknown,
		},
		{
			name: "make_offer_button_marks_taken",
			body: `<div class="tm-section-header-status tm-status-avail">` +
				`<button class="js-make-offer-btn">Make offer</button></div>`,
			want: StatusTaken,
		},
		{
			name: "place_bid_button_marks_available",
			body: `<div class="tm-section-header-status tm-status-avail">` +
				`<button class="js-place-bid-btn">Place bid</button></div>`,
			want: StatusAvailable,
		},
		{
			name: "class_taken_alone",
			body: `<div class="tm-section-header-status tm-status-taken">Taken</div>`,
			want: StatusTaken,
		},
		{
			name: "class_unavail_means_sold",
			body: `<div class="tm-section-header-status tm-status-unavail">Sold</div>`,
			want: StatusSold,
		},
		{
			name: "class_avail_alone_no_buttons",
			body: `<div class="tm-section-header-status tm-status-avail">Avail</div>`,
			want: StatusAvailable,
		},
		{
			name: "marker_but_unknown_class",
			body: `<div class="tm-section-header-status tm-status-xyz">?</div>`,
			want: StatusUnknown,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _, _ := ParseStatus([]byte(c.body))
			if got != c.want {
				t.Fatalf("ParseStatus = %q, want %q", got, c.want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// isReservedRedirect
// -----------------------------------------------------------------------------

func TestIsReservedRedirect(t *testing.T) {
	cases := []struct {
		loc  string
		want bool
	}{
		{loc: "/", want: true},
		{loc: "https://fragment.com/", want: true},
		{loc: "https://fragment.com", want: true},
		{loc: "", want: false},
		{loc: "/login", want: false},
		{loc: "https://fragment.com/buy", want: false},
		{loc: "https://example.com/", want: false},
		{loc: "/?ref=foo", want: false},
	}
	for _, c := range cases {
		t.Run(c.loc, func(t *testing.T) {
			if got := isReservedRedirect(c.loc); got != c.want {
				t.Fatalf("isReservedRedirect(%q) = %v, want %v", c.loc, got, c.want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// parseRetryAfter
// -----------------------------------------------------------------------------

func TestParseRetryAfter(t *testing.T) {
	fb := 7 * time.Second
	t.Run("integer_seconds", func(t *testing.T) {
		if got := parseRetryAfter("3", fb); got != 3*time.Second {
			t.Fatalf("got %v, want 3s", got)
		}
	})
	t.Run("zero_seconds", func(t *testing.T) {
		if got := parseRetryAfter("0", fb); got != 0 {
			t.Fatalf("got %v, want 0", got)
		}
	})
	t.Run("negative_falls_back", func(t *testing.T) {
		if got := parseRetryAfter("-5", fb); got != fb {
			t.Fatalf("got %v, want fallback %v", got, fb)
		}
	})
	t.Run("garbage_falls_back", func(t *testing.T) {
		if got := parseRetryAfter("nope", fb); got != fb {
			t.Fatalf("got %v, want fallback %v", got, fb)
		}
	})
	t.Run("blank_falls_back", func(t *testing.T) {
		if got := parseRetryAfter("   ", fb); got != fb {
			t.Fatalf("got %v, want fallback %v", got, fb)
		}
	})
	t.Run("http_date_in_future", func(t *testing.T) {
		future := time.Now().UTC().Add(4 * time.Second).Format(http.TimeFormat)
		got := parseRetryAfter(future, fb)
		if got <= 0 || got > 5*time.Second {
			t.Fatalf("got %v, want ~4s", got)
		}
	})
	t.Run("http_date_in_past", func(t *testing.T) {
		past := time.Now().UTC().Add(-1 * time.Hour).Format(http.TimeFormat)
		if got := parseRetryAfter(past, fb); got != fb {
			t.Fatalf("got %v, want fallback %v", got, fb)
		}
	})
}

// -----------------------------------------------------------------------------
// ParseNames
// -----------------------------------------------------------------------------

func TestParseNames(t *testing.T) {
	cases := []struct {
		name         string
		blob         string
		wantAccepted []string
		wantRejected int
	}{
		{
			name:         "empty",
			blob:         "",
			wantAccepted: nil,
			wantRejected: 0,
		},
		{
			name:         "single_clean",
			blob:         "durov",
			wantAccepted: []string{"durov"},
		},
		{
			name:         "strip_leading_at",
			blob:         "@pavel",
			wantAccepted: []string{"pavel"},
		},
		{
			name:         "mixed_separators",
			blob:         "alice, bob\nclaire\tdan ; eve",
			wantAccepted: []string{"alice", "bob", "claire", "dan", "eve"},
		},
		{
			name:         "dedupe_case_insensitive",
			blob:         "Alice\nalice\nALICE\n",
			wantAccepted: []string{"alice"},
		},
		{
			name:         "rejects_non_ascii",
			blob:         "good_name\nдуров\nbad!chars",
			wantAccepted: []string{"good_name"},
			wantRejected: 2,
		},
		{
			name:         "rejects_too_long",
			blob:         "short_one\n" + strings.Repeat("a", 65),
			wantAccepted: []string{"short_one"},
			wantRejected: 1,
		},
		{
			name:         "rejects_uppercase_after_strip",
			blob:         "good\nBAD-name", // dash is invalid for fragment usernames
			wantAccepted: []string{"good"},
			wantRejected: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotNames, gotRej := ParseNames(c.blob)
			if !equalStrSlice(gotNames, c.wantAccepted) {
				t.Fatalf("names = %v, want %v", gotNames, c.wantAccepted)
			}
			if gotRej != c.wantRejected {
				t.Fatalf("rejected = %d, want %d", gotRej, c.wantRejected)
			}
		})
	}
}

func equalStrSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// normalizeConfig (clamps + defaults)
// -----------------------------------------------------------------------------

func TestNormalizeConfig_AppliesFloors(t *testing.T) {
	c := normalizeConfig(Config{}) // all zeros
	if c.Workers <= 0 {
		t.Fatalf("Workers must be >0, got %d", c.Workers)
	}
	if c.MaxConns <= 0 {
		t.Fatalf("MaxConns must be >0, got %d", c.MaxConns)
	}
	if c.Timeout <= 0 {
		t.Fatalf("Timeout must be >0, got %v", c.Timeout)
	}
	if c.ReadLimit <= 0 {
		t.Fatalf("ReadLimit must be >0, got %d", c.ReadLimit)
	}
}

func TestDefaultConfigIsValid(t *testing.T) {
	c := DefaultConfig()
	n := normalizeConfig(c)
	if n.Workers != c.Workers {
		t.Errorf("DefaultConfig should already be normalised: Workers %d -> %d", c.Workers, n.Workers)
	}
}
