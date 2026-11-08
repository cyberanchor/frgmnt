// Package tests contains the black-box unit suite for frgmnt.
//
// The tests live outside internal/ on purpose: they exercise the public
// checker API only, the same way the CLI and the web panel do. White-box
// tests for unexported helpers remain next to the code they test.
package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cyberanchor/frgmnt/internal/checker"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return body
}

// TestParseStatusFixtures pins every status class observed on the real
// site, including the two tricky layouts (BANNED + Sold, auction + buy-now).
func TestParseStatusFixtures(t *testing.T) {
	cases := []struct {
		name      string
		fixture   string
		want      checker.Status
		wantFound bool
		minConf   int
		evidence  []string
	}{
		{
			name: "auction", fixture: "auction.html", want: checker.StatusAuction,
			wantFound: true, minConf: 95,
			evidence: []string{`text="on auction"`, "timer", "place-bid"},
		},
		{
			name: "auction_with_buy_now", fixture: "auction_buynow.html",
			want: checker.StatusAuction, wantFound: true, minConf: 95,
			evidence: []string{"place-bid", "buy-now"},
		},
		{
			name: "fixed_price_sale", fixture: "for_sale.html",
			want: checker.StatusAvailable, wantFound: true, minConf: 95,
			evidence: []string{`text="for sale"`, "buy-now"},
		},
		{
			name: "sold", fixture: "sold.html", want: checker.StatusSold,
			wantFound: true, minConf: 95,
			evidence: []string{`text="sold"`, "purchased-on"},
		},
		{
			name: "taken", fixture: "taken.html", want: checker.StatusTaken,
			wantFound: true, minConf: 95,
			evidence: []string{`text="taken"`, "make-offer"},
		},
		{
			name: "banned_and_sold", fixture: "banned_sold.html",
			want: checker.StatusSold, wantFound: true, minConf: 95,
			evidence: []string{"badges=", `text="sold"`},
		},
		{
			name: "unknown_layout", fixture: "unknown.html",
			want: checker.StatusUnknown, wantFound: false, minConf: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info := checker.ParseStatusDetailed(fixture(t, tc.fixture))
			if info.Status != tc.want {
				t.Fatalf("status = %q, want %q (evidence: %s)", info.Status, tc.want, info.Evidence)
			}
			if info.Found != tc.wantFound {
				t.Fatalf("found = %v, want %v", info.Found, tc.wantFound)
			}
			if info.Confidence < tc.minConf {
				t.Fatalf("confidence = %d, want >= %d (evidence: %s)", info.Confidence, tc.minConf, info.Evidence)
			}
			for _, want := range tc.evidence {
				if !strings.Contains(info.Evidence, want) {
					t.Fatalf("evidence %q lacks %q", info.Evidence, want)
				}
			}
		})
	}
}

// TestForSaleIsNotAuction is the regression test for the original bug:
// fixed-price pages also carry tm-countdown-timer, which used to force
// "auction".
func TestForSaleIsNotAuction(t *testing.T) {
	info := checker.ParseStatusDetailed(fixture(t, "for_sale.html"))
	if info.Status != checker.StatusAvailable {
		t.Fatalf("status = %q, want available", info.Status)
	}
	if strings.Contains(info.Evidence, "text=\"on auction\"") {
		t.Fatalf("fixture parsed as an auction: %s", info.Evidence)
	}
}

func TestChallengePage(t *testing.T) {
	info := checker.ParseStatusDetailed(fixture(t, "challenge.html"))
	if !info.Challenge {
		t.Fatal("expected Challenge=true for a Cloudflare interstitial")
	}
}

// TestParseStatusCompatibility keeps the legacy three-value API working.
func TestParseStatusCompatibility(t *testing.T) {
	body := fixture(t, "sold.html")
	st, offset, found := checker.ParseStatus(body)
	if st != checker.StatusSold || !found || offset <= 0 {
		t.Fatalf("got (%q, %d, %v), want (sold, >0, true)", st, offset, found)
	}
}
