package checker

import (
	"bytes"
	"fmt"
	"html"
	"strings"
)

// ParseInfo is the detailed outcome of parsing one Fragment username page.
// It is deliberately richer than a bare Status: the web layer logs Evidence
// and can show Confidence, which makes misclassifications diagnosable
// without re-fetching the page.
type ParseInfo struct {
	Status     Status
	Confidence int    // 0..100; 0 when Found is false
	Evidence   string // human-readable list of the signals that decided it
	Offset     int    // byte offset of the status marker in body
	Found      bool   // false => page layout not recognised
	Challenge  bool   // true => Cloudflare challenge / anti-bot page
}

// statusTextMap maps the visible section-header label to a status. This is
// the strongest signal on a Fragment username page: the class alone is
// ambiguous ("tm-status-avail" is used for both auctions and fixed-price
// sales), while the label says exactly which one it is.
var statusTextMap = map[string]Status{
	"on auction":  StatusAuction,
	"for sale":    StatusAvailable,
	"on sale":     StatusAvailable,
	"available":   StatusAvailable,
	"sold":        StatusSold,
	"taken":       StatusTaken,
	"reserved":    StatusReserved,
	"unavailable": StatusSold,
}

// ParseStatus is the backwards-compatible wrapper used by older call sites.
func ParseStatus(body []byte) (Status, int, bool) {
	info := ParseStatusDetailed(body)
	return info.Status, info.Offset, info.Found
}

// ParseStatusDetailed classifies a Fragment username page using several
// independent signals and reports how the decision was made.
//
// Priority:
//
//  1. visible status label ("On auction", "For sale", "Sold", "Taken", ...)
//  2. buy-now button      -> available (fixed-price sale)
//  3. place-bid + timer   -> auction
//  4. make-offer button   -> taken
//  5. "Purchased on" text -> sold
//  6. tm-status-* class   -> low-confidence fallback
//
// The label is not merely decoration: Fragment renders both auctions and
// fixed-price sales with tm-status-avail and a countdown timer. The old
// "timer => auction" rule therefore misclassified every "For sale" name as
// an auction. The label plus the buy-now/place-bid hooks removes that
// ambiguity.
func ParseStatusDetailed(body []byte) ParseInfo {
	info := ParseInfo{Status: StatusUnknown}

	if looksLikeChallenge(body) {
		info.Challenge = true
		info.Evidence = "cloudflare challenge page"
		return info
	}

	idx := bytes.Index(body, []byte(statusMarker))
	if idx < 0 {
		return info
	}
	info.Offset = idx

	// A page may carry several badges in the same header. The common case
	// is BANNED + Sold: "BANNED" is an annotation, "Sold" is the commercial
	// status. Collect them and prefer the first one that maps to a status.
	badges := collectStatusBadges(body, idx, 4, 2048)
	if len(badges) == 0 {
		return info
	}
	chosen := badges[0]
	for _, b := range badges {
		if _, ok := statusTextMap[b.text]; ok {
			chosen = b
			break
		}
	}
	class := chosen.class
	text := chosen.text

	hasTimer := bytes.Contains(body, []byte(markerCountdown))
	hasMakeOffer := bytes.Contains(body, []byte(markerMakeOffer))
	hasPlaceBid := bytes.Contains(body, []byte(markerPlaceBid))
	hasBuyNow := bytes.Contains(body, []byte(markerBuyNow))
	hasPurchased := bytes.Contains(body, []byte(markerPurchased))

	evidence := make([]string, 0, 9)
	if text != "" {
		evidence = append(evidence, fmt.Sprintf("text=%q", text))
	}
	if class != "" {
		evidence = append(evidence, "class="+class)
	}
	if len(badges) > 1 {
		parts := make([]string, 0, len(badges))
		for _, b := range badges {
			parts = append(parts, b.text+"/"+b.class)
		}
		evidence = append(evidence, "badges="+strings.Join(parts, "|"))
	}
	if hasTimer {
		evidence = append(evidence, "timer")
	}
	if hasMakeOffer {
		evidence = append(evidence, "make-offer")
	}
	if hasPlaceBid {
		evidence = append(evidence, "place-bid")
	}
	if hasBuyNow {
		evidence = append(evidence, "buy-now")
	}
	if hasPurchased {
		evidence = append(evidence, "purchased-on")
	}
	info.Evidence = strings.Join(evidence, "; ")

	// 1. The visible label is authoritative.
	if st, ok := statusTextMap[text]; ok {
		info.Status = st
		info.Found = true
		info.Confidence = 90
		if classMatchesStatus(st, class) {
			info.Confidence = 99
		}
		if signalsConflict(st, hasTimer, hasPlaceBid, hasBuyNow, hasMakeOffer) {
			info.Confidence -= 10
		}
		return info
	}

	// 2-5. Structural fallbacks when the label could not be read.
	switch {
	case hasBuyNow:
		info.Status = StatusAvailable
		info.Confidence = 85
	case hasPlaceBid && hasTimer:
		info.Status = StatusAuction
		info.Confidence = 90
	case hasPlaceBid:
		info.Status = StatusAvailable
		info.Confidence = 60
	case hasMakeOffer:
		info.Status = StatusTaken
		info.Confidence = 85
	case hasPurchased:
		info.Status = StatusSold
		info.Confidence = 85
	case class == "taken":
		info.Status = StatusTaken
		info.Confidence = 70
	case class == "unavail":
		info.Status = StatusSold
		info.Confidence = 70
	case class == "avail":
		// "avail" + timer with no label and no button is ambiguous. Do not
		// guess "auction": that was the old, wrong default.
		if hasTimer {
			info.Status = StatusUnknown
			info.Found = false
			info.Confidence = 25
			return info
		}
		info.Status = StatusAvailable
		info.Confidence = 45
	default:
		info.Status = StatusUnknown
		info.Found = false
		return info
	}
	info.Found = true
	return info
}

// classMatchesStatus reports whether the tm-status-* suffix agrees with the
// status derived from the visible label. Agreement pushes confidence to 99;
// disagreement keeps it at 90 but is surfaced in Evidence.
func classMatchesStatus(st Status, class string) bool {
	want := ""
	switch st {
	case StatusAuction, StatusAvailable:
		want = "avail"
	case StatusTaken:
		want = "taken"
	case StatusSold:
		want = "unavail"
	case StatusReserved:
		want = "reserved"
	}
	return want != "" && class == want
}

// signalsConflict returns true when a structural signal contradicts the
// label-derived status. This deliberately ignores the shared countdown timer
// on fixed-price sales.
func signalsConflict(st Status, timer, placeBid, buyNow, makeOffer bool) bool {
	switch st {
	case StatusSold, StatusTaken:
		return timer || placeBid || buyNow
	case StatusAuction:
		// An auction may legitimately offer a buy-now option next to
		// bidding; the label still says "On auction". Not a conflict.
		return false
	case StatusAvailable:
		return makeOffer && !buyNow
	}
	return false
}

// statusBadge is one tm-section-header-status entry in the page header.
type statusBadge struct {
	text  string
	class string
}

// collectStatusBadges returns up to max badges found within window bytes
// starting at first. Fragmented pages can render more than one badge, e.g.
// "BANNED" followed by "Sold".
func collectStatusBadges(body []byte, first, max, window int) []statusBadge {
	limit := first + window
	if limit > len(body) {
		limit = len(body)
	}
	var out []statusBadge
	pos := first
	for len(out) < max && pos < limit {
		i := bytes.Index(body[pos:limit], []byte(statusMarker))
		if i < 0 {
			break
		}
		at := pos + i
		classWindow := body[at+len(statusMarker):]
		if len(classWindow) > 512 {
			classWindow = classWindow[:512]
		}
		out = append(out, statusBadge{
			text:  extractStatusText(body, at),
			class: extractStatusClass(classWindow),
		})
		pos = at + len(statusMarker)
	}
	return out
}

// extractStatusText returns the lower-cased text of the status label.
// Fragment renders it as:
//
//	<span class="tm-section-header-status tm-status-avail">For sale</span>
//
// Nested inline tags are tolerated.
func extractStatusText(body []byte, idx int) string {
	gt := bytes.IndexByte(body[idx:], '>')
	if gt < 0 {
		return ""
	}
	start := idx + gt + 1
	if start >= len(body) {
		return ""
	}
	rest := body[start:]
	end := bytes.Index(rest, []byte("</"))
	if end < 0 || end > 200 {
		if lt := bytes.IndexByte(rest, '<'); lt >= 0 && lt <= 200 {
			end = lt
		} else {
			return ""
		}
	}
	raw := rest[:end]
	if bytes.IndexByte(raw, '<') >= 0 {
		raw = stripTags(raw)
	}
	txt := html.UnescapeString(string(raw))
	return strings.ToLower(strings.Join(strings.Fields(txt), " "))
}

func stripTags(b []byte) []byte {
	out := make([]byte, 0, len(b))
	inTag := false
	for _, c := range b {
		switch {
		case c == '<':
			inTag = true
		case c == '>':
			inTag = false
		case !inTag:
			out = append(out, c)
		}
	}
	return out
}

// looksLikeChallenge detects Cloudflare's interstitial. Such a page is a
// transport failure, not "unknown username", so callers should retry or
// surface an error instead of caching it.
func looksLikeChallenge(body []byte) bool {
	if len(body) > 4096 {
		body = body[:4096]
	}
	return bytes.Contains(body, []byte("cf-mitigated")) ||
		bytes.Contains(body, []byte("Just a moment...")) ||
		bytes.Contains(body, []byte("_cf_chl_opt"))
}

func extractStatusClass(window []byte) string {
	i := bytes.Index(window, []byte(statusClassPrefix))
	if i < 0 {
		return ""
	}
	start := i + len(statusClassPrefix)
	if start >= len(window) {
		return ""
	}
	end := start
	for end < len(window) {
		c := window[end]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			end++
			continue
		}
		break
	}
	if end == start {
		return ""
	}
	return string(window[start:end])
}
