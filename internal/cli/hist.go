package cli

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

// durHist is a histogram of durations for the -debug summary: buckets
// 2^(1/8) apart (each about 9% wider than the one before) from 0.1 ms to
// about 3.5 minutes, so a quantile comes out at most 9% high, and memory
// does not grow with the run. Safe for concurrent use.
type durHist struct {
	counts [histBuckets]int64
	n      int64
	max    int64 // nanoseconds
}

const (
	histPerDoubling = 8
	histBuckets     = histPerDoubling * 21 // 0.1 ms * 2^21 = 210 s; the last bucket takes the rest
	histBase        = 100 * time.Microsecond
)

// histBucket is the bucket of d: the first one whose upper bound is not
// below d.
func histBucket(d time.Duration) int {
	if d <= histBase {
		return 0
	}
	i := int(math.Ceil(histPerDoubling * math.Log2(float64(d)/float64(histBase))))
	if i >= histBuckets {
		i = histBuckets - 1
	}
	return i
}

// histBound is the upper bound of bucket i.
func histBound(i int) time.Duration {
	return time.Duration(float64(histBase) * math.Pow(2, float64(i)/histPerDoubling))
}

func (h *durHist) add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	atomic.AddInt64(&h.counts[histBucket(d)], 1)
	atomic.AddInt64(&h.n, 1)
	atomicMax(&h.max, int64(d))
}

func (h *durHist) count() int64 { return atomic.LoadInt64(&h.n) }

// quantile returns the upper bound of the bucket that holds quantile q
// (0 < q <= 1), never more than the largest sample.
func (h *durHist) quantile(q float64) time.Duration {
	n := atomic.LoadInt64(&h.n)
	if n == 0 {
		return 0
	}
	max := time.Duration(atomic.LoadInt64(&h.max))
	rank := int64(math.Ceil(q * float64(n)))
	var seen int64
	for i := range h.counts {
		seen += atomic.LoadInt64(&h.counts[i])
		if seen >= rank {
			if b := histBound(i); b < max {
				return b
			}
			return max
		}
	}
	return max
}

// summary is "p50=… p95=… p99=… max=… (n=…)".
func (h *durHist) summary() string {
	return fmt.Sprintf("p50=%s p95=%s p99=%s max=%s (n=%d)",
		fmtDur(h.quantile(0.50)), fmtDur(h.quantile(0.95)), fmtDur(h.quantile(0.99)),
		fmtDur(time.Duration(atomic.LoadInt64(&h.max))), h.count())
}

// fmtDur prints a duration with about two significant figures: 0.4ms,
// 37ms, 230ms, 1.2s, 2m05s.
func fmtDur(d time.Duration) string {
	switch {
	case d < 10*time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	default:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	}
}
