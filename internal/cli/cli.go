// Package cli is the console mode of frgmnt ("frgmnt check"): it reads a
// list of usernames, checks them with the engine in internal/checker (the
// same one the web panel uses) and writes one line per name to a results
// file.
//
// Usage:
//
//	frgmnt check -input names.txt -output results.txt
//	frgmnt check -input names.txt -output results.txt -workers 200 -conns 8 -filter free
//	frgmnt check -input names.txt -output results.txt -resume -append
//	frgmnt check -input - -output results.txt           # stdin
//	frgmnt check -input names.txt -output results.txt -debug
//	frgmnt check -input names.txt -output results.txt -trace run.jsonl.gz
//
// Diagnostics (independent: either, both or none):
//
//	(default)    — results + progress line once per second
//	-debug       — one line per HTTP try with its timings, the verdicts and
//	               cache use on the console, timing figures at the end
//	-trace FILE  — the structured trace of the run in FILE as JSON Lines
//	               (gzip when FILE ends in .gz): every request, cache lookup,
//	               name and engine event; it keeps the end of the run within
//	               -trace-max (default 1G) and stops when the disk runs low
//
// Failure handling:
//
//	-retries N       — maximum extra attempts per request (default 2)
//	-retry-delay D   — fixed pause between attempts (default 500ms)
//	-rl-wait-max D   — upper bound on the wait imposed by HTTP 429 (default 30s)
//
// SIGINT / SIGTERM trigger a graceful shutdown: checks in flight are
// cancelled (their names stay unchecked for -resume), the output buffer is
// flushed, and the final statistics are printed.
package cli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	rpprof "runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
	"github.com/cyberanchor/frgmnt/internal/profiling"
	"github.com/cyberanchor/frgmnt/internal/rtstats"
	"github.com/cyberanchor/frgmnt/internal/tracelog"
	"github.com/cyberanchor/frgmnt/internal/version"
)

// Version is the release of frgmnt (internal/version).
const Version = version.Version

// ProgName is used in usage text and -version; the unified binary sets it
// to "frgmnt check".
var ProgName = "fragment_checker"

const (
	// defaultWorkers matches the web panel (checker.DefaultConfig). It used
	// to be 500: 500 requests at once from one IP, with no rate limit.
	defaultWorkers         = 50
	defaultMaxConnsPerHost = 8 // HTTP/2 multiplex: a few sockets carry hundreds of streams
	defaultTimeout         = 15 * time.Second
	defaultCacheTTL        = 2 * time.Minute
	defaultCacheSoldTTL    = 7 * 24 * time.Hour

	// safetyMargin is added to the largest status offset seen when the
	// -debug summary suggests a -readlimit.
	safetyMargin = 2048

	// scannerBufMax is the maximum line size accepted by the input scanner.
	scannerBufMax = 1 << 20

	// Channel capacities.
	tasksChanCap   = 4096
	resultsChanCap = 4096

	progressInterval = 1 * time.Second
	flushInterval    = 100 * time.Millisecond
)

// -----------------------------------------------------------------------------
// Log level
// -----------------------------------------------------------------------------

// LogLevel is the console verbosity.
type LogLevel int

const (
	// LevelNormal prints results and a periodic progress line only.
	LevelNormal LogLevel = iota
	// LevelDebug adds one line per HTTP try with its timings, the HTTP
	// codes, redirects, verdicts and cache use. There used to be a trace
	// level above it (every step of every request as text, which also
	// turned debug on); that detail goes to the -trace file now.
	LevelDebug
)

func (l LogLevel) String() string {
	if l == LevelDebug {
		return "debug"
	}
	return "normal"
}

// logLevel is the active verbosity, set by Main from -debug.
var logLevel LogLevel

// debugf prints when logLevel >= LevelDebug.
func debugf(format string, args ...any) {
	if logLevel >= LevelDebug {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// warnf always prints.
func warnf(format string, args ...any) {
	log.Printf("[WARN] "+format, args...)
}

// -----------------------------------------------------------------------------
// Status
// -----------------------------------------------------------------------------

// Status and its values are the engine's; the aliases keep this file short.
type Status = checker.Status

const (
	StatusFree      = checker.StatusFree      // 302 -> /?query=NAME: Fragment has no page; a regular account may hold it
	StatusReserved  = checker.StatusReserved  // 302 -> / that persists while a probe name is fine
	StatusAuction   = checker.StatusAuction   // listed on an active auction
	StatusTaken     = checker.StatusTaken     // already owned (Telegram-side)
	StatusSold      = checker.StatusSold      // purchased on Fragment, no longer listed
	StatusAvailable = checker.StatusAvailable // can be bought / a new auction can be started
	StatusUnknown   = checker.StatusUnknown   // page returned but status could not be determined
	StatusError     = checker.StatusError     // network / HTTP / parsing failure
)

// -----------------------------------------------------------------------------
// Stats
//
// NOTE: all int64 fields are grouped at the top to guarantee 8-byte alignment
// of the atomic counters on 32-bit architectures. Do not add non-int64 fields
// above startTime.
// -----------------------------------------------------------------------------

// Stats holds what the console adds to the engine's counters
// (checker.Stats): the number of names read, the start time, and the
// latency and marker-offset figures of the -debug summary.
type Stats struct {
	total           int64
	statusOffsetMax int64

	// listNames is the number of names in the input file, counted before
	// the run (-1: unknown, the input is a pipe); dropped counts the names
	// the producer left out so far (duplicates, -resume skips). The names
	// left to queue are listNames - dropped - total.
	listNames int64
	dropped   int64

	// The -debug summary: the network time of a name (its own requests)
	// for redirects (free/reserved, no body) and for pages, the time it
	// waited (starts, pauses, retry delays, probes), and how many tries it
	// took (1, 2, 3, 4 or more). The summary used to give avg/min/max of the
	// whole time of a name, which mixed the network with the queue.
	netRedirect durHist
	netPage     durHist
	wait        durHist
	connWait    durHist
	work        durHist
	tries       [4]int64

	startTime time.Time
}

// newStats returns a Stats with the start time set and min-trackers seeded
// to MaxInt64 so the first observed sample wins the CAS.
func newStats() *Stats {
	return &Stats{
		startTime: time.Now(),
		listNames: -1,
	}
}

// progressTotal returns the size of the run for the progress line and
// whether it is final. With a counted file it is the names of the file
// minus the ones left out so far, which becomes exact once the whole file
// is read; it used to be the names read so far (the producer is at most
// 4096 names ahead), so a list of 9,995 names showed "1290/5396". From a
// pipe only the names read so far are known.
func (s *Stats) progressTotal(checked int64) (total int64, known bool) {
	total = atomic.LoadInt64(&s.total)
	if n := atomic.LoadInt64(&s.listNames); n >= 0 {
		if est := n - atomic.LoadInt64(&s.dropped); est > total {
			total = est
		}
		known = true
	}
	if total < checked {
		total = checked
	}
	return total, known
}

// countListNames counts the names ScanNames reads from f and rewinds it,
// so the progress line can show the size of the run from the start. It
// keeps no names (about 250 MB/s). ok is false when f cannot be read
// twice.
func countListNames(f *os.File) (n int64, ok bool) {
	if err := checker.ScanNames(f, func(string) error { n++; return nil }); err != nil {
		return 0, false
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, false
	}
	return n, true
}

// atomicMax sets *p = max(*p, v) using a CAS loop.
func atomicMax(p *int64, v int64) {
	for {
		old := atomic.LoadInt64(p)
		if old >= v {
			return
		}
		if atomic.CompareAndSwapInt64(p, old, v) {
			return
		}
	}
}

// observe feeds the timing figures of the -debug summary and the largest
// marker offset. Cache answers and errors are left out: they would skew
// the figures of real requests.
func (s *Stats) observe(r checker.Result) {
	if r.Cached || r.Status == StatusError {
		return
	}
	switch r.Status {
	case StatusFree, StatusReserved:
		s.netRedirect.add(r.Net)
	default:
		s.netPage.add(r.Net)
	}
	s.wait.add(r.Wait)
	s.connWait.add(r.ConnWait)
	s.work.add(r.Work)
	k := min(max(r.Attempts, 1), len(s.tries))
	atomic.AddInt64(&s.tries[k-1], 1)
	if r.Offset > 0 {
		atomicMax(&s.statusOffsetMax, int64(r.Offset))
	}
}

// snapshot returns a compact single-line summary for the progress logger.
func (s *Stats) snapshot(e checker.Stats) string {
	elapsed := time.Since(s.startTime).Seconds()
	rps := 0.0
	if elapsed > 0 {
		rps = float64(e.Checked) / elapsed
	}
	total, known := s.progressTotal(e.Checked)
	size := strconv.FormatInt(total, 10)
	if !known {
		size += "+" // a pipe: more names may come
	}
	eta := ""
	if known && rps > 0 && total > e.Checked {
		eta = ", eta " + formatETA(time.Duration(float64(total-e.Checked)/rps*float64(time.Second)))
	}
	return fmt.Sprintf(
		"%d/%s (%.1f req/s%s) | free=%d reserved=%d auction=%d available=%d taken=%d sold=%d unknown=%d err=%d | new=%d reuse=%d | 429=%d retry=%d pause=%d",
		e.Checked, size, rps, eta,
		e.Free, e.Reserved, e.Auction, e.Available, e.Taken, e.Sold, e.Unknown, e.Errors,
		e.ConnNew, e.ConnReuse, e.RateLimit, e.Retries, e.Pauses,
	)
}

// formatETA rounds d for the progress line: 45s, 12m, 3h05m.
func formatETA(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

// finalSummary returns the multi-line summary printed at shutdown.
// Debug mode adds per-bucket latency stats and a -readlimit tuning hint.
func (s *Stats) finalSummary(e checker.Stats, readLimit int) string {
	elapsed := time.Since(s.startTime).Seconds()
	rps := 0.0
	if elapsed > 0 {
		rps = float64(e.Checked) / elapsed
	}
	line := fmt.Sprintf(
		"Checked: %d/%d | Free: %d | Reserved: %d | Auction: %d | Available: %d | Taken: %d | Sold: %d | Unknown: %d | Errors: %d | Elapsed: %.1fs (%.1f req/s)",
		e.Checked, atomic.LoadInt64(&s.total),
		e.Free, e.Reserved, e.Auction, e.Available, e.Taken, e.Sold, e.Unknown, e.Errors,
		elapsed, rps,
	)
	line += fmt.Sprintf("\n  Connections: new=%d, reused=%d | Wire: in=%s, out=%s | Rate-limits: %d | Retries: %d",
		e.ConnNew, e.ConnReuse, fmtBytes(e.BytesIn), fmtBytes(e.BytesOut), e.RateLimit, e.Retries)
	line += fmt.Sprintf("\n  Page cache: hits=%d, stale=%d, misses=%d", e.CacheHits, e.CacheStale, e.CacheMisses)
	if e.CacheErrors > 0 {
		line += fmt.Sprintf(" (%d of them broken entries; see -debug)", e.CacheErrors)
	}
	if e.Challenges > 0 {
		line += fmt.Sprintf("\n  Cloudflare challenges: %d", e.Challenges)
	}
	if logLevel >= LevelDebug {
		if s.netRedirect.count() > 0 {
			line += "\n  Network per name, redirects (free, reserved): " + s.netRedirect.summary()
		}
		if s.netPage.count() > 0 {
			line += "\n  Network per name, pages (the other verdicts): " + s.netPage.summary()
		}
		if s.wait.count() > 0 {
			line += "\n  Waiting per name (-rate starts, pauses, retry delays, probes, connections): " + s.wait.summary()
			line += "\n  Waiting for a free connection per name (-conns): " + s.connWait.summary()
			line += "\n  Work per name on this machine (parsing, page cache): " + s.work.summary()
			line += fmt.Sprintf("\n  Tries per name: 1: %d | 2: %d | 3: %d | 4+: %d",
				atomic.LoadInt64(&s.tries[0]), atomic.LoadInt64(&s.tries[1]),
				atomic.LoadInt64(&s.tries[2]), atomic.LoadInt64(&s.tries[3]))
		}
		if maxOff := atomic.LoadInt64(&s.statusOffsetMax); maxOff > 0 {
			line += fmt.Sprintf("\n  Status offset max: %d bytes (readLimit=%d, you can tighten it to %d via -readlimit)",
				maxOff, readLimit, maxOff+safetyMargin)
		}
		if e.Unknown > 0 {
			line += fmt.Sprintf("\n  WARN: unknown=%d — either readLimit is too small or the redirect target is new; re-run with -trace FILE: it keeps the pages of the unknown answers", e.Unknown)
		}
	}
	return line
}

// -----------------------------------------------------------------------------
// Input streaming
// -----------------------------------------------------------------------------

// produceUsernames streams names from src (file or stdin) into tasks. The
// list is split like in the web panel (checker.ScanNames): new lines,
// spaces, commas and semicolons separate names, and the part of a line
// after "|" is ignored, so a results file can be read back. When dedup is
// true, duplicates are skipped. When skip is non-nil, names in it are
// dropped (used for -resume). Invalid names are passed on: the engine
// reports them as "invalid" errors without a request. Memory grows only
// with the number of unique names when dedup is on (checker.NameSet, about
// 8 bytes per name of up to 12 characters), and stays flat otherwise.
//
// onSkip, when set, hears every name left out and why (the trace).
func produceUsernames(
	ctx context.Context,
	src io.Reader,
	tasks chan<- string,
	dedup bool,
	skip *checker.NameSet,
	stats *Stats,
	onSkip func(name, why string),
) (rejected int64, err error) {
	var seen checker.NameSet
	err = checker.ScanNames(src, func(name string) error {
		if dedup && !seen.Add(name) {
			atomic.AddInt64(&stats.dropped, 1)
			if onSkip != nil {
				onSkip(name, "duplicate")
			}
			return nil
		}
		if skip.Has(name) {
			rejected++
			atomic.AddInt64(&stats.dropped, 1)
			if onSkip != nil {
				onSkip(name, "has a verdict in the output (-resume)")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tasks <- name:
			atomic.AddInt64(&stats.total, 1)
			return nil
		}
	})
	return rejected, err
}

// -----------------------------------------------------------------------------
// Resume: load already-processed names from an existing output file
// -----------------------------------------------------------------------------

// loadResumeSet reads the existing output file and returns the set of
// usernames that already have a verdict there (lines with an error do not
// count). Returns nil, nil when the file does not exist (first run is
// allowed).
func loadResumeSet(path string) (*checker.NameSet, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), scannerBufMax)
	set := new(checker.NameSet)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "@") {
			continue
		}
		pipe := strings.Index(line, " | ")
		if pipe < 0 {
			continue
		}
		// An error is not a verdict (a network failure, a Cloudflare
		// challenge, or a check that older versions wrote as "error
		// (cancelled)" on Ctrl+C): -resume checks the name again. A name
		// with a verdict on any line is done.
		if strings.HasPrefix(strings.TrimSpace(line[pipe+3:]), string(StatusError)) {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(line[1:pipe]))
		if name != "" {
			set.Add(name)
		}
	}
	return set, scanner.Err()
}

// addCheckedList adds the names of a <output>.checked list (one per line)
// to set. A missing file is not an error.
func addCheckedList(path string, set *checker.NameSet) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), scannerBufMax)
	for scanner.Scan() {
		if name := strings.TrimSpace(scanner.Text()); name != "" {
			set.Add(name)
		}
	}
	return scanner.Err()
}

// -----------------------------------------------------------------------------
// Filter parsing
// -----------------------------------------------------------------------------

// parseFilter accepts a comma-separated list of statuses (e.g. "free,auction")
// and returns a set used by workers to decide which results to write to the
// output file. An empty string disables filtering.
func parseFilter(raw string) (map[Status]bool, error) {
	if raw == "" {
		return nil, nil
	}
	filter := make(map[Status]bool)
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(strings.ToLower(p))
		switch Status(p) {
		case StatusFree, StatusReserved, StatusAuction,
			StatusTaken, StatusSold, StatusAvailable,
			StatusUnknown, StatusError:
			filter[Status(p)] = true
		default:
			return nil, fmt.Errorf("unknown status: %q", p)
		}
	}
	return filter, nil
}

// -----------------------------------------------------------------------------
// Output: one goroutine writes the results file and stdout
// -----------------------------------------------------------------------------

// outputs are the writers of one run. checked is nil without -filter.
// st, when set, counts what they wrote for the trace.
type outputs struct {
	file    *bufio.Writer // the results file
	stdout  *bufio.Writer
	checked *bufio.Writer // <output>.checked: every name with a verdict
	st      *outStats
}

// outStats count what the writer wrote: lines and bytes per file, the
// flushes and their time, the failed flushes.
type outStats struct {
	fileLines, fileBytes, checkedLines, stdoutLines int64
	flushes, flushNs, flushMaxNs, flushErrors       int64
}

func (o *outStats) info() map[string]any {
	return map[string]any{
		"file_lines": atomic.LoadInt64(&o.fileLines), "file_bytes": atomic.LoadInt64(&o.fileBytes),
		"checked_lines": atomic.LoadInt64(&o.checkedLines), "stdout_lines": atomic.LoadInt64(&o.stdoutLines),
		"flushes": atomic.LoadInt64(&o.flushes), "flush_ms": float64(atomic.LoadInt64(&o.flushNs)) / 1e6,
		"flush_max_ms": float64(atomic.LoadInt64(&o.flushMaxNs)) / 1e6, "flush_errors": atomic.LoadInt64(&o.flushErrors),
	}
}

// flush flushes every writer and returns the first error of the files.
func (o outputs) flush() error {
	if o.st != nil {
		start := time.Now()
		defer func() {
			d := int64(time.Since(start))
			atomic.AddInt64(&o.st.flushes, 1)
			atomic.AddInt64(&o.st.flushNs, d)
			atomicMax(&o.st.flushMaxNs, d)
		}()
	}
	ferr := o.file.Flush()
	var cerr error
	if o.checked != nil {
		cerr = o.checked.Flush()
	}
	serr := o.stdout.Flush()
	switch {
	case ferr != nil:
		return fmt.Errorf("file flush: %w", ferr)
	case cerr != nil:
		return fmt.Errorf("checked-list flush: %w", cerr)
	case serr != nil:
		return fmt.Errorf("stdout flush: %w", serr)
	}
	return nil
}

// flushCounted flushes and counts a failure (the final flush returns it).
func (o outputs) flushCounted() {
	if err := o.flush(); err != nil && o.st != nil {
		atomic.AddInt64(&o.st.flushErrors, 1)
	}
}

// count notes a written line in the stats.
func (o outputs) count(field *int64, bytes *int64, n int) {
	if o.st == nil {
		return
	}
	atomic.AddInt64(field, 1)
	if bytes != nil {
		atomic.AddInt64(bytes, int64(n))
	}
}

// writeResults writes every result to the output file and stdout until
// results is closed. When no other result is waiting and the last flush
// was at least flushInterval (100 ms) ago it flushes at once, so at
// network speed a line shows up when its check ends; otherwise the ticker
// flushes within 100 ms, so a fast stream (answers from the page cache)
// still makes at most ten flushes a second instead of one write per line.
// It used to flush every 2 s only, and the screen showed results in
// bursts. With -filter it also lists every name that got a verdict in
// <output>.checked, so -resume can skip the names the filter left out of
// the results file. It does not react to Ctrl+C on purpose: every result
// the engine counted must land on disk. A failed final flush (disk full,
// I/O error) is returned.
func writeResults(results <-chan checker.Result, out outputs, filter map[Status]bool, quiet bool, stats *Stats) error {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	const resetColor = "\033[0m"
	var lastFlush time.Time
	for {
		select {
		case <-ticker.C:
			out.flushCounted()
			lastFlush = time.Now()
		case r, ok := <-results:
			if !ok {
				err := out.flush()
				if err != nil && out.st != nil {
					atomic.AddInt64(&out.st.flushErrors, 1)
				}
				return err
			}
			stats.observe(r)
			fileLine, stdoutLine, color := formatResult(r, filter, quiet)
			if fileLine != "" {
				_, _ = out.file.WriteString(fileLine)
				_ = out.file.WriteByte('\n')
				if out.st != nil {
					out.count(&out.st.fileLines, &out.st.fileBytes, len(fileLine)+1)
				}
			}
			if out.checked != nil && r.Status != StatusError {
				_, _ = out.checked.WriteString(r.Username)
				_ = out.checked.WriteByte('\n')
				if out.st != nil {
					out.count(&out.st.checkedLines, nil, 0)
				}
			}
			if stdoutLine != "" && out.st != nil {
				out.count(&out.st.stdoutLines, nil, 0)
			}
			if stdoutLine != "" {
				if color != "" {
					_, _ = out.stdout.WriteString(color + stdoutLine + resetColor + "\n")
				} else {
					_, _ = out.stdout.WriteString(stdoutLine + "\n")
				}
			}
			if len(results) == 0 && time.Since(lastFlush) >= flushInterval {
				out.flushCounted() // nothing else waiting: show it now
				lastFlush = time.Now()
			}
		}
	}
}

// formatResult returns the results-file line ("" when -filter leaves the
// status out) and the stdout line with its ANSI color: green for free,
// magenta for reserved, yellow for auction, red for errors, gray for the
// rest at -debug. -quiet keeps only free and errors on stdout.
func formatResult(r checker.Result, filter map[Status]bool, quiet bool) (fileLine, stdoutLine, color string) {
	fileLine = fmt.Sprintf("@%s | %s", r.Username, r.Status)
	if r.Error != "" {
		fileLine += fmt.Sprintf(" (%s)", r.Error)
	}
	if filter != nil && !filter[r.Status] {
		fileLine = ""
	}
	plain := fmt.Sprintf("@%s | %s", r.Username, r.Status)
	switch r.Status {
	case StatusFree:
		return fileLine, plain, "\033[92m" // bright green
	case StatusReserved:
		if !quiet {
			return fileLine, plain, "\033[35m" // magenta
		}
	case StatusAuction:
		if !quiet {
			return fileLine, plain, "\033[93m" // yellow
		}
	case StatusError:
		return fileLine, fmt.Sprintf("@%s | %s (%s)", r.Username, r.Status, r.Error), "\033[91m" // red
	default:
		if logLevel >= LevelDebug && !quiet {
			return fileLine, plain, "\033[90m" // gray
		}
	}
	return fileLine, "", ""
}

// -----------------------------------------------------------------------------
// Progress reporter
// -----------------------------------------------------------------------------

// runProgress prints a one-line snapshot every progressInterval, with an
// "instant" requests-per-second value to help spot throughput stalls.
func runProgress(ctx context.Context, stats *Stats, chk *checker.Checker, q queues, trace *tracelog.Writer, ost *outStats) {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	var lastChecked int64
	lastTime := time.Now()
	traceStopWarned := false
	rt := rtstats.NewReader()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e := chk.Snapshot()
			now := time.Now()
			instRPS := 0.0
			if dt := now.Sub(lastTime).Seconds(); dt > 0 {
				instRPS = float64(e.Checked-lastChecked) / dt
			}
			lastChecked = e.Checked
			lastTime = now
			line := fmt.Sprintf("[progress] %s | inst=%.0f r/s", stats.snapshot(e), instRPS)
			var rs rtstats.Sample
			if logLevel >= LevelDebug || trace != nil {
				rs = rt.Read()
			}
			if logLevel >= LevelDebug {
				line += " | " + pipelineLine(e, q, chk.Config().Workers) + " | " + runtimeLine(rs)
			}
			log.Print(line)
			if trace != nil {
				ts := trace.Stats()
				info := pipelineInfo(e, q, chk.Config().Workers, instRPS)
				info["trace"] = ts
				info["runtime"] = rs
				if ost != nil {
					info["output"] = ost.info()
				}
				trace.Write(checker.TraceEvent{Time: now, Kind: "pipeline", Info: info})
				if ts.Stopped != "" && !traceStopWarned {
					traceStopWarned = true
					log.Printf("[WARN] the trace stopped: %s; the run goes on", ts.Stopped)
				}
			}
		}
	}
}

// queues are the two queues of a run: names waiting for a worker, results
// waiting for the writer.
type queues struct {
	in  func() int
	out func() int
}

// pipelineLine is the state of the run in the -debug progress line: the
// queues, then the workers by what they do.
func pipelineLine(e checker.Stats, q queues, workers int) string {
	idle := int64(workers) - e.Busy
	return fmt.Sprintf("queue in=%d out=%d | workers net=%d conn=%d gate=%d pause=%d rate=%d retry=%d probe=%d idle=%d",
		q.in(), q.out(), max(e.InFlight-e.WaitConn, 0), e.WaitConn, e.WaitGate, e.WaitPause, e.WaitRate,
		e.WaitRetry, e.WaitProbe, max(idle, 0))
}

// runtimeLine is the state of the process in the -debug progress line:
// goroutines, live heap, resident memory, how long goroutines waited for a
// CPU (p99 in the last second) and the GC cycles so far.
func runtimeLine(s rtstats.Sample) string {
	line := fmt.Sprintf("go=%d heap=%s", s.Goroutines, fmtBytes(int64(s.HeapBytes)))
	if s.RSSBytes > 0 {
		line += " rss=" + fmtBytes(s.RSSBytes)
	}
	return line + fmt.Sprintf(" sched_p99=%.1fms gc=%d", s.SchedLatP99Ms, s.GCCycles)
}

// pipelineInfo is the same for the "pipeline" event of the trace.
func pipelineInfo(e checker.Stats, q queues, workers int, instRPS float64) map[string]any {
	return map[string]any{
		"queue_in": q.in(), "queue_out": q.out(), "checked": e.Checked, "inst_rps": instRPS,
		"workers": workers, "busy": e.Busy, "in_flight": e.InFlight, "wait_gate": e.WaitGate,
		"wait_pause": e.WaitPause, "wait_rate": e.WaitRate, "wait_retry": e.WaitRetry, "wait_probe": e.WaitProbe,
		"wait_conn": e.WaitConn, "conn_open": e.ConnOpen, "bytes_in": e.BytesIn, "bytes_out": e.BytesOut,
	}
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

// Main wires together the input producer, the engine, the output writer,
// the progress reporter and the signal handler, then waits for everything
// to drain before printing the final summary. It returns the exit code.
func Main(args []string) int {
	fs := flag.NewFlagSet(ProgName, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	// Disable the standard log prefix; we use our own tags.
	log.SetFlags(log.Ltime)

	var (
		inputPath  = fs.String("input", "", "Path to the username list (use \"-\" for stdin)")
		baseURL    = fs.String("base-url", "", "Override Fragment base URL (testing/mirror)")
		outputPath = fs.String("output", "", "Path to the results file (required)")
		workers    = fs.Int("workers", defaultWorkers, "Number of parallel workers")
		maxConns   = fs.Int("conns", defaultMaxConnsPerHost, "Max TCP connections to fragment.com (HTTP/2 multiplex)")
		rateRPS    = fs.Int("rate", 0, "Requests per second to Fragment, retries included; answers from the cache do not count (0 = unlimited)")
		retries    = fs.Int("retries", 2, "Max manual retries per request (no exponential backoff)")
		retryDelay = fs.Duration("retry-delay", 500*time.Millisecond, "Delay between retries")
		rlWaitMax  = fs.Duration("rl-wait-max", 30*time.Second, "Maximum wait honoured for HTTP 429 Retry-After")
		throttleMx = fs.Duration("throttle-max", 30*time.Minute, "Stop when Fragment keeps throttling this long; names without a verdict stay for -resume")
		timeout    = fs.Duration("timeout", defaultTimeout, "Total HTTP request timeout")
		readLimitF = fs.Int("readlimit", checker.DefaultReadLimit, "Body bytes to read for status parsing (the -debug summary suggests an optimal value)")

		cacheDir     = fs.String("cache-dir", ".fragment_cache", "Directory for the on-disk page cache")
		cacheTTLFlag = fs.Duration("cache-ttl", defaultCacheTTL, "TTL for volatile pages (available/auction)")
		cacheSoldTTL = fs.Duration("cache-sold-ttl", defaultCacheSoldTTL, "TTL for sold/bought pages (negative = never expire)")
		noCache      = fs.Bool("no-cache", false, "Disable the on-disk page cache")

		filterRaw  = fs.String("filter", "", "Comma-separated status filter (free,reserved,auction,taken,sold,available,unknown,error)")
		appendFlag = fs.Bool("append", false, "Open the output file in append mode (do not truncate)")
		resume     = fs.Bool("resume", false, "Skip usernames already present in the output file")
		noDedup    = fs.Bool("no-dedup", false, "Do not deduplicate the input (saves RAM on huge files)")
		quiet      = fs.Bool("quiet", false, "Stdout: print only free results and errors")
		insecure   = fs.Bool("insecure", false, "Skip TLS certificate verification (debug only)")

		debugFlag = fs.Bool("debug", false, "Print one line per HTTP try with its timings, the verdicts and cache use, and timing figures at the end")
		traceOut  = fs.String("trace", "", "Write the structured trace of the run to `FILE` as JSON Lines (gzip when it ends in .gz): every request, cache lookup, name and engine event; appended with -resume/-append. Independent of -debug")
		traceOld  = fs.String("trace-file", "", "Deprecated spelling of -trace FILE")
		traceBody = fs.String("trace-bodies", checker.BodiesAnomalies, "Pages the -trace keeps, whole and decoded (up to 1 MB each): anomalies (unknown answers, challenges, unexpected codes, errors), all, or none")
		pprofAddr = fs.String("pprof", "", "Serve pprof on `ADDR` while the run goes on, e.g. 127.0.0.1:6060 (/debug/pprof/: CPU, heap, goroutines, block and mutex contention, execution trace)")
		cpuProf   = fs.String("cpuprofile", "", "Write a CPU profile of the run to `FILE` (go tool pprof FILE)")
		memProf   = fs.String("memprofile", "", "Write the heap profile at the end of the run to `FILE` (live and allocated memory: go tool pprof [-sample_index=alloc_space] FILE)")
		keyLog    = fs.String("tls-keylog", "", "Append the TLS keys of every connection to `FILE` (NSS key log): with a capture of the traffic (tcpdump), Wireshark decrypts every byte, HTTP/2 frames included. Keep the file private")
		execTrace = fs.String("exectrace", "", "Write the execution trace of the run to `FILE` (go tool trace FILE): every goroutine switch, network wait, syscall and GC, with a task per name; it stops at -trace-max or when the disk runs low")
		showVer   = fs.Bool("version", false, "Print version and exit")
	)
	traceMax := byteSize(1 << 30)
	fs.Var(&traceMax, "trace-max", "Largest size of the -trace files on disk, e.g. 512M or 2G (0 = no limit). The trace keeps the end of the run: FILE holds the newest part, FILE with .1 before the extension the part before it")
	fs.Usage = func() { printUsage(fs) }
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if *showVer {
		fmt.Println(ProgName, Version)
		return 0
	}

	logLevel = LevelNormal
	if *debugFlag {
		logLevel = LevelDebug
	}
	tracePath := *traceOut
	if *traceOld != "" {
		if tracePath == "" {
			tracePath = *traceOld
		}
		log.Printf("[WARN] -trace-file is an old spelling; use -trace %s", *traceOld)
	}
	if err := checkTracePath(tracePath); err != nil {
		log.Printf("[ERROR] %v", err)
		return 2
	}
	switch *traceBody {
	case checker.BodiesAnomalies, checker.BodiesAll, checker.BodiesNone:
	default:
		log.Printf("[ERROR] -trace-bodies must be anomalies, all or none, got %q", *traceBody)
		return 2
	}

	if *inputPath == "" || *outputPath == "" {
		printUsage(fs)
		return 2
	}
	for _, f := range []struct{ flag, path string }{{"-cpuprofile", *cpuProf}, {"-memprofile", *memProf}, {"-exectrace", *execTrace},
		{"-tls-keylog", *keyLog}} {
		if f.path == "" {
			continue
		}
		for _, other := range []string{*inputPath, *outputPath, tracePath} {
			if other != "" && other != "-" && sameFile(f.path, other) {
				log.Printf("[ERROR] %s %s is a file of the run; write it to another file", f.flag, f.path)
				return 2
			}
		}
	}
	if *baseURL != "" {
		checker.FragmentURLTemplate = strings.TrimRight(*baseURL, "/") + "/username/%s"
	}

	// --- Filter ---
	// Parsed before the output file is opened: opening truncates it, and a
	// typo in -filter used to wipe the previous results before failing.
	filter, err := parseFilter(*filterRaw)
	if err != nil {
		log.Printf("[ERROR] -filter: %v", err)
		return 2
	}

	// --- Open input ---
	listNames := int64(-1) // names in the input file, counted up front (-1: a pipe)
	var inReader io.Reader
	var inInfo os.FileInfo
	if *inputPath == "-" {
		inReader = os.Stdin
		inInfo, _ = os.Stdin.Stat()
	} else {
		f, err := os.Open(*inputPath)
		if err != nil {
			log.Fatalf("Cannot open %s: %v", *inputPath, err)
		}
		defer f.Close()
		inReader = f
		inInfo, _ = f.Stat()
		if inInfo != nil && inInfo.Mode().IsRegular() {
			if n, ok := countListNames(f); ok {
				listNames = n
			}
		}
	}
	// Opening the output truncates it. When it is the input file (also via
	// another path, a symlink or "-input - < file"), the list was gone before
	// the first name was read: 0 names checked, exit code 0.
	if inInfo != nil && inInfo.Mode().IsRegular() {
		if outInfo, err := os.Stat(*outputPath); err == nil && os.SameFile(inInfo, outInfo) {
			log.Printf("[ERROR] -input and -output are the same file (%s); write the results to another file", *outputPath)
			return 2
		}
	}

	// --- Resume set ---
	// With -filter the results file lacks the names the filter left out;
	// <output>.checked lists every name that got a verdict.
	checkedPath := *outputPath + ".checked"
	if err := traceConflict(tracePath, inInfo, *inputPath, *outputPath, checkedPath); err != nil {
		log.Printf("[ERROR] %v", err)
		return 2
	}
	var skip *checker.NameSet
	if *resume {
		s, err := loadResumeSet(*outputPath)
		if err != nil {
			log.Fatalf("Cannot read resume set from %s: %v", *outputPath, err)
		}
		if s == nil {
			s = new(checker.NameSet)
		}
		if err := addCheckedList(checkedPath, s); err != nil {
			log.Fatalf("Cannot read %s: %v", checkedPath, err)
		}
		skip = s
		log.Printf("[resume] loaded %d already-processed names", skip.Len())
	}

	// --- Open output ---
	flags := os.O_CREATE | os.O_WRONLY
	if *appendFlag || *resume {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	outFile, err := os.OpenFile(*outputPath, flags, 0o644)
	if err != nil {
		log.Fatalf("Cannot open %s: %v", *outputPath, err)
	}
	out := outputs{
		file:   bufio.NewWriterSize(outFile, 64*1024),
		stdout: bufio.NewWriterSize(os.Stdout, 64*1024),
	}
	var checkedFile *os.File
	if filter != nil {
		checkedFile, err = os.OpenFile(checkedPath, flags, 0o644)
		if err != nil {
			log.Fatalf("Cannot open %s: %v", checkedPath, err)
		}
		out.checked = bufio.NewWriterSize(checkedFile, 64*1024)
	}

	// --- Engine ---
	// The same checker as the web panel: retries, the 302 / probe, the
	// shared cool-down, 429 handling, the page cache and the parser.
	cfg := checker.DefaultConfig()
	cfg.Workers = *workers
	cfg.MaxConns = *maxConns
	cfg.RateRPS = *rateRPS
	cfg.Retries = *retries
	cfg.RetryDelay = *retryDelay
	cfg.RLWaitMax = *rlWaitMax
	cfg.ThrottleMax = *throttleMx
	cfg.Timeout = *timeout
	cfg.ReadLimit = *readLimitF
	cfg.InsecureTLS = *insecure
	cfg.TraceBodies = *traceBody
	if *keyLog != "" {
		f, err := os.OpenFile(*keyLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			log.Printf("[ERROR] -tls-keylog: %v", err)
			return 2
		}
		defer f.Close()
		cfg.KeyLog = f
		log.Printf("[WARN] -tls-keylog: the TLS keys of every connection go to %s; with a capture of the traffic "+
			"they decrypt all of it. Keep the file private and delete it after use", *keyLog)
	}
	if !*noCache {
		cfg.CacheDir = *cacheDir
		cfg.CacheTTL = *cacheTTLFlag
		cfg.CacheSoldTTL = *cacheSoldTTL
	}
	chk := checker.New(cfg) // clamps out-of-range values (negative retries, huge -readlimit, ...)
	defer chk.Close()
	logger := checker.Logger{Warnf: warnf}
	if logLevel >= LevelDebug {
		logger.Debugf = debugf
	}
	var trace *tracelog.Writer
	if tracePath != "" {
		trace, err = tracelog.Open(tracePath, tracelog.Options{
			Append: *appendFlag || *resume, MaxBytes: int64(traceMax), MinFree: traceMinFree,
		})
		if err != nil {
			log.Printf("[ERROR] -trace: %v", err)
			return 2
		}
		logger.Event = trace.Write
		out.st = new(outStats)
	}
	chk.SetLogger(logger)
	eff := chk.Config()
	if trace != nil {
		trace.Write(checker.TraceEvent{Time: time.Now(), Kind: "run_start", Info: map[string]any{
			"program": ProgName, "version": Version, "input": *inputPath, "output": *outputPath,
			"list_names": listNames, "resume": *resume, "dedup": !*noDedup, "filter": *filterRaw,
			"workers": eff.Workers, "max_conns": eff.MaxConns, "rate_rps": eff.RateRPS, "retries": eff.Retries,
			"retry_delay_ms": eff.RetryDelay.Milliseconds(), "timeout_ms": eff.Timeout.Milliseconds(),
			"read_limit": eff.ReadLimit, "rl_wait_max_ms": eff.RLWaitMax.Milliseconds(),
			"block_cooldown_ms": eff.BlockCooldown.Milliseconds(), "throttle_max_ms": eff.ThrottleMax.Milliseconds(),
			"cache_dir": eff.CacheDir, "log": logLevel.String(), "trace_bodies": eff.TraceBodies,
			"trace_max": int64(traceMax), "env": tracelog.Environment(), "config": eff, "args": args,
		}})
	}

	// --- Log level banner ---
	levelName := logLevel.String()
	filterStr := *filterRaw
	if filterStr == "" {
		filterStr = "all"
	}
	log.Printf("%s v%s | workers=%d conns=%d retries=%d retry-delay=%v rate=%d timeout=%v filter=%s log=%s",
		ProgName, Version, eff.Workers, eff.MaxConns, eff.Retries, eff.RetryDelay, eff.RateRPS, eff.Timeout, filterStr, levelName)

	// --- Profiling: pprof, the CPU profile, the execution trace ---
	// Started before the run so they cover all of it.
	stopProfiling, err := startProfiling(*pprofAddr, *cpuProf, *execTrace, int64(traceMax))
	if err != nil {
		log.Printf("[ERROR] %v", err)
		return 2
	}

	// --- Context and signals ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stopAt atomic.Int64 // when a signal asked to stop (UnixNano), for the trace
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case sig := <-sigCh:
			// A second Ctrl+C quits at once (default signal handling); it
			// used to be swallowed, so a stuck shutdown could not be ended.
			signal.Reset(syscall.SIGINT, syscall.SIGTERM)
			log.Printf("[INFO] received signal %s, shutting down gracefully… (press Ctrl+C again to quit at once)", sig)
			if trace != nil {
				trace.Write(checker.TraceEvent{Kind: "stop", Reason: "signal " + sig.String()})
			}
			stopAt.Store(time.Now().UnixNano())
			cancel()
		case <-ctx.Done():
		}
	}()

	stats := newStats()
	stats.listNames = listNames
	tasks := make(chan string, tasksChanCap)
	results := make(chan checker.Result, resultsChanCap)

	// --- Output writer (lives until close(results), independent of ctx) ---
	writeDone := make(chan error, 1)
	go rpprof.Do(ctx, rpprof.Labels("component", "writer"), func(context.Context) {
		writeDone <- writeResults(results, out, filter, *quiet, stats)
	})

	// --- Progress reporter ---
	progCtx, progCancel := context.WithCancel(ctx)
	progDone := make(chan struct{})
	go func() {
		defer close(progDone)
		rpprof.SetGoroutineLabels(rpprof.WithLabels(progCtx, rpprof.Labels("component", "progress")))
		runProgress(progCtx, stats, chk, queues{in: func() int { return len(tasks) }, out: func() int { return len(results) }}, trace, out.st)
	}()

	// --- Checks: eff.Workers goroutines inside the engine ---
	runDone := make(chan struct{})
	go func() {
		chk.Run(ctx, tasks, results)
		close(runDone)
	}()

	// --- Input producer ---
	producerDone := make(chan error, 1)
	inCount := &byteCounter{r: inReader}
	var onSkip func(name, why string)
	if trace != nil {
		skipped := 0
		onSkip = func(name, why string) {
			if skipped++; skipped <= maxSkipEvents {
				trace.Write(checker.TraceEvent{Kind: "input_skip", Name: name, Reason: why})
			}
		}
	}
	go func() {
		rpprof.SetGoroutineLabels(rpprof.WithLabels(ctx, rpprof.Labels("component", "producer")))
		readStart := time.Now()
		rejected, err := produceUsernames(ctx, inCount, tasks, !*noDedup, skip, stats, onSkip)
		if rejected > 0 {
			log.Printf("[resume] skipped %d already-processed names", rejected)
		}
		if trace != nil {
			dropped := atomic.LoadInt64(&stats.dropped)
			trace.Write(checker.TraceEvent{Kind: "input", Info: map[string]any{
				"path": *inputPath, "bytes": inCount.n.Load(), "read_ms": time.Since(readStart).Milliseconds(),
				"queued": atomic.LoadInt64(&stats.total), "duplicates": dropped - rejected, "resume_skipped": rejected,
				"complete": err == nil, "skip_events": min(dropped, maxSkipEvents),
			}})
		}
		close(tasks)
		producerDone <- err
	}()

	// --- Wait for the checks ---
	// Run returns when the list is done, on Ctrl+C, or when it gives up
	// because Fragment answers every request with a Cloudflare challenge.
	// In the last case the producer still waits to hand over a name.
	<-runDone
	runDoneAt := time.Now()
	blocked := chk.Blocked()
	if blocked {
		cancel()
	}

	// --- Wait for producer ---
	// After Ctrl+C (or a block) nothing the producer reads would be checked,
	// so do not wait for it: with -input - on an open pipe or terminal it
	// sits in a read of stdin that only new input or EOF ends, and Main
	// used to hang there after "shutting down gracefully".
	select {
	case err := <-producerDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[WARN] producer error: %v", err)
		}
	case <-ctx.Done():
	}

	// --- Stop progress reporter ---
	// Waited for: a progress line or a pipeline event after this point
	// would land after the summary, or after run_end in the trace.
	progCancel()
	<-progDone

	// --- Close the results and wait for the final flush ---
	// A failed write (disk full, I/O error) means results.txt is missing
	// results: report it in the exit code, not only in the log.
	exitCode := 0
	close(results)
	if err := <-writeDone; err != nil {
		log.Printf("[ERROR] writing results: %v", err)
		exitCode = 1
	}
	writeDoneAt := time.Now()

	// --- Close output files ---
	if err := outFile.Close(); err != nil {
		log.Printf("[ERROR] closing output: %v", err)
		exitCode = 1
	}
	if checkedFile != nil {
		if err := checkedFile.Close(); err != nil {
			log.Printf("[ERROR] closing %s: %v", checkedPath, err)
			exitCode = 1
		}
	}

	// --- Profiles ---
	stopProfiling()
	if *memProf != "" {
		if err := profiling.WriteHeap(*memProf); err != nil {
			log.Printf("[ERROR] -memprofile: %v", err)
		} else {
			log.Printf("[INFO] heap profile in %s (go tool pprof %s; -sample_index=alloc_space for all allocations)", *memProf, *memProf)
		}
	}

	// --- Final summary ---
	final := chk.Snapshot()
	log.Println(stats.finalSummary(final, eff.ReadLimit))
	switch {
	case blocked && chk.BlockedBy() == checker.BlockedByThrottle:
		log.Printf("[ERROR] Fragment kept throttling this IP for %v (every request and the probe redirected to \"/\", "+
			"or HTTP 429), so the run stopped. Names without a verdict are checked again by a later run with -resume; "+
			"a lower -rate makes throttling less likely.", eff.ThrottleMax)
		if exitCode == 0 {
			exitCode = 3
		}
	case blocked:
		log.Printf("[ERROR] Fragment answered %d requests in a row with a Cloudflare challenge "+
			"(HTTP 403, cf-mitigated: challenge), so it blocks this IP and the run stopped. "+
			"Names without a verdict (errors and the rest of the list) are checked again by a later run with -resume.",
			checker.ChallengeLimit)
		if exitCode == 0 {
			exitCode = 3
		}
	case final.Checked > 0 && final.Errors == final.Checked:
		log.Printf("[ERROR] every checked name ended with an error")
		if exitCode == 0 {
			exitCode = 1
		}
	}
	if trace != nil {
		info := tracelog.StatsInfo(final)
		info["elapsed_ms"] = time.Since(stats.startTime).Milliseconds()
		info["exit_code"] = exitCode
		if b := tracelog.BlockedName(chk.BlockedBy()); b != "" {
			info["blocked"] = b
		}
		info["trace"] = trace.Stats()
		info["output"] = out.st.info()
		info["runtime"] = rtstats.NewReader().Read()
		// How long the end took: the checks after the stop, then writing
		// the last results out.
		shutdown := map[string]any{"results_ms": writeDoneAt.Sub(runDoneAt).Milliseconds()}
		if at := stopAt.Load(); at != 0 {
			shutdown["checks_ms"] = runDoneAt.Sub(time.Unix(0, at)).Milliseconds()
		}
		info["shutdown"] = shutdown
		trace.Write(checker.TraceEvent{Time: time.Now(), Kind: "run_end", Info: info})
		st, err := trace.Close()
		switch {
		case err != nil:
			log.Printf("[ERROR] -trace %s: %v (the trace is incomplete)", tracePath, err)
		case st.Rotations > 0:
			log.Printf("[INFO] trace: %d events, %s in %s and %s (the trace keeps the last %s of the run; the start was dropped)",
				st.Events, fmtBytes(st.Bytes), tracelog.OlderPath(tracePath), tracePath, traceMax.String())
		default:
			log.Printf("[INFO] trace: %d events, %s in %s", st.Events, fmtBytes(st.Bytes), tracePath)
		}
		if st.Stopped != "" {
			log.Printf("[WARN] the trace stopped before the end of the run: %s", st.Stopped)
		}
	}
	return exitCode
}

// byteSize is a flag value in bytes: "0", "512K", "100M", "1G" (base 1024;
// a trailing "B" is allowed).
type byteSize int64

func (b *byteSize) String() string {
	n := int64(*b)
	switch {
	case n == 0:
		return "0"
	case n%(1<<30) == 0:
		return strconv.FormatInt(n>>30, 10) + "G"
	case n%(1<<20) == 0:
		return strconv.FormatInt(n>>20, 10) + "M"
	case n%(1<<10) == 0:
		return strconv.FormatInt(n>>10, 10) + "K"
	}
	return strconv.FormatInt(n, 10)
}

func (b *byteSize) Set(s string) error {
	v := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(s)), "B")
	shift := 0
	switch {
	case strings.HasSuffix(v, "K"):
		shift = 10
	case strings.HasSuffix(v, "M"):
		shift = 20
	case strings.HasSuffix(v, "G"):
		shift = 30
	}
	if shift > 0 {
		v = v[:len(v)-1]
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 || n > (1<<62)>>shift {
		return fmt.Errorf("want a size such as 512M or 2G (0 = no limit), got %q", s)
	}
	*b = byteSize(n << shift)
	return nil
}

// fmtBytes prints a size for the log: 812 B, 3.4 KB, 12.0 MB, 1.2 GB.
func fmtBytes(n int64) string {
	switch {
	case n < 1<<10:
		return fmt.Sprintf("%d B", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	case n < 1<<30:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	}
	return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
}

// startProfiling starts what the profiling flags ask for and returns the
// function that ends it and reports the files. pprof turns on the block
// and mutex profiles too.
func startProfiling(pprofAddr, cpuPath, execPath string, execMax int64) (stop func(), err error) {
	var stops []func()
	stop = func() {
		for i := len(stops) - 1; i >= 0; i-- {
			stops[i]()
		}
	}
	if pprofAddr != "" {
		if !profiling.IsLoopback(pprofAddr) {
			log.Printf("[WARN] -pprof %s can be reached from other machines: pprof shows the command line, "+
				"the stack of every goroutine and the memory profile", pprofAddr)
		}
		profiling.EnableContention()
		addr, stopServer, err := profiling.Serve(pprofAddr)
		if err != nil {
			stop()
			return nil, fmt.Errorf("-pprof: %w", err)
		}
		log.Printf("[INFO] pprof on http://%s/debug/pprof/ (block and mutex profiles on)", addr)
		stops = append(stops, stopServer)
	}
	if cpuPath != "" {
		stopCPU, err := profiling.StartCPU(cpuPath)
		if err != nil {
			stop()
			return nil, fmt.Errorf("-cpuprofile: %w", err)
		}
		stops = append(stops, func() {
			if err := stopCPU(); err != nil {
				log.Printf("[ERROR] -cpuprofile: %v", err)
				return
			}
			log.Printf("[INFO] CPU profile in %s (go tool pprof %s)", cpuPath, cpuPath)
		})
	}
	if execPath != "" {
		et, err := profiling.StartExecTrace(execPath, execMax, traceMinFree)
		if err != nil {
			stop()
			return nil, fmt.Errorf("-exectrace: %w", err)
		}
		stops = append(stops, func() {
			n, early, err := et.Stop()
			switch {
			case err != nil:
				log.Printf("[ERROR] -exectrace: %v", err)
			case early != "":
				log.Printf("[WARN] the execution trace %s stopped before the end of the run: it %s (%s)", execPath, early, fmtBytes(n))
			default:
				log.Printf("[INFO] execution trace: %s in %s (go tool trace %s)", fmtBytes(n), execPath, execPath)
			}
		})
	}
	return stop, nil
}

// maxSkipEvents caps the input_skip events of a run (a list can hold
// millions of duplicates); the input event counts them all.
const maxSkipEvents = 1000

// byteCounter counts the bytes read through it.
type byteCounter struct {
	r io.Reader
	n atomic.Int64
}

func (b *byteCounter) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n.Add(int64(n))
	return n, err
}

// checkTracePath catches the old boolean -trace. It takes a file now, so
// "-trace -workers 200" reads "-workers" as the file name (and the rest of
// the command line goes wrong), and "-trace=true" reads "true".
func checkTracePath(p string) error {
	if strings.HasPrefix(p, "-") || p == "true" || p == "false" {
		return fmt.Errorf("-trace takes the path of the trace file (for example -trace run.jsonl.gz), got %q; "+
			"for one line per request on the console use -debug", p)
	}
	return nil
}

// traceConflict refuses a trace path that is one of the run's other files:
// the trace is opened with O_TRUNC, so "-trace results.txt" would wipe the
// results (and the input with "-trace names.txt"). The input is compared
// by its open file too, so "-input - < names.txt" is caught.
func traceConflict(tracePath string, inInfo os.FileInfo, inputPath, outputPath, checkedPath string) error {
	if tracePath == "" {
		return nil
	}
	for _, f := range []struct{ flag, path string }{
		{"-input", inputPath}, {"-output", outputPath}, {"the checked list of -filter", checkedPath},
	} {
		if f.path != "-" && sameFile(tracePath, f.path) {
			return fmt.Errorf("-trace %s is the same file as %s; write the trace to another file", tracePath, f.flag)
		}
	}
	if inInfo != nil && inInfo.Mode().IsRegular() {
		if ti, err := os.Stat(tracePath); err == nil && os.SameFile(inInfo, ti) {
			return fmt.Errorf("-trace %s is the input file; write the trace to another file", tracePath)
		}
	}
	return nil
}

// sameFile reports whether a and b name one file: the same absolute path,
// or, when both exist, the same file through another path or a link.
func sameFile(a, b string) bool {
	if aa, err := filepath.Abs(a); err == nil {
		if bb, err := filepath.Abs(b); err == nil && aa == bb {
			return true
		}
	}
	ai, err := os.Stat(a)
	if err != nil {
		return false
	}
	bi, err := os.Stat(b)
	return err == nil && os.SameFile(ai, bi)
}

// traceMinFree stops the trace when the disk runs low; a variable so tests
// do not depend on the free space of the machine they run on.
var traceMinFree int64 = tracelog.DefaultMinFree

// hiddenFlags work but are left out of -h: old spellings kept so that
// scripts do not break.
var hiddenFlags = map[string]bool{"trace-file": true}

// printUsage is installed as flag.Usage and is also called manually when
// required flags are missing.
func printUsage(fs *flag.FlagSet) {
	fmt.Fprintf(os.Stderr, "%s v%s — production-ready Fragment.com username checker\n\n", ProgName, Version)
	fmt.Fprintln(os.Stderr, "Build:")
	fmt.Fprintln(os.Stderr, "  go build -o frgmnt ./cmd/frgmnt    # unified binary")
	fmt.Fprintln(os.Stderr, "  go build -o fragment_checker ./... # standalone")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Examples:")
	fmt.Fprintf(os.Stderr, "  %s -input names.txt -output results.txt\n", ProgName)
	fmt.Fprintf(os.Stderr, "  %s -input names.txt -output results.txt -workers 200 -conns 8\n", ProgName)
	fmt.Fprintf(os.Stderr, "  %s -input names.txt -output results.txt -resume -append\n", ProgName)
	fmt.Fprintf(os.Stderr, "  cat names.txt | %s -input - -output results.txt -no-dedup\n", ProgName)
	fmt.Fprintf(os.Stderr, "  %s -input names.txt -output results.txt -debug -workers 200 -rate 1000\n", ProgName)
	fmt.Fprintf(os.Stderr, "  %s -input names.txt -output results.txt -trace run.jsonl.gz\n", ProgName)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Statuses describe Fragment, not Telegram: \"free\" means Fragment has no page")
	fmt.Fprintln(os.Stderr, "for the name (not a collectible, not for sale); a regular account may still hold it.")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Flags:")
	shown := flag.NewFlagSet(fs.Name(), flag.ContinueOnError)
	shown.SetOutput(os.Stderr)
	fs.VisitAll(func(f *flag.Flag) {
		if hiddenFlags[f.Name] {
			return
		}
		shown.Var(f.Value, f.Name, f.Usage)
		// The default as declared, not the value parsed from the command
		// line (printUsage also runs after Parse).
		shown.Lookup(f.Name).DefValue = f.DefValue
	})
	shown.PrintDefaults()
}
