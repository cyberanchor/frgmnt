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
//	frgmnt check -input names.txt -output results.txt -trace
//
// Log levels:
//
//	(default) — results + progress line once per second
//	-debug    — HTTP codes, body sizes, parser verdicts, cache use
//	-trace    — everything from -debug plus DNS / TCP / TLS / headers / body preview
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
	"math"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
)

// Version is the human-readable release tag printed on startup.
const Version = "3.1"

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
	flushInterval    = 2 * time.Second
)

// -----------------------------------------------------------------------------
// Log level
// -----------------------------------------------------------------------------

// LogLevel is the console verbosity.
type LogLevel int

const (
	// LevelNormal prints results and a periodic progress line only.
	LevelNormal LogLevel = iota
	// LevelDebug adds HTTP codes, body sizes, verdicts and cache use.
	LevelDebug
	// LevelTrace adds DNS / TCP / TLS / header / body-preview events.
	LevelTrace
)

// logLevel is the active verbosity, set by Main from -debug/-trace.
var logLevel LogLevel

// debugf prints when logLevel >= LevelDebug.
func debugf(format string, args ...any) {
	if logLevel >= LevelDebug {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// tracef prints when logLevel >= LevelTrace.
func tracef(format string, args ...any) {
	if logLevel >= LevelTrace {
		log.Printf("[TRACE] "+format, args...)
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

	freeCount    int64
	freeSumUs    int64
	freeMinUs    int64
	freeMaxUs    int64
	nonfreeCount int64
	nonfreeSumUs int64
	nonfreeMinUs int64
	nonfreeMaxUs int64

	startTime time.Time
}

// newStats returns a Stats with the start time set and min-trackers seeded
// to MaxInt64 so the first observed sample wins the CAS.
func newStats() *Stats {
	return &Stats{
		startTime:    time.Now(),
		freeMinUs:    math.MaxInt64,
		nonfreeMinUs: math.MaxInt64,
	}
}

// atomicMin sets *p = min(*p, v) using a CAS loop.
func atomicMin(p *int64, v int64) {
	for {
		old := atomic.LoadInt64(p)
		if old <= v {
			return
		}
		if atomic.CompareAndSwapInt64(p, old, v) {
			return
		}
	}
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

// observe feeds the latency figures (free/reserved vs. the rest) and the
// largest marker offset. Cache answers and errors are left out: they would
// skew the latency of real requests.
func (s *Stats) observe(r checker.Result) {
	if r.Cached || r.Status == StatusError {
		return
	}
	us := r.Took.Microseconds()
	switch r.Status {
	case StatusFree, StatusReserved:
		atomic.AddInt64(&s.freeCount, 1)
		atomic.AddInt64(&s.freeSumUs, us)
		atomicMin(&s.freeMinUs, us)
		atomicMax(&s.freeMaxUs, us)
	default:
		atomic.AddInt64(&s.nonfreeCount, 1)
		atomic.AddInt64(&s.nonfreeSumUs, us)
		atomicMin(&s.nonfreeMinUs, us)
		atomicMax(&s.nonfreeMaxUs, us)
	}
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
	return fmt.Sprintf(
		"%d/%d (%.1f req/s) | free=%d reserved=%d auction=%d available=%d taken=%d sold=%d unknown=%d err=%d | new=%d reuse=%d | 429=%d retry=%d pause=%d",
		e.Checked, atomic.LoadInt64(&s.total), rps,
		e.Free, e.Reserved, e.Auction, e.Available, e.Taken, e.Sold, e.Unknown, e.Errors,
		e.ConnNew, e.ConnReuse, e.RateLimit, e.Retries, e.Pauses,
	)
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
	line += fmt.Sprintf("\n  Connections: new=%d, reused=%d | Rate-limits: %d | Retries: %d",
		e.ConnNew, e.ConnReuse, e.RateLimit, e.Retries)
	line += fmt.Sprintf("\n  Page cache: hits=%d, stale=%d, misses=%d", e.CacheHits, e.CacheStale, e.CacheMisses)
	if e.Challenges > 0 {
		line += fmt.Sprintf("\n  Cloudflare challenges: %d", e.Challenges)
	}
	if logLevel >= LevelDebug {
		if fc := atomic.LoadInt64(&s.freeCount); fc > 0 {
			avg := float64(atomic.LoadInt64(&s.freeSumUs)) / float64(fc) / 1000
			mn := float64(atomic.LoadInt64(&s.freeMinUs)) / 1000
			mx := float64(atomic.LoadInt64(&s.freeMaxUs)) / 1000
			line += fmt.Sprintf("\n  Timing FREE:     avg=%.0fms min=%.0fms max=%.0fms (n=%d)", avg, mn, mx, fc)
		}
		if nc := atomic.LoadInt64(&s.nonfreeCount); nc > 0 {
			avg := float64(atomic.LoadInt64(&s.nonfreeSumUs)) / float64(nc) / 1000
			mn := float64(atomic.LoadInt64(&s.nonfreeMinUs)) / 1000
			mx := float64(atomic.LoadInt64(&s.nonfreeMaxUs)) / 1000
			line += fmt.Sprintf("\n  Timing NON-FREE: avg=%.0fms min=%.0fms max=%.0fms (n=%d)", avg, mn, mx, nc)
		}
		if maxOff := atomic.LoadInt64(&s.statusOffsetMax); maxOff > 0 {
			line += fmt.Sprintf("\n  Status offset max: %d bytes (readLimit=%d, you can tighten it to %d via -readlimit)",
				maxOff, readLimit, maxOff+safetyMargin)
		}
		if e.Unknown > 0 {
			line += fmt.Sprintf("\n  WARN: unknown=%d — either readLimit is too small or the redirect target is new; re-run with -trace", e.Unknown)
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
func produceUsernames(
	ctx context.Context,
	src io.Reader,
	tasks chan<- string,
	dedup bool,
	skip *checker.NameSet,
	stats *Stats,
) (rejected int64, err error) {
	var seen checker.NameSet
	err = checker.ScanNames(src, func(name string) error {
		if dedup && !seen.Add(name) {
			return nil
		}
		if skip.Has(name) {
			rejected++
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
type outputs struct {
	file    *bufio.Writer // the results file
	stdout  *bufio.Writer
	checked *bufio.Writer // <output>.checked: every name with a verdict
}

// flush flushes every writer and returns the first error of the files.
func (o outputs) flush() error {
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

// writeResults writes every result to the output file and stdout until
// results is closed, flushing both every flushInterval so the file fills in
// real time. With -filter it also lists every name that got a verdict in
// <output>.checked, so -resume can skip the names the filter left out of
// the results file. It does not react to Ctrl+C on purpose: every result
// the engine counted must land on disk. A failed final flush (disk full,
// I/O error) is returned.
func writeResults(results <-chan checker.Result, out outputs, filter map[Status]bool, quiet bool, stats *Stats) error {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	const resetColor = "\033[0m"
	for {
		select {
		case <-ticker.C:
			_ = out.flush()
		case r, ok := <-results:
			if !ok {
				return out.flush()
			}
			stats.observe(r)
			fileLine, stdoutLine, color := formatResult(r, filter, quiet)
			if fileLine != "" {
				_, _ = out.file.WriteString(fileLine)
				_ = out.file.WriteByte('\n')
			}
			if out.checked != nil && r.Status != StatusError {
				_, _ = out.checked.WriteString(r.Username)
				_ = out.checked.WriteByte('\n')
			}
			if stdoutLine != "" {
				if color != "" {
					_, _ = out.stdout.WriteString(color + stdoutLine + resetColor + "\n")
				} else {
					_, _ = out.stdout.WriteString(stdoutLine + "\n")
				}
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
func runProgress(ctx context.Context, stats *Stats, chk *checker.Checker) {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	var lastChecked int64
	lastTime := time.Now()
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
			log.Printf("[progress] %s | inst=%.0f r/s", stats.snapshot(e), instRPS)
		}
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

		debugFlag = fs.Bool("debug", false, "Enable debug-level logging")
		traceFlag = fs.Bool("trace", false, "Enable trace-level logging (implies -debug)")
		showVer   = fs.Bool("version", false, "Print version and exit")
	)
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
	switch {
	case *traceFlag:
		logLevel = LevelTrace
	case *debugFlag:
		logLevel = LevelDebug
	}

	if *inputPath == "" || *outputPath == "" {
		printUsage(fs)
		return 2
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
	if logLevel >= LevelTrace {
		logger.Tracef = tracef
	}
	chk.SetLogger(logger)
	eff := chk.Config()

	// --- Log level banner ---
	levelName := "normal"
	switch logLevel {
	case LevelDebug:
		levelName = "debug"
	case LevelTrace:
		levelName = "trace"
	}
	filterStr := *filterRaw
	if filterStr == "" {
		filterStr = "all"
	}
	log.Printf("%s v%s | workers=%d conns=%d retries=%d retry-delay=%v rate=%d timeout=%v filter=%s log=%s",
		ProgName, Version, eff.Workers, eff.MaxConns, eff.Retries, eff.RetryDelay, eff.RateRPS, eff.Timeout, filterStr, levelName)

	// --- Context and signals ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
			cancel()
		case <-ctx.Done():
		}
	}()

	stats := newStats()
	tasks := make(chan string, tasksChanCap)
	results := make(chan checker.Result, resultsChanCap)

	// --- Output writer (lives until close(results), independent of ctx) ---
	writeDone := make(chan error, 1)
	go func() { writeDone <- writeResults(results, out, filter, *quiet, stats) }()

	// --- Progress reporter ---
	progCtx, progCancel := context.WithCancel(ctx)
	go runProgress(progCtx, stats, chk)

	// --- Checks: eff.Workers goroutines inside the engine ---
	runDone := make(chan struct{})
	go func() {
		chk.Run(ctx, tasks, results)
		close(runDone)
	}()

	// --- Input producer ---
	producerDone := make(chan error, 1)
	go func() {
		rejected, err := produceUsernames(ctx, inReader, tasks, !*noDedup, skip, stats)
		if rejected > 0 {
			log.Printf("[resume] skipped %d already-processed names", rejected)
		}
		close(tasks)
		producerDone <- err
	}()

	// --- Wait for the checks ---
	// Run returns when the list is done, on Ctrl+C, or when it gives up
	// because Fragment answers every request with a Cloudflare challenge.
	// In the last case the producer still waits to hand over a name.
	<-runDone
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
	progCancel()

	// --- Close the results and wait for the final flush ---
	// A failed write (disk full, I/O error) means results.txt is missing
	// results: report it in the exit code, not only in the log.
	exitCode := 0
	close(results)
	if err := <-writeDone; err != nil {
		log.Printf("[ERROR] writing results: %v", err)
		exitCode = 1
	}

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
	return exitCode
}

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
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Statuses describe Fragment, not Telegram: \"free\" means Fragment has no page")
	fmt.Fprintln(os.Stderr, "for the name (not a collectible, not for sale); a regular account may still hold it.")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Flags:")
	fs.PrintDefaults()
}
