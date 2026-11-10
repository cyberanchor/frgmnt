// Package tracelog writes the structured trace of the engine
// (checker.TraceEvent) as JSON Lines, for the console's -trace and the
// panel's per-worker trace, and reads it back.
//
// One goroutine encodes and writes; the workers only queue their events.
// A path ending in ".gz" is written with gzip. With a size limit the trace
// keeps its tail in two segments: when the current file holds half of the
// limit it becomes the older segment ("run.1.jsonl.gz" next to
// "run.jsonl.gz") and a new current file starts with a "segment" event that
// repeats the run_start of the run, so the end of a run (where it failed or
// stopped) is always kept and the tail can be read on its own. When the
// disk runs low the trace stops: the results of a run matter more.
package tracelog

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	rpprof "runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
)

// Schema is the version of the event format; run_start and segment events
// carry it in Info["schema"]. 2 is the format of frgmnt 3.0.0: seq on
// every line, the events of the "every byte" trace.
const Schema = 2

// DefaultMinFree is the free disk space below which a trace stops.
const DefaultMinFree = 256 << 20

// queueLen is how many events can wait for the writer goroutine. When the
// queue is full the workers wait for room, so the trace loses nothing and
// memory stays bounded (an event can carry a page body of up to 1 MB).
const queueLen = 512

// flushEvery is how often the writer flushes and checks the disk; a
// variable so tests can make it short.
var flushEvery = time.Second

// freeSpace reports the free space of the file system holding dir; ok is
// false where it cannot be read. A variable so tests can fake a full disk.
var freeSpace = diskFree

// FreeSpace reports the space left on the file system holding dir (Linux;
// ok is false elsewhere). The execution trace uses it too.
func FreeSpace(dir string) (int64, bool) { return freeSpace(dir) }

// Options configure Open.
type Options struct {
	// Append adds to the files of an earlier run (a resumed run) instead
	// of starting them again.
	Append bool
	// MaxBytes caps the trace on disk, both segments together (as stored,
	// so compressed for ".gz"); 0 means no limit.
	MaxBytes int64
	// MinFree stops the trace when the disk holding it has less free
	// space; 0 = no check. The check works on Linux.
	MinFree int64
}

// Stats are the counters of a Writer.
type Stats struct {
	Events    int64   `json:"events"`            // written
	Dropped   int64   `json:"dropped"`           // not written: after Close, or after the trace stopped
	Bytes     int64   `json:"bytes"`             // written by this Writer, as stored
	Rotations int64   `json:"rotations"`         // times the current file became the older segment
	Queue     int     `json:"queue"`             // events waiting to be written
	BlockedMs float64 `json:"blocked_ms"`        // time the workers waited for room in the queue
	Stopped   string  `json:"stopped,omitempty"` // why the trace stopped before the end of the run
}

// Writer writes one event per line. Safe for concurrent use: every worker
// of a run writes to it.
type Writer struct {
	path, older string
	opt         Options
	gzip        bool

	mu     sync.RWMutex // Write holds it shared while it queues; Close takes it to close the queue
	closed bool
	queue  chan checker.TraceEvent
	done   chan struct{} // closed when the writer goroutine has finished

	events, dropped, bytes, rotations, blockedNs atomic.Int64
	stopped                                      atomic.Pointer[string]

	// Owned by the writer goroutine.
	seg       *segment
	bytesDone int64 // bytes of the segments closed so far
	seq       int64
	header    *checker.TraceEvent // run_start of the run, repeated by each new segment
	buf       bytes.Buffer
	enc       *json.Encoder
	err       error // the first write error; read by Close after done
}

// segment is the current file.
type segment struct {
	f    *os.File
	buf  *bufio.Writer
	cnt  *counter     // bytes as stored
	gz   *gzip.Writer // nil for a plain file
	out  io.Writer    // where lines go: gz or cnt
	base int64        // size of the file when it was opened (appending)
}

func (s *segment) size() int64 { return s.base + s.cnt.n }

func (s *segment) flush() error {
	if s.gz != nil {
		if err := s.gz.Flush(); err != nil {
			return err
		}
	}
	return s.buf.Flush()
}

func (s *segment) close() error {
	var err error
	if s.gz != nil {
		err = s.gz.Close()
	}
	if e := s.buf.Flush(); err == nil {
		err = e
	}
	if e := s.f.Close(); err == nil {
		err = e
	}
	return err
}

type counter struct {
	w io.Writer
	n int64
}

func (c *counter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// OlderPath is the name of the older segment of path: "run.jsonl.gz" ->
// "run.1.jsonl.gz", "trace" -> "trace.1".
func OlderPath(path string) string {
	dir, base := filepath.Split(path)
	if i := strings.Index(base[min(1, len(base)):], "."); i >= 0 {
		i += min(1, len(base))
		return dir + base[:i] + ".1" + base[i:]
	}
	return path + ".1"
}

// Files lists the files of the trace at path that exist, oldest first.
func Files(path string) []string {
	var out []string
	for _, p := range []string{OlderPath(path), path} {
		if _, err := os.Stat(p); err == nil {
			out = append(out, p)
		}
	}
	return out
}

// Open starts the trace at path with mode 0600 (a trace holds the URLs,
// headers and pages of a run), or appends to it.
func Open(path string, opt Options) (*Writer, error) {
	t := &Writer{
		path: path, older: OlderPath(path), opt: opt, gzip: strings.HasSuffix(path, ".gz"),
		queue: make(chan checker.TraceEvent, queueLen), done: make(chan struct{}),
	}
	t.enc = json.NewEncoder(&t.buf)
	t.enc.SetEscapeHTML(false) // pages and headers stay readable: "<" not "\u003c"
	if !opt.Append {
		// A new trace: the older segment of an earlier run is not part of it.
		if err := os.Remove(t.older); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	seg, err := t.openSegment(opt.Append)
	if err != nil {
		return nil, err
	}
	t.seg = seg
	go rpprof.Do(context.Background(), rpprof.Labels("component", "trace-writer"), func(context.Context) { t.run() })
	return t, nil
}

func (t *Writer) openSegment(appendTo bool) (*segment, error) {
	flags := os.O_CREATE | os.O_WRONLY
	if appendTo {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(t.path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	s := &segment{f: f, buf: bufio.NewWriterSize(f, 64<<10)}
	if appendTo {
		if fi, err := f.Stat(); err == nil {
			s.base = fi.Size()
		}
	}
	s.cnt = &counter{w: s.buf}
	s.out = s.cnt
	if t.gzip {
		// A new gzip member; appended to an earlier one it still reads as
		// one stream (gzip -dc, zcat, ReadAll).
		s.gz, _ = gzip.NewWriterLevel(s.cnt, gzip.BestSpeed)
		s.out = s.gz
	}
	return s, nil
}

// Write queues one event. It is the checker.Logger.Event of a run. The
// time is stamped here, when the event happens, not when it is written.
func (t *Writer) Write(e checker.TraceEvent) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		t.dropped.Add(1)
		return
	}
	select {
	case t.queue <- e:
		return
	default:
	}
	start := time.Now()
	t.queue <- e
	t.blockedNs.Add(int64(time.Since(start)))
}

// Close writes what is queued, flushes and closes the file. It can be
// called more than once.
func (t *Writer) Close() (Stats, error) {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		close(t.queue)
	}
	t.mu.Unlock()
	<-t.done
	return t.Stats(), t.err
}

// Stats returns the counters so far.
func (t *Writer) Stats() Stats {
	s := Stats{
		Events: t.events.Load(), Dropped: t.dropped.Load(), Bytes: t.bytes.Load(),
		Rotations: t.rotations.Load(), Queue: len(t.queue),
		BlockedMs: float64(t.blockedNs.Load()) / float64(time.Millisecond),
	}
	if p := t.stopped.Load(); p != nil {
		s.Stopped = *p
	}
	return s
}

func (t *Writer) run() {
	defer close(t.done)
	tick := time.NewTicker(flushEvery)
	defer tick.Stop()
	for {
		select {
		case e, ok := <-t.queue:
			if !ok {
				t.finish()
				return
			}
			t.write(e)
		case <-tick.C:
			if t.seg != nil {
				if err := t.seg.flush(); err != nil {
					t.fail(err)
				}
			}
			t.checkDisk()
		}
	}
}

func (t *Writer) write(e checker.TraceEvent) {
	if t.stopped.Load() != nil || t.seg == nil {
		t.dropped.Add(1)
		return
	}
	if e.Kind == "run_start" {
		e.Info = withSchema(e.Info)
		h := e
		t.header = &h
	}
	e.Seq = t.seq + 1
	line := t.encode(e)
	if t.opt.MaxBytes > 0 && t.seg.size() > 0 && t.seg.size()+int64(len(line)) > t.opt.MaxBytes/2 {
		if !t.rotate() {
			t.dropped.Add(1)
			return
		}
		e.Seq = t.seq + 1
		line = t.encode(e)
	}
	if t.put(line) {
		t.seq = e.Seq
		t.events.Add(1)
	} else {
		t.dropped.Add(1)
	}
}

// encode returns the JSON line of e; it is valid until the next call.
func (t *Writer) encode(e checker.TraceEvent) []byte {
	t.buf.Reset()
	if err := t.enc.Encode(e); err != nil {
		// Cannot happen with the types of TraceEvent; keep the line count.
		t.buf.Reset()
		fmt.Fprintf(&t.buf, `{"seq":%d,"kind":"encode_error","message":%q}`+"\n", e.Seq, err.Error())
	}
	return t.buf.Bytes()
}

func (t *Writer) put(line []byte) bool {
	if _, err := t.seg.out.Write(line); err != nil {
		t.fail(err)
		return false
	}
	t.bytes.Store(t.bytesDone + t.seg.cnt.n)
	return true
}

// rotate turns the current file into the older segment (replacing the
// one before it) and starts a new current file with a segment event.
func (t *Writer) rotate() bool {
	t.bytesDone += t.seg.cnt.n
	if err := t.seg.close(); err != nil {
		t.seg = nil
		t.fail(err)
		return false
	}
	t.seg = nil
	if err := os.Rename(t.path, t.older); err != nil {
		t.fail(err)
		return false
	}
	seg, err := t.openSegment(false)
	if err != nil {
		t.fail(err)
		return false
	}
	t.seg = seg
	t.rotations.Add(1)
	h := checker.TraceEvent{Kind: "segment", Time: time.Now(), Seq: t.seq + 1,
		Message: fmt.Sprintf("the trace keeps its last %d MB: the part before this line is in %s, anything older was dropped",
			t.opt.MaxBytes>>20, filepath.Base(t.older))}
	if t.header != nil {
		h.Info = t.header.Info
	} else {
		h.Info = withSchema(nil)
	}
	if !t.put(t.encode(h)) {
		return false
	}
	t.seq = h.Seq
	t.events.Add(1)
	return true
}

func (t *Writer) checkDisk() {
	if t.opt.MinFree <= 0 || t.seg == nil || t.stopped.Load() != nil {
		return
	}
	free, ok := freeSpace(filepath.Dir(t.path))
	if !ok || free >= t.opt.MinFree {
		return
	}
	reason := fmt.Sprintf("less than %d MB free on the disk (%d MB left)", t.opt.MinFree>>20, free>>20)
	e := checker.TraceEvent{Kind: "trace_stopped", Time: time.Now(), Seq: t.seq + 1,
		Message: "the trace stops here: " + reason + "; the run goes on"}
	if t.put(t.encode(e)) {
		t.seq = e.Seq
		t.events.Add(1)
	}
	if err := t.seg.flush(); err != nil {
		t.fail(err)
	}
	t.stopped.CompareAndSwap(nil, &reason)
}

// fail records the first write error and stops the trace; the run goes on.
func (t *Writer) fail(err error) {
	if t.err == nil {
		t.err = err
	}
	reason := "write error: " + err.Error()
	t.stopped.CompareAndSwap(nil, &reason)
}

func (t *Writer) finish() {
	if t.seg == nil {
		return
	}
	t.bytesDone += t.seg.cnt.n
	if err := t.seg.close(); err != nil && t.err == nil {
		t.err = err
	}
	t.seg = nil
	t.bytes.Store(t.bytesDone)
}

// withSchema returns a copy of info with the schema version.
func withSchema(info map[string]any) map[string]any {
	out := make(map[string]any, len(info)+1)
	for k, v := range info {
		out[k] = v
	}
	out["schema"] = Schema
	return out
}

// ReadAll reads the events of the given trace files in order; a file may
// be plain JSON Lines or gzip (several members too). A gzip file that is
// still being written ends at its last flush, which is not an error.
func ReadAll(paths ...string) ([]checker.TraceEvent, error) {
	var out []checker.TraceEvent
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return out, err
		}
		events, err := Decode(f)
		f.Close()
		out = append(out, events...)
		if err != nil {
			return out, fmt.Errorf("%s: %w", p, err)
		}
	}
	return out, nil
}

// Decode reads events from r, plain or gzip.
func Decode(r io.Reader) ([]checker.TraceEvent, error) {
	br := bufio.NewReaderSize(r, 64<<10)
	var src io.Reader = br
	if magic, _ := br.Peek(2); len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		src = gz
	}
	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 64<<10), 16<<20) // a line can hold a page body
	var out []checker.TraceEvent
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		var e checker.TraceEvent
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return out, fmt.Errorf("line %d: %w", len(out)+1, err)
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return out, err
	}
	return out, nil
}

// Environment describes the process for run_start: the build (Go
// version, module, VCS revision), the machine (OS, CPUs) and the runtime
// settings, so a trace says what produced it.
func Environment() map[string]any {
	env := map[string]any{
		"go": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"num_cpu": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0), "pid": os.Getpid(),
	}
	if v := os.Getenv("GODEBUG"); v != "" {
		env["godebug"] = v
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		env["module"] = bi.Main.Path
		if bi.Main.Version != "" {
			env["module_version"] = bi.Main.Version
		}
		for _, st := range bi.Settings {
			switch st.Key {
			case "vcs.revision", "vcs.time", "vcs.modified", "-trimpath", "CGO_ENABLED", "-race":
				env[st.Key] = st.Value
			}
		}
	}
	return env
}

// StatsInfo turns the engine's counters into the info of a run_end event.
func StatsInfo(e checker.Stats) map[string]any {
	return map[string]any{
		"checked": e.Checked, "free": e.Free, "reserved": e.Reserved, "auction": e.Auction,
		"available": e.Available, "taken": e.Taken, "sold": e.Sold, "unknown": e.Unknown,
		"errors": e.Errors, "retries": e.Retries, "rate_limited": e.RateLimit, "pauses": e.Pauses,
		"challenges": e.Challenges, "conn_new": e.ConnNew, "conn_reused": e.ConnReuse,
		"cache_hits": e.CacheHits, "cache_stale": e.CacheStale, "cache_misses": e.CacheMisses, "cache_errors": e.CacheErrors,
		"bytes_in": e.BytesIn, "bytes_out": e.BytesOut,
	}
}

// BlockedName is the "blocked" info of run_end: why the run stopped, or "".
func BlockedName(k checker.BlockKind) string {
	switch k {
	case checker.BlockedByChallenge:
		return "challenge"
	case checker.BlockedByThrottle:
		return "throttle"
	}
	return ""
}
