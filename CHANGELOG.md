# Changelog

## 3.0.1 (2026-11-11)

### Changed

- Go 1.26 or newer is needed (`go.mod`); releases are built with Go 1.27.
  The versions used so far (1.21 to 1.23) no longer get security fixes,
  and a binary takes `net/http` and `crypto/tls` from the Go that builds it.
- CI runs on the two supported Go releases (`stable` and `oldstable`) and
  runs `govulncheck`.

### Fixed

- A bad path in `-trace`, `-tls-keylog`, `-cpuprofile`, `-memprofile` or
  `-exectrace`, or a `-pprof` address that cannot be used, stopped the run
  after the results file had been truncated: the previous results were
  lost. These files are opened before the results file now.
- Invalid names (too short, a digit first) are final: `-resume` no longer
  checks them again, and they no longer make a run end with exit 1 when
  nothing else was left. The summary counts them.
- Trace sizes in the log: "keeps its last 0 MB" for a 96 KB limit, "0.0 MB"
  in the panel, and the bytes written instead of the bytes on disk after a
  rotation.
- Panel: "Pages in the trace" is disabled until "Write a trace" is on.
- README: `seq` is numbered per run (a `-resume` run appended to a trace
  starts at 1 again).

## 3.0.0 (2026-11-10)

The trace covers everything a run does, down to the bytes on the wire, and
the program can be profiled.

### Changed behaviour: read before upgrading

- `-trace` takes a file now: `-trace run.jsonl.gz` writes the structured
  trace (it was a flag that printed every step on the console and turned
  `-debug` on). `-debug` and `-trace` are independent. The text trace level
  is gone; `-trace-file` still works. A script that says `-trace -workers
  200` or `-trace=true` gets exit 2 and an explanation.
- The panel's "trace" log level is gone (a saved or sent "trace" means
  debug); every step of every request is in the worker's trace file.
- Panel traces are gzip (`<data>/traces/<id>.jsonl.gz`) and keep the last
  100 MB (they kept the first 100 MB, so the end of a long run was cut);
  the download is `application/gzip`.
- Result times split three ways: `Took = Net + Wait + Work`. The wait for
  a free connection (`-conns`) moved from Net to Wait, parsing and the page
  cache from Net to Work (a new `work_ns` column in the panel).

### Added

- The trace (console `-trace FILE`, panel "Write a trace"):
  - every request with all its headers both ways (cookie values hidden),
    status line, length, trailers, every timing (DNS with its addresses,
    TCP, TLS, headers written, first byte, body), the wait for a
    connection, the bytes on the wire (HTTP/1.x) and of the body;
  - the pages: `-trace-bodies anomalies` (default: unknown answers,
    challenges, unexpected codes, errors), `all`, `none`; up to 1 MB each;
  - connections: `conn_open`, `conn_tls` (version, cipher, ALPN,
    certificate), `conn_close` (lifetime, bytes, requests), `conn_error`;
  - the engine's decisions: why each try was repeated and how long it
    waited, reused probes, challenge runs, `stop` with the reason;
  - ids that tie it together: `nid` (a check of a name), `aid` (a request),
    `cid` (a connection), `w` (the worker), `seq` (the line);
  - around the engine: the list read (`input`, `input_skip`), the output
    written, the time the end took, the panel's writes to SQLite
    (`db_flush`) and the list a run started from;
  - the process every second (`runtime`): goroutines, heap, GC pauses,
    scheduling latency, CPU, resident memory, open files;
  - `run_start` with the build, the machine and the whole config.
- The trace writer: one goroutine, gzip for `.gz`, `-trace-max` (default
  1G) keeping the tail in two segments, and it stops when less than 256 MB
  are free on its disk.
- Profiling: `-pprof ADDR`, `-cpuprofile`, `-memprofile`, `-exectrace`
  in the console, `frgmnt web -pprof` (behind the login); pprof labels per
  component and tile; a `check` task per name in the execution trace.
- `-tls-keylog FILE` (console and panel): the TLS keys for Wireshark.
- The panel logs every request to it at the debug level (not its own
  quick polls, never a token).
- README: "Debug, trace and profiling".

### Fixed

- The httptrace hooks wrote a request's record from the transport's
  goroutines without a lock (a data race under HTTP/2 and on cancelled
  dials).
- The trace lost the `conn_close` events and could have a progress event
  after `run_end`; a `-trace` path equal to `-output` (or the input)
  wiped that file; a broken page-cache entry was a silent miss;
  `wait_probe` counted the worker making the probe twice.
- A rate test failed on a loaded machine (it measured from the first
  arrival instead of the run's start).

## 2.1.0 (2026-11-09)

Debug and trace cover every request and every step of the engine.

- Every result splits its time into network and waiting (a -rate start,
  the gate, a pause, a retry delay, the probe) and counts its tries; the
  -debug summary shows p50/p95/p99/max of both instead of avg/min/max of
  the whole time (which counted the wait for a -rate start as latency).
- At -debug every HTTP try ends with one line: its number (#1, #2), the
  outcome, the waits before it, DNS/TCP/TLS/first byte/body and the
  connection; the throttle probe gets the same line. At -trace the request
  is printed too; cookie values are hidden everywhere.
- Structured trace (JSON Lines): `-trace-file PATH` in the console, and a
  "Write a trace" switch per worker in the panel (<data>/traces/<id>.jsonl,
  100 MB cap, Download trace in the details, GET/DELETE
  /api/workers/{id}/trace). One event per request, cache lookup, name,
  pause, gate change and stop, plus run_start, run_end and a pipeline event
  every second.
- The panel's Logs tab gets the engine's debug and trace lines, following
  the log level while a run goes on.
- Stats and the -debug progress line show the queues and what the workers
  do (on the network, at the gate, in a pause, waiting for a start, a
  retry, the probe).
- Results in the panel keep network time, waiting time and tries (three
  new columns, added on start); the details tooltip shows them.

## 2.0.0 (2026-11-09)

One version for the whole program: `frgmnt version`, `frgmnt check
-version`, `frgmnt web -version` and the panel's header all show 2.0.0
(they showed 1.0.0, 3.1, 0.3.0 and "v0.2").

### Changed behaviour: read before upgrading

- The panel needs an access token: create one with `frgmnt token -data
  ./data` and paste it on the login page, or send `Authorization: Bearer`.
  HTTPS with `-tls-cert`/`-tls-key`, a reverse proxy domain with
  `-allow-host`.
- `-rate` (`rate_rps`) counts requests to Fragment, retries included;
  answers from the page cache and invalid names do not count. Requests are
  spaced evenly, and a pause does not save up starts for later.
- Throttling (a bare `302 /` for the probe too, or HTTP 429) pauses every
  worker, 3 s and then twice as long each time up to 2 minutes, and does not
  use up the attempts of a name: the name gets its verdict afterwards
  instead of a `persistent 302 /` error. When throttling lasts
  `-throttle-max` (30 min, `throttle_max_ns`), the run stops like on a
  Cloudflare block: exit code 3 or status `error`, and the names without a
  verdict stay for `-resume` / Resume.
- HTTP 429 pauses every worker for its `Retry-After`; it made only the
  worker that got it sleep.
- Names that cannot be Telegram usernames (shorter than 4 or longer than 32
  characters, not starting with a letter, ending with `_`) are never sent.
- A list upload over 100 MB is answered with 413.

### Faster

- SQLite: multi-row statements, `synchronous=NORMAL`, a 16 MB page cache:
  200,000 names stored in 1.9 s instead of 7.3 s, results saved at about
  18,000 rows/s instead of 2,326. A panel upload of 500,000 shuffled names
  takes 12.6 s instead of 21.8 s.
- Duplicates and the `-resume` set kept in a compact exact set: 1,000,000
  names take 82 MB instead of 162 MB.
- Name lists read in 64 KB chunks: about 2.3 times faster.
- The panel builds the per-result debug event only at the debug level.

### More robust

- A busy database no longer loses results: the writer keeps them, tries
  again, and makes the checks wait.
- A panel upload is parsed while it arrives (no temporary file, no copy of
  the list as strings), and the database is only taken for the write.
  Start and a second upload are refused while a list is stored; the WAL
  file no longer keeps the size of the last big list.
- After a throttle pause one request goes first; the others follow when it
  gets a normal answer. The gate of the first requests wakes every waiting
  request when it opens (a run whose second name was slow took 4.0 s
  instead of 2.0 s).
- Cloudflare challenges are recognised (`cf-mitigated: challenge`, or the
  challenge page); after 10 in a row the run stops, exit code 3, instead of
  turning the rest of the list into errors. Resume and `-resume` check the
  names that ended with an error again.

### Console

- The progress line shows the real size of the list and an ETA, and
  `pause=N`; results appear as they come instead of every 2 s.
- The console uses the panel's engine (retries, the `302 /` probe, the page
  cache, the parser), and reads lists like the panel.

### Panel

- Start all continues stopped workers and keeps results; progress counts
  come from the database and survive a restart; a new list keeps the
  results of the names that are in it; Clear makes every name pending.
- Settings and worker configs are validated (negative or too large values
  get 400); the page cache directory cannot be set through the API.
- The engine's warnings (throttle pauses, 429, the stop) are in the tile's
  log.

`free` means Fragment has no page for the name; a regular Telegram account
may still hold it (the help, the README and the panel say so).
