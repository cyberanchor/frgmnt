<div align="center">

<h1> frgmnt </h1>

<sub>Fragment.com username availability checker. One static Go binary with two modes: a console checker for big lists and a web panel with worker tiles, resume and live progress. Every verdict carries a confidence score and the evidence behind it.</sub>

<kbd>🔎 Usernames</kbd> <kbd>⚡ Concurrent</kbd> <kbd>⏯️ Resume</kbd> <kbd>🗄️ Page cache</kbd> <kbd>🖥️ Web panel</kbd> <kbd>⌨️ Console</kbd>

![GitHub Stars](https://img.shields.io/github/stars/cyberanchor/frgmnt?color=green&style=social)
![GitHub Forks](https://img.shields.io/github/forks/cyberanchor/frgmnt?color=green&style=social)
![GitHub Issues](https://img.shields.io/github/issues/cyberanchor/frgmnt?color=green&style=flat-square)
![GitHub Pull Requests](https://img.shields.io/github/issues-pr/cyberanchor/frgmnt?color=green&style=flat-square)
![GitHub Contributors](https://img.shields.io/github/contributors/cyberanchor/frgmnt?color=green&style=flat-square)

[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white&style=flat-square)](https://go.dev/)
[![SQLite](https://img.shields.io/badge/SQLite-pure_Go-003B57?logo=sqlite&logoColor=white&style=flat-square)](https://pkg.go.dev/modernc.org/sqlite)
![HTML5](https://img.shields.io/badge/HTML5-E34F26?logo=html5&logoColor=white&style=flat-square)
![CSS3](https://img.shields.io/badge/CSS3-1572B6?logo=css3&logoColor=white&style=flat-square)
![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?logo=javascript&logoColor=black&style=flat-square)
[![CI](https://img.shields.io/github/actions/workflow/status/cyberanchor/frgmnt/ci.yml?branch=main&label=CI&style=flat-square)](https://github.com/cyberanchor/frgmnt/actions/workflows/ci.yml)

</div>

> **Based on:** the public username pages of [fragment.com](https://fragment.com/) (`https://fragment.com/username/<name>`). No API key and no Telegram account are needed.

> **Output:** the console mode writes one `@name | status` line per name to `results.txt`; the web mode keeps workers, inputs and results in SQLite ([`data/data.db`](#data-layout)) and serves them over a REST + SSE API.

## Quick start

```bash
git clone https://github.com/cyberanchor/frgmnt && cd frgmnt
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o frgmnt ./cmd/frgmnt

# console: a list of names in, "@name | status" lines out
./frgmnt check -input names.txt -output results.txt -workers 200

# web panel on http://127.0.0.1:8080: create an access token, then open it
./frgmnt token -data ./data
./frgmnt web
```

Go **1.26+** (build releases with the newest Go: the standard library's
security fixes, in net/http and crypto/tls for one, come with it), no CGO,
no system SQLite. `./frgmnt version` prints
`frgmnt 3.0.0` (the console, the panel and its header show the same version); `./frgmnt help`, `./frgmnt check -h` and
`./frgmnt web -h` and `./frgmnt token -h` list every flag. The old shorthands still work:
`./frgmnt -input ...` runs `check`, `./frgmnt -addr ...` runs `web`.

## Statuses

| Status       | Meaning                                                  | Signal on the page                     |
| ------------ | -------------------------------------------------------- | -------------------------------------- |
| 🟢 free      | Not on Fragment: no page, not a collectible, not for sale. A regular account may still hold it (see below) | redirect to `/?query=<name>` |
| 🟣 reserved  | Reserved by Fragment (a bare `302 /` that survives retries and a healthy probe) | redirect to `/`  |
| 🟡 auction   | Live auction                                             | `On auction`, `js-place-bid-btn`, timer |
| 🔵 available | Fixed-price sale                                         | `For sale`, `js-buy-now-btn`           |
| ⚪ taken     | A collectible bound to an account, offers only           | `Taken`, `js-make-offer-btn`           |
| ⚫ sold      | Bought on Fragment                                       | `Sold`, `Purchased on`                 |
| ❔ unknown   | The page matched no known layout                         |                                        |
| ❌ error     | Network or HTTP error, Cloudflare challenge, throttling, or a name that cannot exist |                  |

The statuses describe Fragment, not Telegram. Fragment only has pages for
the names of its own market (auctions, sales, sold and bound collectibles)
and redirects every other name to its search, which is what `free` reports.
Whether a regular account holds such a name cannot be seen on Fragment: in
a live check on 2026-11-08 `@telegram`, the official channel, came back
`free`, and Fragment's search showed "Unavailable · Not for sale" for it and
for a random never-used name alike.

The parser trusts the visible status label first (`On auction`, `For sale`,
`Sold`, `Taken`), then the JS hooks and "Purchased on", and only then the
`tm-status-*` class. Every result has a confidence score (0–100) and an
evidence chain such as `text="for sale"; class=avail; timer; buy-now`.

- **Several badges**: a header can show `BANNED` and `Sold`; the commercial badge wins.
- **Auction with buy-now**: `On auction` still wins, it is not a conflict.
- **`302 /` is ambiguous**: Fragment uses it for rare reserved names and for
  throttled clients. A random free name is probed at once (one probe shared
  by all workers): if it is redirected to `/` as well, the client is
  throttled. A reserved name is retried with the same cookie session, and the
  verdict takes a fresh probe: only a healthy probe gives `reserved`, never a
  false one. A reserved name does not slow the other workers down.
- **Throttling** (`302 /` for the probe too, or HTTP 429): every worker pauses,
  3 s the first time and twice as long each next time, up to 2 minutes (for
  429: its `Retry-After`). After the pause one request goes first; the others
  follow when it gets a normal answer. A try while throttled does not count
  as an attempt, so the name waits and gets its real verdict afterwards
  instead of an error. If the throttling lasts `-throttle-max` (30 min), the
  run stops like on a Cloudflare block: exit code 3, status `error` in the
  panel, and the names without a verdict stay for `-resume` / Resume.
- **Cloudflare challenge**: an answer with `cf-mitigated: challenge` (HTTP 403)
  or a "Just a moment..." page says nothing about the name and is an error.
  After 10 of them in a row, with no other answer in between, Fragment is
  blocking this IP: the run stops instead of burning through the list. The
  console exits with code 3, a web worker ends with status `error`, and the
  names without a verdict stay for `-resume` / Resume.
- **Impossible names**: Telegram usernames have 4–32 characters from `a-z`,
  `0-9` and `_`, start with a letter and do not end with `_` (5 is the minimum
  for regular names, 4-character ones are Fragment collectibles). Fragment
  redirects any name it has no page for to `/?query=` (a `free`), so such names are
  never sent: the console writes `error (invalid: ...)`, the web panel counts
  them as rejected when the list is loaded.
- **Listings lag**: search pages can still show a name that was delisted. The
  username page is the source of truth.

## Console mode

```bash
./frgmnt check -input names.txt -output results.txt                  # 50 workers, 8 connections
./frgmnt check -input names.txt -output results.txt -resume          # skip names with a verdict in results.txt
cat names.txt | ./frgmnt check -input - -output free.txt -filter free -quiet
```

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `-input` | | Name list (`-` = stdin), read like in the web panel: new lines, spaces, commas and semicolons separate names, a leading `@` is stripped, the part of a line after `\|` is ignored (a results file can be read back) |
| `-output` | | Results file (truncated unless `-append` or `-resume`) |
| `-workers` | `50` | Parallel workers (up to 1000) |
| `-conns` | `8` | TCP connections to fragment.com (HTTP/2 multiplexing) |
| `-rate` | `0` | Requests per second to Fragment, retries included; names answered from the cache and invalid names do not count (0 = unlimited) |
| `-retries` / `-retry-delay` | `2` / `500ms` | Extra attempts per name and the pause between them |
| `-rl-wait-max` | `30s` | Longest `Retry-After` honoured on HTTP 429 |
| `-throttle-max` | `30m` | Stop when Fragment keeps throttling this long (names without a verdict stay for `-resume`) |
| `-timeout` | `15s` | Whole-request timeout |
| `-readlimit` | `16384` | Body bytes scanned for the status (1 KiB–1 MiB) |
| `-cache-dir` / `-no-cache` | `.fragment_cache` | Page cache directory, or no cache |
| `-cache-ttl` / `-cache-sold-ttl` | `2m` / `168h` | TTL of auction/sale pages and of sold pages |
| `-filter` | | Write only these statuses, e.g. `free,auction`; every name with a verdict is also listed in `<output>.checked`, so `-resume` skips the ones the filter left out |
| `-append` / `-resume` | | Append to the output; `-resume` also skips names that already have a verdict in it (error lines are checked again, except invalid names) |
| `-no-dedup` | | Do not deduplicate the input (saves RAM on huge files) |
| `-quiet` / `-debug` | | Less output on stdout, or one line per HTTP try with its timings on stderr and timing figures at the end |
| `-trace FILE` | | Structured trace of the run as JSON Lines (gzip when `FILE` ends in `.gz`); independent of `-debug` |
| `-trace-max` | `1G` | Largest size of the trace on disk (`0` = no limit); it keeps the end of the run: `FILE` holds the newest part, `FILE` with `.1` before the extension the part before it. The trace also stops when less than 256 MB are free on the disk |
| `-trace-bodies` | `anomalies` | Pages the trace keeps whole: `anomalies` (unknown answers, challenges, unexpected codes, errors), `all`, `none` |
| `-pprof ADDR` / `-cpuprofile` / `-memprofile` / `-exectrace FILE` | | Profiling: live pprof, a CPU profile, the heap profile, the execution trace (see [Debug, trace and profiling](#debug-trace-and-profiling)) |
| `-tls-keylog FILE` | | Append the TLS keys of the run to `FILE` for Wireshark (keep it private) |
| `-insecure` | | Skip TLS verification (debugging only) |
| `-base-url` | | Another Fragment base URL (tests, mirrors) |

Ctrl+C stops gracefully: finished results are flushed, checks that were cut
short are not written, so `-resume` checks them again.

Exit codes: `0` done; `1` the results file could not be written, or every
checked name ended with an error; `2` bad flags (unknown `-filter` status,
`-input` and `-output` being the same file, `-trace` without a file, a
trace or profile on a file of the run); `3` stopped because Fragment
answers with a Cloudflare challenge (see above).

## Web panel

```bash
./frgmnt token -data ./data                    # prints an access token (valid 30 days)
./frgmnt web                                   # http://127.0.0.1:8080, data in ./data
./frgmnt web -addr 127.0.0.1:9090 -data ./var -log-level debug
```

- **Worker tiles**: independent checker sessions, each with its own config,
  input list, progress bar and status breakdown. Start runs from scratch,
  Resume continues from the first unchecked name and checks names that
  ended with an error again. Start all starts new
  workers and continues stopped ones (it never deletes results; finished
  workers are left as they are), Stop all stops every running one.
- **Input**: paste names or upload a file; commas, spaces, tabs, semicolons and
  new lines all separate names, a leading `@` is stripped, and the part of a
  line after `|` is ignored (the console reads lists the same way). A new list keeps
  the results of the names that are in it (Resume checks only the new ones)
  and deletes the others; the panel asks first when there are results.
- **Results**: filter chips per status, plain-text download, confidence,
  evidence and cache marks.
- **Settings**: defaults for new workers (workers, rate, retries, timeout),
  theme, log level, history limit; saved to `data/settings.json`.
- **Logs** with a topic filter, pause and live tail; a **debug console** that
  sends raw API calls; progress in the tab title; browser notifications when a
  worker finishes.
- **State survives restarts**: workers, inputs, checked flags and results.
  On Ctrl+C the panel waits for running workers to save their last results;
  a second Ctrl+C quits at once.

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `-addr` | `127.0.0.1:8080` | Listen address |
| `-data` | `./data` | Directory for `data.db` and `settings.json` |
| `-log-level` | `info` | `error`, `warn`, `info`, `debug` (the old `trace` counts as `debug`) |
| `-cache-dir` / `-no-cache` | `<data>/pagecache` | Page cache directory, or no cache |
| `-cache-ttl` / `-cache-sold-ttl` | `2m` / `168h` | TTL of auction/sale pages and of sold pages |
| `-allow-host` | | Host names the panel answers to besides `localhost`, comma-separated: the domain of a reverse proxy in front of it |
| `-tls-cert` / `-tls-key` | | PEM certificate (full chain) and key: serve HTTPS; the files are read again every 10 s, so renewals apply without a restart |
| `-base-url` | | Another Fragment base URL (tests, mirrors) |
| `-pprof` | | Serve pprof under `/debug/pprof/`, behind the login |
| `-tls-keylog` | | Append the TLS keys of every run to a file for Wireshark (keep it private) |
| `-version` | | Print the version |

### Access

Every request needs an **access token**: a JWT signed (HS256) with the key in
`<data>/auth.key`, which the panel creates on its first start (mode 0600).
There are no users or passwords; whoever holds a valid token gets in.

```bash
./frgmnt token -data ./data                          # 30 days
./frgmnt token -data ./data -ttl 0 -name laptop      # never expires
./frgmnt token -data ./data -rotate                  # new key: every older token stops working
```

The token goes on the login page (`/login`), which keeps it in an HttpOnly
cookie (`Secure` over HTTPS, also behind a TLS proxy that sets
`X-Forwarded-Proto`), or into the header `Authorization: Bearer <token>` of
API calls. Without it the API answers 401 and the pages redirect to
`/login`. Logging out (the button in the top bar) only removes the cookie;
to revoke tokens, rotate the key, which a running panel picks up within a
second.

Two ways to reach the panel, both with a token:

- **SSH**: keep the default `-addr 127.0.0.1:8080` and open
  `ssh -L 8080:127.0.0.1:8080 server`, then `http://localhost:8080`.
- **A domain with a certificate**: either a TLS reverse proxy in front
  (nginx, Caddy) with the default `-addr` and
  `-allow-host panel.example.com`, or the panel serves HTTPS itself:

  ```bash
  ./frgmnt web -addr :8443 -allow-host panel.example.com \
      -tls-cert /etc/frgmnt/fullchain.pem -tls-key /etc/frgmnt/privkey.pem
  ```

  The panel user must be able to read the key (Let's Encrypt keys are
  root-only: copy them in a deploy hook). Port 443 needs root or
  `setcap cap_net_bind_service=+ep ./frgmnt`. A panel on a public address
  without TLS says in its log that tokens would travel in clear text.

The panel also refuses state-changing requests that a browser marks as
coming from another site (`Origin`, `Sec-Fetch-Site`), the login form
included, and while it listens on a loopback address it only answers to a
loopback `Host` (`127.0.0.1`, `localhost`, `::1`, any port, so `ssh -L`
works) or to a name given with `-allow-host` (its `Origin` is accepted too,
also when the proxy sends its own `Host`).

### REST API

| Method | Path | Description |
| ------ | ---- | ----------- |
| GET | `/api/health` | Liveness, worker count, log level |
| GET / PUT | `/api/settings` | Global settings; PUT changes only the fields it sends, out-of-range values get 400 |
| GET / POST | `/api/workers` | List / create workers (`{"name": ..., "config": {...}}`) |
| GET / PATCH / DELETE | `/api/workers/{id}` | One worker; PATCH changes only the fields it sends |
| POST | `/api/workers/{id}/input` | Load names from `{"text": "..."}` |
| POST | `/api/workers/{id}/input/upload` | Multipart upload, field `file` |
| POST | `/api/workers/{id}/start` · `/stop` · `/clear` | Run (`?resume=true` keeps results), cancel, clear results (every name is checked again by the next run) |
| POST | `/api/workers-bulk/start` · `/stop` | Continue every worker with unchecked names (never from scratch), stop every running one |
| GET | `/api/workers/{id}/results` | `?after=ID&limit=N`, with counts per status |
| GET | `/api/workers/{id}/results/download` | Plain-text results |
| GET | `/api/workers/{id}/events` | SSE: progress and log of one worker |
| GET / DELETE | `/api/workers/{id}/trace` | The worker's trace (gzip JSON Lines, the older segment first), or delete it (not while it runs) |
| GET | `/debug/pprof/...` | pprof, with `frgmnt web -pprof` only |
| GET | `/api/logs` · `/api/logs/stream` | Recent log entries (`?topic=ID&limit=N`), SSE live tail |

Bodies are JSON (50 MB cap; uploads 100 MB). Config limits: `workers` and
`max_conns` up to 1000, `rate_rps` up to 1000, `retries` up to 10,
`read_limit` up to 1 MiB, `retry_delay_ns` up to 1 minute, `timeout_ns`,
`rl_wait_max_ns` and `block_cooldown_ns` up to 10 minutes, `throttle_max_ns`
up to 24 hours. 0 or a missing
field means the default; a negative or too large value is answered with 400.
`cache_dir` cannot be set through the API: the page cache directory is set
for the whole panel with `-cache-dir`. `trace` (true/false) writes the
worker's trace, `trace_bodies` (`anomalies`, `all`, `none`) chooses the
pages it keeps.

```bash
TOKEN=$(./frgmnt token -data ./data -name script 2>/dev/null)
A="Authorization: Bearer $TOKEN"
curl -s -H "$A" -X POST localhost:8080/api/workers -d '{"name":"w1","config":{"workers":100,"retries":1}}'
curl -s -H "$A" -X POST localhost:8080/api/workers/<id>/input -d '{"text":"durov, @pavel\nwallet"}'
curl -s -H "$A" -X POST localhost:8080/api/workers/<id>/start
```

### Data layout

```
data/
  data.db          # SQLite (WAL): workers, inputs with checked flags, results
  settings.json    # global settings, editable by hand
  auth.key         # the key that signs access tokens (mode 0600); replace it with "frgmnt token -rotate"
  pagecache/       # gzipped Fragment pages + JSON metadata, two-level shards
  traces/          # <id>.jsonl.gz (+ <id>.1.jsonl.gz): the traces of workers with "Write a trace" (mode 0600)
```

## Large lists and load

Both modes run a list through the same pipeline; nothing waits for a batch
to finish, and memory does not grow with the length of the list.

```
file --64 KB chunks--> names --> dedup, -resume skip --> queue (4,096) --> N workers --> queue (4,096) --> writer
                                 (compact NameSet)                         gate, pause, -rate             file and screen
```

1. **Reading.** The list is read in 64 KB chunks and split as it comes
   (new lines, spaces, commas, semicolons; `|` ends the names of a line); it
   is never loaded whole. About 250 MB/s.
2. **Dedup and `-resume`.** A name of up to 12 characters is kept as an
   exact 8-byte key, a longer one as a string: about 68 MB per million unique
   names. `-no-dedup` keeps memory flat.
3. **Queue.** The reader stays at most 4,096 names ahead of the workers and
   waits when they are slower. The console counts the file first, so the
   progress line shows the real size of the run and an ETA.
4. **Workers.** N goroutines take names one by one, each as soon as it is
   free. A name is checked against the Telegram rules and the page cache
   without a request; a request then waits for the gate (one at a time until
   the first answer, and again after a throttle pause), for the shared pause
   and for its `-rate` start. `-rate` counts requests, retries included, and
   spaces them evenly.
5. **Writer.** One goroutine writes the results file and the screen as
   results arrive (at most ten flushes a second under a fast stream).

In the panel an uploaded list is parsed while it arrives into a compact
list, then stored in one transaction; a run reads it from the database 500
names at a time into a 1,024-name queue, and results are saved in batches
of 32 rows or every 250 ms. When the database is busy (another tile stores a
big list) the writer keeps its rows and tries again, and the checks wait
for it instead of losing results.

Measured on a VM with one CPU and 1 GB of RAM:

| | |
| --- | --- |
| 1,000,000 names in the console, no network | 1.6 s and about 80 MB with dedup, 1.3 s and about 16 MB with `-no-dedup` |
| 500,000 shuffled names uploaded to the panel | 12.6 s, 89 MB at the peak |
| results saved by the panel | about 18,000 rows/s |
| panel database | about 140 MB per million names in the lists, plus the results |

The speed of a run is set by Fragment, not by the program: at `-rate 3` a
million names take almost four days. Workers beyond `rate × answer time × 2`
(5 workers for `-rate 5` and half-second answers) only wait for starts. For
lists of several million names use `-no-dedup` in the console; in the panel
the database is the limit on a small disk.

## Page cache

Fetched pages are stored gzipped under `.fragment_cache/` (console) or
`<data>/pagecache/` (web) and re-parsed with the current parser on every hit,
so parser fixes apply to old entries.

| Status | TTL |
| ------ | --- |
| sold | 7 days (`-cache-sold-ttl`, negative = forever) |
| taken | 6 hours |
| reserved | 24 hours |
| auction, available | 2 minutes (`-cache-ttl`) |
| free, unknown, error | not cached |

When Fragment cannot be reached, a stale **sold** or **taken** entry is served
and marked as such (`network failed (...); served stale cache`). Stale auction
or sale pages are not served: those are likely over.

## Debug, trace and profiling

Three tools, each independent of the others:

| What | Console | Panel |
| ---- | ------- | ----- |
| One line per HTTP try (outcome, waits, DNS/TCP/TLS/first byte/body, connection) and timing figures at the end | `-debug` | log level `debug` (the Logs tab), with every request to the panel itself |
| The structured trace: every request and answer, every engine decision, the connections, the process | `-trace run.jsonl.gz` | "Write a trace" in the worker's settings, **Download trace** in its details |
| Profiles: CPU, heap, goroutines, blocking, mutexes, the execution trace | `-pprof`, `-cpuprofile`, `-memprofile`, `-exectrace` | `frgmnt web -pprof` |

### The trace

JSON Lines, gzip when the file ends in `.gz` (about 11 times smaller; read it with `zcat`
or `gzip -dc`). Every line has `seq` (numbered from 1 for each run, across the file's segments: a gap
means lost lines; a `-resume` run appended to the file starts at 1 again), `ts` and `kind`. Events of one check of a name share `nid`,
every HTTP request has its `aid`, every connection its `cid`, and `w` is the worker
that did it.

| kind | What it records |
| ---- | --------------- |
| `run_start` | version, `schema` (2), `env` (Go version, OS, CPUs, PID, VCS revision), `config` (the whole effective config), the arguments or the panel's worker and its last list |
| `input`, `input_skip` | the list as read (bytes, queued, duplicates, `-resume` skips); the first 1,000 names left out and why |
| `cache` | `hit`, `stale`, `miss` or `error` (a broken entry, and why), with its age and read time |
| `attempt` | one HTTP try: `method`, `req_headers` as sent, the waits before it (`wait_gate_ms`, `wait_pause_ms`, `wait_rate_ms`, `conn_wait_ms`, `retry_delay_ms`, `probe_wait_ms`) and `retry_reason`; `dns_ms`/`dns_addrs`, `connect_ms`, `tls_ms`, `wrote_headers_ms`, `ttfb_ms`, `body_ms`, `net_ms`, `work_ms`; `status_line`, every `resp_headers`, `content_length`, `trailers`; `bytes` read for the parser, `body_raw_bytes` received, `wire_in`/`wire_out` on HTTP/1.x; `parse_us`, `cache_write_ms`; the verdict with `confidence` and `evidence`; `body` (or `body_b64`) when kept |
| `probe` | the throttle probe, like an attempt, with `healthy`; `reused` when another worker's probe answered |
| `result` | one name: status, `attempts`, `took_ms` = `net_ms` + `wait_ms` + `work_ms`, `conn_wait_ms` |
| `conn_open`, `conn_tls`, `conn_close`, `conn_error` | a connection: dial time and addresses; TLS version, cipher, ALPN, resumption, certificate; lifetime, `bytes_in`/`bytes_out` on the wire, `requests` |
| `gate`, `pause`, `throttle_over`, `blocked`, `stop` | the engine's decisions and why the run ended |
| `pipeline` (every second) | queues, what the workers do, the writer's and the trace's counters, `runtime` (goroutines, heap, GC pauses, scheduling latency p50/p99, CPU, RSS, open files) |
| `db_flush` (panel) | every write of results to SQLite: rows, time, errors |
| `segment`, `trace_stopped` | the trace kept its tail (the start was dropped to stay under `-trace-max`), or stopped on a low disk |
| `run_end` | every counter, the output written, the time the end took, the runtime |

Pages: `-trace-bodies anomalies` (the default) keeps the whole decoded page of unknown
answers, challenges, unexpected codes and errors, up to 1 MB; `all` keeps every page.
Cookie values never appear (names only), and access tokens are never logged.

Size and cost, measured against a local stand-in with real Fragment pages and headers:

| | per name, plain | per name, `.gz` | CPU of the checker |
| - | --------------- | --------------- | ------------------ |
| no trace | | | +1% over 2.1.0 |
| free names | 1.7 KB | 157 B | +35% |
| pages (16 KB each) | 2.0 KB | 200 B | +35% |
| pages, `-trace-bodies all` | 19 KB | 3 KB | +100% |

A million names make about 160–200 MB of `.gz` trace without pages. The console keeps
the last `-trace-max` (1 GB), the panel the last 100 MB per worker, and a trace stops
when less than 256 MB are free on its disk.

```bash
T=run.jsonl.gz
zcat $T | jq -c 'select(.kind=="result") | [.took_ms, .name, .net_ms, .wait_ms, .work_ms, .attempts]' | sort -t'[' -k2 -g -r | head  # slowest names
zcat $T | jq -n '[inputs | select(.kind=="result")] | {net: (map(.net_ms // 0)|add), wait: (map(.wait_ms // 0)|add), work: (map(.work_ms // 0)|add)}'  # where the time went
zcat $T | jq -c 'select(.kind=="conn_close") | {cid, requests, bytes_in, lifetime_ms}'  # the connections
zcat $T | jq -r 'select(.kind=="attempt" and .retry_reason) | .retry_reason' | sort | uniq -c   # why tries were repeated
zcat $T | jq -c 'select(.kind=="pause" or .kind=="throttle_over" or .kind=="blocked" or .kind=="stop")'  # throttling and the end
zcat $T | jq -c 'select(.nid==42)'                                     # everything about one check
zcat $T | jq -r 'select(.name=="durov" and .body) | .body' > durov.html  # a kept page
zcat $T | jq -c 'select(.kind=="pipeline") | .info.runtime | {goroutines, heap_bytes, sched_lat_p99_ms}'
```

### Profiling

```bash
./frgmnt check -input names.txt -output r.txt -pprof 127.0.0.1:6060 &
go tool pprof -top http://127.0.0.1:6060/debug/pprof/profile?seconds=30     # CPU
go tool pprof -tagfocus component=check-worker http://127.0.0.1:6060/debug/pprof/profile?seconds=30
go tool pprof http://127.0.0.1:6060/debug/pprof/heap                        # memory (-sample_index=alloc_space)
go tool pprof http://127.0.0.1:6060/debug/pprof/block                       # where goroutines wait
./frgmnt check -input names.txt -output r.txt -cpuprofile cpu.pprof -memprofile mem.pprof -exectrace exec.trace
go tool trace exec.trace    # "User-defined tasks": one "check" per name, with its regions
curl -H "Authorization: Bearer $TOKEN" -o cpu.pprof "https://panel/debug/pprof/profile?seconds=30"  # panel (frgmnt web -pprof)
```

Labels: `component` = `check-worker`, `producer`, `writer`, `progress`, `persister`,
`trace-writer`, `http-api`; `tile` = the worker id in the panel. `-pprof` also turns on
the block profile (one event per millisecond blocked) and the mutex profile (1 in 100).
pprof shows the command line, every goroutine's stack and the memory: `-pprof` warns when
its address is not on this machine, and the panel serves it behind its login.

### Every byte on the wire

The trace has the requests and answers as the program sees them. The bytes on the
wire (TLS records, HTTP/2 frames) need a capture and the session keys:

```bash
sudo tcpdump -i any -w fragment.pcap 'tcp port 443' &
./frgmnt check -input names.txt -output r.txt -tls-keylog keys.log
tshark -r fragment.pcap -o tls.keylog_file:keys.log -Y http2   # or Wireshark: TLS → (Pre)-Master-Secret log filename
GODEBUG=http2debug=2 ./frgmnt check ...                        # the HTTP/2 frames on stderr, very verbose
```

`keys.log` decrypts the whole capture: keep it private and delete it after use.

## Build and tests

```bash
make unified      # static ./frgmnt (check + web)
make release      # + the web-only ./fragment_checker_web (cmd/server)
make test         # go test ./...
make test-race    # go test -race -count=1 ./...
make test-suite   # black-box suite: go test ./tests/... -v
make vet
make clean        # removes the binaries only; data/ is never touched
make clean-cache  # drops .fragment_cache and data/pagecache
```

- [`tests/`](tests/README.md): black-box tests through the exported API
  (status layouts, redirects and throttling, page cache, a console run)
  against local fake Fragment servers, no network.
- `internal/*/*_test.go`: white-box tests, including one regression test per
  bug fixed after the import (`regression_test.go`, `manager_test.go`,
  `server_test.go`).
- [CI](.github/workflows/ci.yml) on the two Go releases that get security fixes
  (`stable` and `oldstable`): gofmt, vet, race tests, the black-box suite,
  govulncheck and the release build.

## Project layout

```
cmd/frgmnt/          unified binary: check | web | version
cmd/server/          web-only entry point (fragment_checker_web)
internal/cli/        console mode: flags, input, -resume, -filter, results file, summary
internal/webapp/     web mode bootstrap and shutdown, the token command
internal/auth/       access tokens: the key in <data>/auth.key, issuing and checking JWTs
internal/checker/    the engine of both modes: HTTP, retries, cooldown, parser, page cache
internal/worker/     worker tiles: lifecycle, input producer, result writer
internal/server/     REST + SSE, request guard
internal/store/      SQLite schema, migrations, settings.json
internal/logbus/     in-memory log ring and SSE fan-out
internal/version/    the one version number of every mode
internal/staticfs/   embedded panel (index.html, styles.css, app.js)
tests/               black-box suite and HTML fixtures
```

## Contributors

<a href="https://github.com/cyberanchor/frgmnt/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=cyberanchor/frgmnt" />
</a>

<!--
## Star History

<a href="https://www.star-history.com/#cyberanchor/frgmnt&type=date&legend=bottom-right">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=cyberanchor/frgmnt&type=date&theme=dark&legend=bottom-right" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=cyberanchor/frgmnt&type=date&legend=bottom-right" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=cyberanchor/frgmnt&type=date&legend=bottom-right" />
  </picture>
</a>
-->

## License

[MIT](LICENSE)
