# fragment_checker_web

A single-binary, web-panel fork of [`fragment_checker`](../fragment_checker_v3.1)
for checking Fragment.com username availability. Bundles a static frontend,
a small REST + Server-Sent Events backend, and a local SQLite store into
one statically-linked Go executable.

```
┌──────────────────────────────────────────────────────────────────┐
│   browser ──HTTP──▶  fragment_checker_web  ──HTTPS──▶  fragment  │
│                          │                                       │
│                          ├─ /api/*       REST                    │
│                          ├─ /api/.../events   SSE                │
│                          ├─ embedded HTML/CSS/JS                 │
│                          ├─ SQLite (data.db, WAL)                │
│                          └─ settings.json                        │
└──────────────────────────────────────────────────────────────────┘
```

## Features

- **One static binary** — `frgmnt` bundles the console checker (`frgmnt
  check`) and the web panel (`frgmnt web`); no CGO, no runtime assets.
- **Embedded web panel** — HTML/CSS/JS shipped via `embed.FS`.
- **Worker tiles** — independent checker sessions, per-worker config and
  results. Configurable count and parameters from the UI.
- **Start vs Resume** — fresh runs wipe previous results; Resume continues
  from the first unchecked name and keeps everything in place.
- **Bulk Start / Stop** — one click on the Workers tab to fan out across
  every idle / running worker.
- **Live progress via SSE** — no polling, no WebSocket dependency.
- **Document-title progress** — `▶ N · P% — ...` while runs are active so
  background tabs surface progress without focus.
- **Browser notifications** on worker completion (opt-in by browser).
- **Day / night theme** — CSS variables, toggled in the top-right. The
  toggle is authoritative; settings.json provides only the initial seed.
- **File upload or textarea** for username lists. Both parse the same
  permissive separators (`\n`, comma, space, tab, semicolon) and strip a
  leading `@`.
- **Result filter chips** — click a status to narrow the result list.
- **Page cache** — Fragment HTML is cached under `data/pagecache/`. Bought
  (`sold`) pages are reused for 7 days, `taken` for 6 hours, and
  auction/sale pages for 2 minutes. Cache hits are re-parsed with the
  current parser, so detection improvements apply retroactively.
- **Confidence + evidence** — every result carries a 0–100 confidence
  score and the signal chain that decided it (e.g.
  `text="for sale"; class=avail; timer; buy-now`), exposed via the REST API
  and the result list.
- **Debug console** — small REPL that hits the REST API from the browser.
- **Logs tab** with topic filter, pause and live tail.
- **CLI flags** for address, data dir and log level.
- **Persistent state** — workers, inputs, marked-checked flags and results
  survive restarts.
- **Tested** — a separate black-box suite in `tests/` (parser fixtures,
  redirects/throttling, page cache, CLI end-to-end) plus white-box tests for
  internals; `go test -race ./...` is clean.
- **One third-party dependency** — `modernc.org/sqlite` (pure-Go, no CGO).

## Requirements

- Go 1.21+
- No system SQLite or libc dependencies (CGO is off by default).

## Build

The recommended build is the unified binary with `check` and `web` modes:

```bash
# development
go build -o frgmnt ./cmd/frgmnt

# release (stripped, reproducible, static)
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o frgmnt ./cmd/frgmnt
```

The web-only entry point remains available:

```bash
go build -o fragment_checker_web ./cmd/server
```

A `Makefile` provides shortcuts:

```bash
make unified     # unified `frgmnt` binary (check + web)
make build       # web-only debug build
make release     # unified stripped release build
make run         # build + run web on 127.0.0.1:8080
make vet         # go vet ./...
make test        # go test ./...
make test-suite  # separate black-box suite: go test ./tests/... -v
make test-race   # go test -race -count=1 ./...
make tidy        # go mod tidy
make clean
```

## Run

```bash
./frgmnt web                                                  # default :8080
./frgmnt web -addr 127.0.0.1:9090 -data ./var
./frgmnt web -log-level debug
./frgmnt web -version

# web-only binary, same flags
./fragment_checker_web -addr 127.0.0.1:8080 -data ./data
```

The console mode lives in the same binary:

```bash
./frgmnt check -input names.txt -output results.txt -workers 200
```

Then open the printed URL (default <http://127.0.0.1:8080>) in your browser.

## CLI flags

| Flag         | Default            | Description                                 |
| ------------ | ------------------ | ------------------------------------------- |
| `-addr`      | `127.0.0.1:8080`   | HTTP listen address                         |
| `-data`      | `./data`           | Directory for SQLite DB and `settings.json` |
| `-log-level` | `info`             | `error`, `warn`, `info`, `debug`, `trace`   |
| `-cache-dir` | `<data>/pagecache` | On-disk page cache directory                |
| `-cache-ttl` | `2m`               | TTL for volatile pages (auction/available)  |
| `-cache-sold-ttl` | `168h`        | TTL for sold/bought pages; negative = never |
| `-no-cache`  | `false`            | Disable the page cache                      |
| `-version`   | `false`            | Print version and exit                      |

Bind to `0.0.0.0` only if you understand the exposure — this is a local tool
and has no auth layer.

## Web panel

- **Workers** — grid of tiles. Each tile has Start / Stop / Input·Results /
  Edit / Delete buttons, a progress bar and a status-breakdown panel.
- **Settings** — global defaults (workers, rate, retries, timeout), theme,
  log level, UI history limit. Saved to `data/settings.json`.
- **Logs** — live SSE tail of every server event, filterable by worker.
- **Debug Console** — type raw API calls (e.g. `GET /api/workers`) and see
  the JSON response inline. Includes an expandable command crib-sheet.

## REST API

| Method | Path                                       | Description                       |
| ------ | ------------------------------------------ | --------------------------------- |
| GET    | `/api/health`                              | Liveness + counters               |
| GET    | `/api/settings`                            | Global settings                   |
| PUT    | `/api/settings`                            | Replace global settings           |
| GET    | `/api/workers`                             | List workers                     |
| POST   | `/api/workers`                             | Create worker                    |
| GET    | `/api/workers/{id}`                        | Worker details                   |
| PATCH  | `/api/workers/{id}`                        | Update name/config               |
| DELETE | `/api/workers/{id}`                        | Delete worker + its data         |
| POST   | `/api/workers/{id}/input`                  | Load names from `{"text":"..."}` |
| POST   | `/api/workers/{id}/input/upload`           | Multipart upload (`file` field)  |
| POST   | `/api/workers/{id}/start`                  | Start the run (`?resume=true` keeps previous results) |
| POST   | `/api/workers/{id}/stop`                   | Cancel the run                   |
| POST   | `/api/workers-bulk/start`                  | Start every ready/done/error worker (`?resume=true`)  |
| POST   | `/api/workers-bulk/stop`                   | Stop every running worker        |
| POST   | `/api/workers/{id}/clear`                  | Clear results                    |
| GET    | `/api/workers/{id}/results`                | `?after=ID&limit=N`              |
| GET    | `/api/workers/{id}/results/download`       | Plain-text results download      |
| GET    | `/api/workers/{id}/events`                 | SSE: per-worker progress + logs  |
| GET    | `/api/logs`                                | Recent logs (`?topic=ID&limit=N`)|
| GET    | `/api/logs/stream`                         | SSE: live log tail               |

All POST/PUT/PATCH bodies are JSON. Multipart upload is only used by
`/input/upload`. Request bodies are capped at 50 MB JSON / 100 MB upload.
Each result object also carries `confidence` (0–100), `evidence` (the signal
chain) and `cached` (served from the page cache).

## Data layout

```
data/
  data.db          # SQLite database (workers, inputs, results)
  data.db-wal      # WAL files (managed by SQLite)
  data.db-shm
  settings.json    # editable by hand if needed
  pagecache/       # gzipped Fragment HTML + JSON metadata, two-level shard
```

`settings.json`:

```json
{
  "theme": "dark",
  "log_level": "info",
  "default_workers": 50,
  "default_rate_rps": 0,
  "default_retries": 2,
  "default_timeout_s": 15,
  "history_limit_max": 500
}
```

## Status taxonomy

Identical to v3.1, see [the original README](../fragment_checker_v3.1/README.md)
for full details:

| Status    | Meaning                                         |
| --------- | ----------------------------------------------- |
| free      | Available to claim                              |
| reserved  | Reserved (Telegram-style redirect)              |
| auction   | Live auction ("On auction" + place-bid + timer) |
| available | Fixed-price listing ("For sale" + buy-now)      |
| taken     | Bound to an account ("Taken" + make-offer)      |
| sold      | Purchased on Fragment ("Sold" + "Purchased on") |
| unknown   | HTML did not match any pattern                  |
| error     | Network / HTTP / validation                     |

Detection notes:

- Multiple header badges are supported. A sold name can render
  `BANNED` + `Sold`; the commercial badge wins.
- An auction can also offer buy-now; `On auction` still wins.
- A bare `302 /` is retried with a shared cooldown and a cookie session.
  A random free-name probe distinguishes throttling from a genuinely
  reserved name, so rate limits never masquerade as `reserved`.
- Search/listing pages can lag behind the username page; results are based
  on `https://fragment.com/username/<name>`.

## Project layout

```
.
├── cmd/frgmnt/main.go                  # unified dispatcher: check | web
├── cmd/server/main.go                  # web-only entry point (wrapper)
├── internal/cli/cli.go                 # console checker (package cli)
├── internal/cli/cache.go               # console page cache
├── internal/webapp/webapp.go           # web panel bootstrap (package webapp)
├── tests/                              # separate black-box unit suite
│   ├── README.md                       # how to run, what is covered
│   ├── parser_test.go                  # status layouts + confidence
│   ├── redirect_test.go                # free / throttle / reserved
│   ├── cache_test.go                   # hit, TTL, stale fallback
│   ├── cli_test.go                     # end-to-end console run
│   └── testdata/                       # minimal Fragment HTML fixtures
├── internal/checker/checker.go         # checker core (library)
├── internal/checker/parse.go           # multi-signal status parser
├── internal/checker/cache.go           # on-disk page cache
├── internal/checker/cache_test.go      # cache internals (white-box)
├── internal/checker/checker_test.go    # helper tests (white-box)
├── internal/logbus/logbus.go           # event bus + SSE fan-out
├── internal/logbus/logbus_test.go      # ring buffer + close-race tests
├── internal/store/store.go             # SQLite + settings
├── internal/store/store_test.go        # CRUD + resume helpers
├── internal/worker/manager.go          # per-tile lifecycle
├── internal/server/server.go           # HTTP, REST, SSE
├── internal/staticfs/                  # embedded UI bundle
│   ├── staticfs.go
│   └── web/
│       ├── index.html
│       ├── styles.css
│       └── app.js
├── .github/workflows/ci.yml            # vet + race tests + release build
├── go.mod
└── README.md
```

## Tests

The primary suite is deliberately separate from the code:

```bash
go test ./tests/... -v          # black-box: parser, redirects, cache, CLI
go test -race -count=1 ./...    # everything, with the race detector
```

`tests/` uses only exported APIs and local `httptest` servers — no network.
See [tests/README.md](tests/README.md) for the coverage table and fixture
rules. White-box tests for unexported internals stay in `internal/`.

## Changelog

### Detection hardening (post-1.0.0)
- Multi-badge headers: `BANNED` + `Sold` on the same page now resolve to
  `sold` (the commercial badge wins), with the full badge list in evidence.
- `On auction` + `js-buy-now-btn` is accepted as a normal auction instead of
  being treated as a conflicting signal.
- A bare `302 /` is retried with a shared cool-down and persistent cookies.
  A random free-name probe separates throttling from a genuine reserved
  name; a throttled client now gets an error, never a false `reserved`.

### Unified binary 1.0.0
- `cmd/frgmnt` provides both modes from one 11 MB static binary:
  `frgmnt check` (console) and `frgmnt web` (panel).
- Legacy shorthands still work: `frgmnt -input ...` routes to `check`, and
  `frgmnt -addr ...`/`-data ...` routes to `web`.
- The console checker moved from the standalone v3.1 source tree into
  `internal/cli`, sharing the parser/cache design with the web panel.
- `cmd/server` remains as a thin web-only wrapper.

### v0.3.0
- **Detection fix**: `For sale` pages (fixed-price listings) were reported as
  `auction` because they also carry `tm-countdown-timer`. The parser now
  trusts the visible label and the `js-buy-now-btn` / `js-place-bid-btn` /
  `js-make-offer-btn` hooks. Added confidence scores and an evidence chain.
- **Page cache**: gzipped HTML under `data/pagecache/`, with per-status TTLs.
  `sold` (bought) pages are the primary win: 7 days by default. Cache hits
  are re-parsed so parser fixes apply to old entries; a stale entry is used
  as a fallback if the network fails.
- Results API/SQLite gained `confidence`, `evidence` and `cached` columns
  (idempotent migration).
- `http.ServeFileFS` replaced with `fs.ReadFile`, restoring Go 1.21
  compatibility and a green `go vet`.

### v0.2.0
- Persister: final batch is now flushed on a detached context so Stop
  never loses the last ~32 results.
- logbus: `Subscribe`/`Close` race is closed; subscribing on a closed bus
  returns a pre-closed channel instead of leaking.
- `Worker.started_at` persisted across restarts.
- Theme: localStorage is authoritative; server only seeds it on first load.
- New: Start vs Resume, bulk Start/Stop, status filter chips, title-bar
  progress, completion notifications.
- Tests: unit tests for parsers, event bus and store; race-clean.
- CI: GitHub Actions on Go 1.21 + 1.22 — vet, race tests, release build.

### v0.1.0
- Initial release.


## License

MIT. See `LICENSE`.
