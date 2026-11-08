<div align="center">

<h1> frgmnt </h1>

<sub>Fragment.com username availability checker. One static Go binary with two modes: a console checker for big lists and a web panel with worker tiles, resume and live progress. Every verdict carries a confidence score and the evidence behind it.</sub>

<kbd>🔎 Usernames</kbd> <kbd>⚡ Concurrent</kbd> <kbd>⏯️ Resume</kbd> <kbd>🗄️ Page cache</kbd> <kbd>🖥️ Web panel</kbd> <kbd>⌨️ Console</kbd>

![GitHub Stars](https://img.shields.io/github/stars/cyberanchor/frgmnt?color=green&style=social)
![GitHub Forks](https://img.shields.io/github/forks/cyberanchor/frgmnt?color=green&style=social)
![GitHub Issues](https://img.shields.io/github/issues/cyberanchor/frgmnt?color=green&style=flat-square)
![GitHub Pull Requests](https://img.shields.io/github/issues-pr/cyberanchor/frgmnt?color=green&style=flat-square)
![GitHub Contributors](https://img.shields.io/github/contributors/cyberanchor/frgmnt?color=green&style=flat-square)

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go&logoColor=white&style=flat-square)](https://go.dev/)
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

# console: one name per line in, "@name | status" lines out
./frgmnt check -input names.txt -output results.txt -workers 200

# web panel on http://127.0.0.1:8080
./frgmnt web
```

Go **1.21+**, no CGO, no system SQLite. `./frgmnt version` prints
`frgmnt 1.0.0 (cli 3.1, web 0.3.0)`; `./frgmnt help`, `./frgmnt check -h` and
`./frgmnt web -h` list every flag. The old shorthands still work:
`./frgmnt -input ...` runs `check`, `./frgmnt -addr ...` runs `web`.

## Statuses

| Status       | Meaning                                                  | Signal on the page                     |
| ------------ | -------------------------------------------------------- | -------------------------------------- |
| 🟢 free      | Nobody owns it, it can be claimed                        | redirect to `/?query=<name>`           |
| 🟣 reserved  | Reserved by Fragment (a bare `302 /` that survives retries and a healthy probe) | redirect to `/`  |
| 🟡 auction   | Live auction                                             | `On auction`, `js-place-bid-btn`, timer |
| 🔵 available | Fixed-price sale                                         | `For sale`, `js-buy-now-btn`           |
| ⚪ taken     | Bound to an account, offers only                         | `Taken`, `js-make-offer-btn`           |
| ⚫ sold      | Bought on Fragment                                       | `Sold`, `Purchased on`                 |
| ❔ unknown   | The page matched no known layout                         |                                        |
| ❌ error     | Network, HTTP or validation error, Cloudflare challenge, throttling |                             |

The parser trusts the visible status label first (`On auction`, `For sale`,
`Sold`, `Taken`), then the JS hooks and "Purchased on", and only then the
`tm-status-*` class. Every result has a confidence score (0–100) and an
evidence chain such as `text="for sale"; class=avail; timer; buy-now`.

- **Several badges**: a header can show `BANNED` and `Sold`; the commercial badge wins.
- **Auction with buy-now**: `On auction` still wins, it is not a conflict.
- **`302 /` is ambiguous**: Fragment uses it for rare reserved names and for
  throttled clients. All workers pause for a shared cooldown, the request is
  retried with the same cookie session, and a random free name is probed.
  Only a healthy probe gives `reserved`; otherwise the result is an error,
  never a false `reserved`.
- **Listings lag**: search pages can still show a name that was delisted. The
  username page is the source of truth.

## Console mode

```bash
./frgmnt check -input names.txt -output results.txt                  # 500 workers, 8 connections
./frgmnt check -input names.txt -output results.txt -resume          # skip names already in results.txt
cat names.txt | ./frgmnt check -input - -output free.txt -filter free -quiet
```

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `-input` | | Name list, one per line (`-` = stdin); a leading `@` is stripped |
| `-output` | | Results file (truncated unless `-append` or `-resume`) |
| `-workers` | `500` | Parallel workers |
| `-conns` | `8` | TCP connections to fragment.com (HTTP/2 multiplexing) |
| `-rate` | `0` | Global limit, requests per second (0 = unlimited) |
| `-retries` / `-retry-delay` | `2` / `500ms` | Extra attempts per name and the pause between them |
| `-rl-wait-max` | `30s` | Longest `Retry-After` honoured on HTTP 429 |
| `-timeout` | `15s` | Whole-request timeout |
| `-readlimit` | `16384` | Body bytes scanned for the status (1 KiB–1 MiB) |
| `-cache-dir` / `-no-cache` | `.fragment_cache` | Page cache directory, or no cache |
| `-cache-ttl` / `-cache-sold-ttl` | `2m` / `168h` | TTL of auction/sale pages and of sold pages |
| `-filter` | | Write only these statuses, e.g. `free,auction` |
| `-append` / `-resume` | | Append to the output; `-resume` also skips names already in it |
| `-no-dedup` | | Do not deduplicate the input (saves RAM on huge files) |
| `-quiet` / `-debug` / `-trace` | | Less or more output on stdout and stderr |
| `-insecure` | | Skip TLS verification (debugging only) |
| `-base-url` | | Another Fragment base URL (tests, mirrors) |

Ctrl+C stops gracefully: finished results are flushed, checks that were cut
short are not written, so `-resume` checks them again. The exit code is 1 when
the results file could not be written.

## Web panel

```bash
./frgmnt web                                   # http://127.0.0.1:8080, data in ./data
./frgmnt web -addr 127.0.0.1:9090 -data ./var -log-level debug
```

- **Worker tiles**: independent checker sessions, each with its own config,
  input list, progress bar and status breakdown. Start runs from scratch,
  Resume continues from the first unchecked name, Start all / Stop all act on
  every tile.
- **Input**: paste names or upload a file; commas, spaces, tabs, semicolons and
  new lines all separate names, a leading `@` is stripped.
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
| `-log-level` | `info` | `error`, `warn`, `info`, `debug`, `trace` |
| `-cache-dir` / `-no-cache` | `<data>/pagecache` | Page cache directory, or no cache |
| `-cache-ttl` / `-cache-sold-ttl` | `2m` / `168h` | TTL of auction/sale pages and of sold pages |
| `-version` | | Print the version |

The panel has no login. It refuses state-changing requests that a browser
marks as coming from another site (`Origin`, `Sec-Fetch-Site`), and while it
listens on a loopback address it only answers to a loopback `Host`
(`127.0.0.1`, `localhost`, `::1`, any port, so `ssh -L` works). Bind it to
another address only on a network you trust.

### REST API

| Method | Path | Description |
| ------ | ---- | ----------- |
| GET | `/api/health` | Liveness, worker count, log level |
| GET / PUT | `/api/settings` | Global settings |
| GET / POST | `/api/workers` | List / create workers (`{"name": ..., "config": {...}}`) |
| GET / PATCH / DELETE | `/api/workers/{id}` | One worker; PATCH changes only the fields it sends |
| POST | `/api/workers/{id}/input` | Load names from `{"text": "..."}` |
| POST | `/api/workers/{id}/input/upload` | Multipart upload, field `file` |
| POST | `/api/workers/{id}/start` · `/stop` · `/clear` | Run (`?resume=true` keeps results), cancel, clear results |
| POST | `/api/workers-bulk/start` · `/stop` | Start every ready/done/error worker, stop every running one |
| GET | `/api/workers/{id}/results` | `?after=ID&limit=N`, with counts per status |
| GET | `/api/workers/{id}/results/download` | Plain-text results |
| GET | `/api/workers/{id}/events` | SSE: progress and log of one worker |
| GET | `/api/logs` · `/api/logs/stream` | Recent log entries (`?topic=ID&limit=N`), SSE live tail |

Bodies are JSON (50 MB cap; uploads 100 MB). Config limits: `workers` and
`max_conns` up to 1000, `rate_rps` up to 1000, `retries` up to 10,
`read_limit` up to 1 MiB; anything else is answered with 400.

```bash
curl -s -X POST localhost:8080/api/workers -d '{"name":"w1","config":{"workers":100,"retries":1}}'
curl -s -X POST localhost:8080/api/workers/<id>/input -d '{"text":"durov, @pavel\nwallet"}'
curl -s -X POST localhost:8080/api/workers/<id>/start
```

### Data layout

```
data/
  data.db          # SQLite (WAL): workers, inputs with checked flags, results
  settings.json    # global settings, editable by hand
  pagecache/       # gzipped Fragment pages + JSON metadata, two-level shards
```

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

## Build and tests

```bash
make unified      # static ./frgmnt (check + web)
make release      # + the web-only ./fragment_checker_web (cmd/server)
make test         # go test ./...
make test-race    # go test -race -count=1 ./...
make test-suite   # black-box suite: go test ./tests/... -v
make vet
```

- [`tests/`](tests/README.md): black-box tests through the exported API
  (status layouts, redirects and throttling, page cache, a console run)
  against local fake Fragment servers, no network.
- `internal/*/*_test.go`: white-box tests, including one regression test per
  bug fixed after the import (`regression_test.go`, `manager_test.go`,
  `server_test.go`).
- [CI](.github/workflows/ci.yml) on Go 1.21 and 1.22: gofmt, vet, race tests,
  the black-box suite and the release build.

## Project layout

```
cmd/frgmnt/          unified binary: check | web | version
cmd/server/          web-only entry point (fragment_checker_web)
internal/cli/        console checker: flags, worker pool, parser, page cache
internal/webapp/     web mode bootstrap and shutdown
internal/checker/    web engine: HTTP, retries, cooldown, parser, page cache
internal/worker/     worker tiles: lifecycle, input producer, result writer
internal/server/     REST + SSE, request guard
internal/store/      SQLite schema, migrations, settings.json
internal/logbus/     in-memory log ring and SSE fan-out
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
