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

# console: a list of names in, "@name | status" lines out
./frgmnt check -input names.txt -output results.txt -workers 200

# web panel on http://127.0.0.1:8080: create an access token, then open it
./frgmnt token -data ./data
./frgmnt web
```

Go **1.21+**, no CGO, no system SQLite. `./frgmnt version` prints
`frgmnt 1.0.0 (cli 3.1, web 0.3.0)`; `./frgmnt help`, `./frgmnt check -h` and
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
  throttled and all workers pause for a shared cooldown. The name is retried
  with the same cookie session, and the verdict takes a fresh probe: only a
  healthy probe gives `reserved`; otherwise the result is an error, never a
  false `reserved`. A reserved name does not slow the other workers down.
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
| `-rate` | `0` | Global limit, requests per second (0 = unlimited) |
| `-retries` / `-retry-delay` | `2` / `500ms` | Extra attempts per name and the pause between them |
| `-rl-wait-max` | `30s` | Longest `Retry-After` honoured on HTTP 429 |
| `-timeout` | `15s` | Whole-request timeout |
| `-readlimit` | `16384` | Body bytes scanned for the status (1 KiB–1 MiB) |
| `-cache-dir` / `-no-cache` | `.fragment_cache` | Page cache directory, or no cache |
| `-cache-ttl` / `-cache-sold-ttl` | `2m` / `168h` | TTL of auction/sale pages and of sold pages |
| `-filter` | | Write only these statuses, e.g. `free,auction`; every name with a verdict is also listed in `<output>.checked`, so `-resume` skips the ones the filter left out |
| `-append` / `-resume` | | Append to the output; `-resume` also skips names that already have a verdict in it (error lines are checked again) |
| `-no-dedup` | | Do not deduplicate the input (saves RAM on huge files) |
| `-quiet` / `-debug` / `-trace` | | Less or more output on stdout and stderr |
| `-insecure` | | Skip TLS verification (debugging only) |
| `-base-url` | | Another Fragment base URL (tests, mirrors) |

Ctrl+C stops gracefully: finished results are flushed, checks that were cut
short are not written, so `-resume` checks them again.

Exit codes: `0` done; `1` the results file could not be written, or every
checked name ended with an error; `2` bad flags (unknown `-filter` status,
`-input` and `-output` being the same file); `3` stopped because Fragment
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
| `-log-level` | `info` | `error`, `warn`, `info`, `debug`, `trace` |
| `-cache-dir` / `-no-cache` | `<data>/pagecache` | Page cache directory, or no cache |
| `-cache-ttl` / `-cache-sold-ttl` | `2m` / `168h` | TTL of auction/sale pages and of sold pages |
| `-allow-host` | | Host names the panel answers to besides `localhost`, comma-separated: the domain of a reverse proxy in front of it |
| `-tls-cert` / `-tls-key` | | PEM certificate (full chain) and key: serve HTTPS; the files are read again every 10 s, so renewals apply without a restart |
| `-base-url` | | Another Fragment base URL (tests, mirrors) |
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
| GET | `/api/logs` · `/api/logs/stream` | Recent log entries (`?topic=ID&limit=N`), SSE live tail |

Bodies are JSON (50 MB cap; uploads 100 MB). Config limits: `workers` and
`max_conns` up to 1000, `rate_rps` up to 1000, `retries` up to 10,
`read_limit` up to 1 MiB, `retry_delay_ns` up to 1 minute, `timeout_ns`,
`rl_wait_max_ns` and `block_cooldown_ns` up to 10 minutes. 0 or a missing
field means the default; a negative or too large value is answered with 400.
`cache_dir` cannot be set through the API: the page cache directory is set
for the whole panel with `-cache-dir`.

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
make clean        # removes the binaries only; data/ is never touched
make clean-cache  # drops .fragment_cache and data/pagecache
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
internal/cli/        console mode: flags, input, -resume, -filter, results file, summary
internal/webapp/     web mode bootstrap and shutdown, the token command
internal/auth/       access tokens: the key in <data>/auth.key, issuing and checking JWTs
internal/checker/    the engine of both modes: HTTP, retries, cooldown, parser, page cache
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
