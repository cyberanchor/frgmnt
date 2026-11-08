# Black-box unit tests

This directory is the dedicated test suite for **frgmnt**. It
lives outside `internal/` on purpose: the tests use only exported APIs, the
same surface the CLI and the web panel use.

No network access is required. Redirect and cache tests spin up a local
`httptest` server that mimics Fragment, and the CLI test points the real
binary path at it via `-base-url`.

## Run

```bash
cd frgmnt

# The separate suite
go test ./tests/... -v

# Same, with the race detector
go test -race -count=1 ./tests/...

# Everything (black-box + white-box)
go test ./...
```

## What is covered

| File | Behaviour |
| ---- | --------- |
| `parser_test.go` | Every status layout: auction, auction + buy-now, fixed-price sale, sold, taken, `BANNED` + `Sold`, unknown layout, Cloudflare challenge. Checks status, confidence and evidence. |
| `redirect_test.go` | `302 /?query=` → free; bare `302 /` retried with a cookie session; persistent redirect + blocked probe → **error, not reserved**; healthy probe → `reserved`. |
| `cache_test.go` | Cache hit avoids a second request; cache disabled; volatile TTL expiry; sold entry served as an explicit stale fallback when the network dies. |
| `cli_test.go` | End-to-end `cli.Main` run: flags, worker pool, parser and output file against a fake Fragment. |
| `web_test.go` | End-to-end `webapp.Main` run with `-base-url`: create a worker, load names, start, wait for done, check the tile counters and the download over HTTP, stop with SIGINT, find the results in the data directory. |
| `testdata/*.html` | Minimal real-world fixtures. Add a new fixture when Fragment introduces a layout. |

## Why some tests stay in `internal/`

A few behaviours are unexported implementation details (the on-disk cache
layout, CLI helpers, log-bus internals, the worker lifecycle, the HTTP guard).
Their white-box tests remain next to the code:

```
internal/checker/checker_test.go
internal/checker/cache_test.go
internal/checker/regression_test.go
internal/cli/regression_test.go
internal/logbus/logbus_test.go
internal/server/server_test.go
internal/store/store_test.go
internal/store/regression_test.go
internal/worker/manager_test.go
```

`regression_test.go`, `manager_test.go` and `server_test.go` hold one test
per bug fixed after the import into this repository; the comment above each
test describes the old behaviour, and `git log` has the matching fix.

The rule of thumb: if it can be tested through a public API, it belongs in
`tests/`; otherwise it stays in-package.

## Updating fixtures

`tests/testdata/*.html` are deliberately small but contain the exact classes
and JS hooks that the parser keys on:

- `tm-section-header-status` + `tm-status-*`
- `js-place-bid-btn`, `js-buy-now-btn`, `js-make-offer-btn`
- `tm-countdown-timer`, `Purchased on`

When a real page changes, save the relevant fragment, update the fixture and
add a row to `TestParseStatusFixtures`.
