# Changelog

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
