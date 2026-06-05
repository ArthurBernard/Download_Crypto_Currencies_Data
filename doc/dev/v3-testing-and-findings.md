# v3 — Testing playbook & findings log

Capitalises on the v3 remediation pass: **what** was fixed, **how** the bugs
were found, and **reusable recipes** to keep finding them. Companion to
`RETROSPECTIVE-v3.md` (analysis) and `PLAN-v3-remediation.md` (the plan).

> Guiding principle that found most of these: **challenge every result on real
> data.** A green unit suite said nothing about a backfill that wrote 0 rows, a
> stored file that lost 58 % of trades, or a "Stop" button that did nothing.
> Run the real thing, inspect what landed on disk, compare it to what was asked.

---

## 1. Testing layers (how to run each)

| Layer | Command | Catches |
|-------|---------|---------|
| **Unit** | `pytest` | logic, schema, regressions (188 tests, network excluded) |
| **Network E2E** | `pytest -m network` | real exchange APIs: pagination drains, symbol mapping, OHLC round-trip |
| **Type** | `mypy dccd/` | strict on `domain/`, lenient glue (Success = green) |
| **Lint** | `ruff check dccd/` | style, dead imports |
| **API E2E** | run `dccd ui`, drive with `curl` | every route, status codes, real backfills/streams |
| **UI E2E** | Playwright (headless Chromium) | JS execution, console errors, interactive flows, visual |
| **Docs** | `cd doc && make html` | autodoc breakage (must be 0 warnings) |

### API E2E recipe (curl)
Run an **isolated** instance (temp config + temp `data_path`, a free port) so you
never touch real data. Then hit every route and *challenge the result*:

```bash
dccd ui -c /tmp/t/config.yml &            # isolated config, data_path=/tmp/t/data
B=http://127.0.0.1:8137
curl -s "$B/api/inventory"                 # GET routes
curl -s -X POST "$B/api/backfill" -H 'Content-Type: application/json' \
     -d '{"exchange":"binance","symbol":"BTC/USDT","data_type":"ohlc","span":3600,"start":"2026-05-28"}'
# then CHALLENGE: read the parquet, check the time range matches the request
python -c "import polars as pl; df=pl.read_parquet('.../2026.parquet'); print(df['TS'].min(), len(df))"
```
Error cases matter as much as happy paths: assert 400/404/422 on bad symbol,
missing span, unknown run, unknown stream, invalid config.

### UI E2E recipe (Playwright)
```bash
pip install playwright && playwright install chromium
```
A good UI audit captures **all four** failure channels and walks every control:

```python
page.on("console",     lambda m: errs.append((m.type, m.text)) if m.type in ("error","warning") else None)
page.on("pageerror",   lambda e: page_errs.append(str(e)))          # uncaught JS
page.on("response",    lambda r: net.append(r.status) if r.status >= 400 else None)  # 4xx/5xx
page.on("requestfailed", lambda r: req_fail.append(r.url))          # aborted/failed
```
Notes learned the hard way:
- Use `wait_until="domcontentloaded"` **not** `networkidle` — the dashboard
  polls and the logs page holds an SSE connection, so the network never goes idle.
- A `GET /api/events :: net::ERR_ABORTED` on navigation is **benign** (the browser
  closing the SSE EventSource), not a bug.
- Don't just read `inner_text` early — wait ~2 s for XHRs, then assert the
  dynamic containers (`#inv-content`, `#runs-body`) actually populated.
- **Look at the screenshots.** Several issues (stale labels, layout) are only
  visible, not in the DOM text.

### Before any mutating action on real data
Back up first. The real v2→v3 migration was run only after
`cp -a /home/arthur/data/crypto crypto.backup-pre-v3-...`, then verified
row-for-row against the backup (zero loss) before trusting it.

---

## 2. Findings log (every bug, and how it was found)

Severity: 🔴 data loss/corruption · 🟠 broken feature · 🟡 UX/correctness sharp edge.

| # | Sev | Bug | Found by | Commit |
|---|-----|-----|----------|--------|
| 1 | 🔴 | Trades backfill dropped >95 % of data (fixed window vs capped page) | code review + network test | `0580030` |
| 2 | 🔴 | `_merge` overwrote legacy v2 files on schema mismatch (data loss) | reading the merge path | `a44ba48` |
| 3 | 🔴 | Migration only rescaled TS, never renamed columns → files stuck | inspecting real on-disk schema | `a44ba48` |
| 4 | 🔴 | Provenance computed but never written to Parquet | reading `_write_parquet` | `a44ba48` |
| 5 | 🔴 | Dedup on `TS` alone lost 58 % of trades + collapsed order books | **review challenge: collected vs stored on real data** | `ea7beb7` |
| 6 | 🔴 | Shared `AsyncHTTPClient` closed mid-flight under concurrency ("run all jobs") | API E2E: a failed run | `acb7625` |
| 7 | 🟠 | Backfill `start` ignored — snapped to window (~41 d), not bar | **API E2E: requested 05-28, got 05-10** | `acb7625` |
| 8 | 🟠 | `dccd start` never ran scheduled backfills (only streams) | reading `cmd_start` | `60acf68` |
| 9 | 🟠 | Bitfinex BTC/USDT → 0 rows (USDT must map to UST), silent | CLI sweep of all 7 exchanges | `60acf68` |
| 10 | 🟠 | Custom ISO start date 500'd (`JobParams.start` too narrow) | reproduced in a shell | `dbefb93` |
| 11 | 🟠 | `dccd inventory` crashed on any OHLC dataset (int in `str.join`) | reading the CLI | `dbefb93` |
| 12 | 🟠 | Coinbase/Bitfinex declared WS channels they didn't implement → "running", 0 data | grepping `return;yield` | `86df650` |
| 13 | 🟠 | No way to cancel a runaway backfill; "Cancel" only closed the modal | **UI E2E: 1.6 M-row trades backfill couldn't be stopped** | `04e635d` |
| 14 | 🟡 | First `start=last` backfill on empty trades = 30-day default = millions | reasoning from #13 | `3a3e6cd` |
| 15 | 🟡 | No auth enforcement despite `ui_auth_token`; wildcard CORS | reading `app.py` | `bbc6127` |
| 16 | 🟡 | Inventory lists on-disk data unrelated to jobs, unexplained | **user feedback** | `ac2b0a6` |
| 17 | 🟡 | Stale "migrate timestamps" copy (it does schema too) | UI visual review | `ac2b0a6` |
| 18 | 🟡 | Stream stop took ~9 s (WS close handshake) | API E2E timing | `acb7625` |
| 19 | 🟡 | Fabricated OHLC `quote_volume`; dead `parallel`/`Page`/htmx | code review | `dfcec9b` |
| 20 | 🟡 | mypy never ran (aborted on Sphinx); CI silently red | running `mypy` | `500b2ec` |
| 21 | 🟠 | Deleting a stream job left its worker running + controllable (config gone, worker not) | code review of the UI-rework PR (#76) | `0586fef` |
| 22 | 🟡 | Historical *first date* edit reverted on reload — `GET /api/jobs` didn't return `start`, so the UI reset the field | **user feedback + tracing the field round-trip** | UI-polish PR |

The pattern: **3 of the worst (🔴 #1, #5, #7) were invisible to unit tests and
only surfaced by running the real operation and comparing input to output.**
#21 came from the cross-file tracer angle: `register_streams()` was append-only,
so the reconciliation (`sync_streams()`) had to be added when job-delete arrived.
#22 is the same lesson at UI scale: a write (`POST /api/jobs/update`) is only
real if the read (`GET /api/jobs`) returns the field back — test the round-trip,
not just the write.

---

## 3. Invariants to preserve (don't regress these)

- **Trades pagination is cursor-based** (`paginate_trades` + per-adapter opaque
  cursor). Never advance trades by a fixed time window.
- **Dedup key is per data type** (`ParquetStore._dedup_subset`): OHLC=`TS`,
  trades=`tid`(or composite), order book=`(TS,side,price)`. `TS` alone is unique
  only for OHLC.
- **Declared capabilities must be honest** — if a WS channel or `history` depth
  isn't really implemented, don't declare it; the engine rejects undeclared ones.
- **All timestamps ns UTC int64.** Legacy frames pass through `canonicalize()`
  before any `concat`.
- **A first `start=last` backfill is bounded** per type (`_DEFAULT_LOOKBACK_NS`).
- **Backfills are cancellable** (`stop_event` → `DELETE /api/backfill/{id}`).
- **Adapters share one HTTP client** that is reference-counted (concurrency-safe).
- **Stream workers are reconciled, not appended** — deleting a stream job runs
  `Scheduler.sync_streams()` to stop+drop the worker (see #21).
- **`EventBus` fans out to every registered queue** — SSE consumers `add_queue`
  on connect and `remove_queue` on disconnect; Live + Logs can stream at once.

---

## 4. Quick UI smoke script

A self-contained Playwright audit lives at `doc/dev/ui_smoke.py`. Start an
isolated `dccd ui`, then:

```bash
python doc/dev/ui_smoke.py http://127.0.0.1:8137
```
It walks every page (Dashboard, Data, Historical, Live, Config, Logs, Storage),
checks the `Collect ▾`/`System ▾` nav dropdowns route, exercises the inline
Historical job flow (create → Run → delete) and a Live stream create/delete, and
fails if there is any console error, uncaught JS exception, or HTTP ≥400. (The
old backfill *modal* and the `/jobs` page were removed in the UI rework — actions
are inline per page; `Inventory` was renamed `Data` with a `/inventory`→`/data`
redirect.)
