# 5 — Testing & findings

> Guiding principle that found most of the serious bugs: **challenge every result
> on real data.** A green unit suite said nothing about a backfill that wrote 0
> rows, a store that lost 58% of trades, a "Stop" button that did nothing, or an
> order book showing a crossed bid/ask. Run the real thing, inspect what landed,
> compare it to what was asked.

## Layers (and how to run each)

| Layer | Command | Catches |
|-------|---------|---------|
| **Unit** | `pytest` | logic, schema, regressions (~480 tests; network excluded) |
| **Network E2E** | `pytest -m network` | real exchange APIs: pagination drains, symbol mapping, round-trip |
| **Type** | `mypy dccd/` | strict on `domain/`, lenient glue (Success = green) |
| **Lint** | `ruff check dccd/` | style, dead imports |
| **API E2E** | run `dccd ui`, drive with `curl` | every route, status codes, real backfills/streams |
| **UI E2E** | Playwright (headless Chromium) | JS execution, console errors, interactive flows |
| **Docs** | `cd doc && make html` | autodoc breakage (must be 0 warnings) |

## Recipes

**Isolated instance.** Always test against a temp config + temp `data_path` on a
free port, never real data:
```bash
dccd ui -c /tmp/t/config.yml &        # data_path=/tmp/t/data
B=http://127.0.0.1:8137
curl -s "$B/api/inventory"
curl -s -XPOST "$B/api/backfill" -H 'Content-Type: application/json' \
  -d '{"exchange":"binance","symbol":"BTC/USDT","data_type":"ohlc","span":3600,"start":"2026-05-28"}'
# then CHALLENGE: read the parquet, check the time range matches the request
python -c "import polars as pl; df=pl.read_parquet('.../2026.parquet'); print(df['TS'].min(), len(df))"
```
Assert error cases too: 400/404/422 on bad symbol, missing span, unknown run/stream.

**UI smoke (`ui_smoke.py`).** Headless Chromium audit; fails on any console error,
uncaught JS, or HTTP ≥400. Walks every page, the `Collect ▾`/`System ▾` nav
dropdowns, the inline Historical job flow (create → Run → delete) and Live
create/delete. Notes learned the hard way:
- Use `wait_until="domcontentloaded"`, not `networkidle` (the SSE connection
  never goes idle).
- `GET /api/events :: net::ERR_ABORTED` on navigation is **benign** (EventSource
  closing).
- Don't read `inner_text` immediately — wait for XHRs, then assert the dynamic
  containers populated. **Look at the screenshots**; some issues are only visible.
```bash
pip install playwright && playwright install chromium
python doc/dev/ui_smoke.py http://127.0.0.1:8137
```

**Before any mutating action on real data**: back up first, then verify the result
row-for-row against the backup (the real v2→v3 migration was validated this way).

## Findings log (every bug, and how it was found)

Severity: 🔴 data loss/corruption · 🟠 broken feature · 🟡 UX/correctness edge.

| # | Sev | Bug | Found by |
|---|-----|-----|----------|
| 1 | 🔴 | Trades backfill dropped >95% (fixed window vs capped page) | code review + network test |
| 2 | 🔴 | `_merge` overwrote legacy v2 files on schema mismatch | reading the merge path |
| 3 | 🔴 | Migration only rescaled TS, never renamed columns | inspecting real on-disk schema |
| 4 | 🔴 | Provenance computed but never written | reading `_write_parquet` |
| 5 | 🔴 | Dedup on `TS` alone lost 58% of trades | review challenge: collected vs stored |
| 6 | 🔴 | Shared HTTP client closed mid-flight under concurrency | API E2E: a failed "run all" |
| 7 | 🟠 | Backfill `start` ignored — snapped to window, not bar | API E2E: requested 05-28, got 05-10 |
| 8 | 🟠 | `dccd start` never ran scheduled backfills (only streams) | reading `cmd_start` |
| 9 | 🟠 | Bitfinex BTC/USDT → 0 rows (USDT must map to UST) | CLI sweep of all 7 exchanges |
| 10 | 🟠 | Custom ISO start date 500'd (`JobParams.start` too narrow) | reproduced in a shell |
| 11 | 🟠 | `dccd inventory` crashed on OHLC (int in `str.join`) | reading the CLI |
| 12 | 🟠 | Coinbase/Bitfinex declared WS channels they didn't implement | grepping `return;yield` |
| 13 | 🟠 | No way to cancel a runaway backfill | UI E2E: 1.6M-row trades couldn't be stopped |
| 14 | 🟡 | First `start=last` on empty trades = 30d default = millions | reasoning from #13 |
| 15 | 🟡 | No auth despite `ui_auth_token`; wildcard CORS | reading `app.py` |
| 16 | 🟡 | Inventory lists on-disk data unrelated to jobs, unexplained | user feedback |
| 17 | 🟡 | Stale "migrate timestamps" copy (it does schema too) | UI visual review |
| 18 | 🟡 | Stream stop took ~9s (WS close handshake) | API E2E timing |
| 19 | 🟡 | Fabricated OHLC `quote_volume`; dead `parallel`/`Page`/htmx | code review |
| 20 | 🟡 | mypy never ran (aborted on Sphinx); CI silently red | running `mypy` |
| 21 | 🟠 | Deleting a stream job left its worker running + controllable | code review of PR #76 |
| 22 | 🟡 | Historical *first date* edit reverted (`GET /api/jobs` lacked `start`) | user feedback + field round-trip trace |
| 23 | 🟠 | Live order book showed a **crossed** best bid/ask (WS emitted unmerged diffs) | testing the Live UI on real data |
| 24 | 🟡 | Order-book liveness ticked every second though cadence was "Δ 30s" | user feedback; sampled WS instead of the saved snapshot |

The pattern: the worst (🔴 #1, #5, #7, #23) were invisible to unit tests and only
surfaced by running the real operation and comparing input to output. #21 came
from the cross-file tracer angle (`register_streams` was append-only). #22/#24 are
the same lesson at UI scale — a write is only real if the read returns it, and a
liveness must reflect what's actually captured.

## Invariants
See `02-architecture.md` (and `CLAUDE.md`) for the full list. The data-path ones
worth re-stating: cursor-based trades pagination; per-type dedup keys; honest
capabilities; ns int64 with `canonicalize()` before `concat`; bounded first
backfill; cancellable backfills; one reference-counted HTTP client.
