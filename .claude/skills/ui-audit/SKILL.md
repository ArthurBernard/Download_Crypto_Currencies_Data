---
name: ui-audit
description: Deep, hands-on audit of the dccd web UI. Use when asked to "test the UI", "check the UI", "vérifier l'UI", verify the front-end, or make sure a UI change has no bug/regression. Goes far beyond a smoke test: it drives a real headless browser, presses every control, and — for every action — verifies BOTH what is displayed AND the data actually written to disk, plus console/JS/HTTP errors and cross-page consistency.
---

# Deep UI audit for dccd

The web UI is a pure HTTP client of the API (`interfaces/ui/` → `interfaces/api/`).
A UI bug can be: a JS error, a control that does nothing, a wrong/stale display,
an action that says "done" but wrote nothing (or the wrong thing) to disk, or two
pages disagreeing. This audit catches all of them.

**Golden rule:** never trust the UI's own message. When a button claims success,
go read the Parquet/SQLite it should have written and confirm it matches what was
asked. Never point the test at real data — always an isolated instance.

## 0. Prerequisites (install once)

```bash
pip install playwright && playwright install chromium
```

## 1. Stand up an isolated instance

Create a throwaway config + empty `data_path` on a free port (NOT the user's):

```bash
rm -rf /tmp/dccd-ui && mkdir -p /tmp/dccd-ui/data
cat > /tmp/dccd-ui/config.yml <<'EOF'
settings: {data_path: /tmp/dccd-ui/data, timezone: UTC, ui_host: 127.0.0.1, ui_port: 8151, ui_auth_token: null}
storage: {local_path: /tmp/dccd-ui/data, remotes: [], sync_interval: 0}
alerts: {webhook_url: null, max_consecutive_errors: 3}
jobs:
- {exchange: binance, pairs: [BTC/USDT, ETH/USDT], data_type: ohlc, operation: backfill, span: 3600, trigger_kind: interval, every: 3600, start: last}
- {exchange: binance, pairs: [BTC/USDT], data_type: trades, operation: stream, trigger_kind: supervised, start: last}
- {exchange: binance, pairs: [BTC/USDT], data_type: orderbook, operation: stream, trigger_kind: supervised, snapshot_interval: 60, start: last}
EOF
```

Seed a little data across types/exchanges so Inventory/Dashboard render, and so
some datasets are NOT in the jobs (to exercise the inventory≠jobs case):

```bash
C="-c /tmp/dccd-ui/config.yml"
dccd backfill $C -e binance -s BTC/USDT -t ohlc --span 3600 --start 2026-06-02
dccd backfill $C -e kraken  -s BTC/USD  -t ohlc --span 3600 --start 2026-06-03
dccd backfill $C -e binance -s BTC/USDT -t orderbook
```

Start the server **in the background** (use the Bash tool's background mode, not
`&` — that errors in this harness):

```bash
dccd ui -c /tmp/dccd-ui/config.yml   # run_in_background: true
```

## 2. Run the automated smoke first (fast fail)

```bash
python doc/dev/ui_smoke.py http://127.0.0.1:8151
```
It walks every page + the backfill modal (OHLC / trades-with-cancel / order book)
and fails on any console error, uncaught JS exception, or HTTP ≥400. A
`GET /api/events :: net::ERR_ABORTED` on navigation is benign (SSE closing).
If this fails, fix before going deeper.

## 3. The deep pass — write a Playwright script that, for EACH action, verifies display + disk

Capture all four browser failure channels on the page object:

```python
page.on("console",      lambda m: errs.append((m.type, m.text)) if m.type in ("error","warning") else None)
page.on("pageerror",    lambda e: js_exc.append(str(e)))
page.on("response",     lambda r: http_err.append((r.status, r.url)) if r.status >= 400 else None)
page.on("requestfailed", lambda r: req_fail.append(r.url))
page.on("dialog",       lambda d: asyncio.create_task(d.accept()))
```
Use `wait_until="domcontentloaded"` (never `networkidle` — the dashboard polls and
Logs holds an SSE connection). After navigating, wait ~2 s for XHRs, then assert.

For **every interactive control**, do the press → display → disk triangle:

| Action | Press (UI) | Verify displayed | Verify on disk |
|--------|-----------|------------------|----------------|
| Backfill OHLC (modal, custom date) | fill symbol/span/date, click Launch | progress bar → 100 %, "succeeded — N rows" toast | read `…/ohlc/<pair>/<span>/<year>.parquet`: row count ≈ N, **min(TS) matches the requested start date**, TS sorted, `low≤open,close≤high` |
| Backfill trades (modal) | type=trades → span row hidden; Launch | "running", then Stop appears | a 1-day window is millions of rows — click **Stop**, expect state "cancelled", and the collected rows ARE persisted (distinct `tid` count == stored rows; no dedup loss) |
| Backfill order book (modal) | type=orderbook; Launch | "succeeded · N rows" | one snapshot = many levels sharing one TS; stored rows == levels (NOT collapsed to 1) |
| Per-row Backfill button | click a row's Backfill | modal prefilled (exchange/symbol/type/span) | n/a (prefill only) |
| Jobs → Run now | click | new run appears in Dashboard "Recent runs" | a new run row in `runs.db` with rows_written>0 |
| Jobs → Start/Stop stream (trades AND orderbook) | click Start, wait, click Stop | status ○ Stopped → ● running → ○ Stopped | trades/orderbook Parquet gains rows; **stored rows == distinct tids** (trades) / levels (book) |
| Config → edit a field → Save | change `#a-max-errors`, click Save | "saved" status | `grep` the field in the on-disk `config.yml` |
| Config → Raw JSON tab | switch tab | editor shows valid JSON | `json.loads` it |
| Storage → Dry run | click | report appears | no files mutated |

**Cross-page consistency** — assert these agree (a classic source of "incoherence"):
- Dashboard "Datasets" count == `/api/inventory` length == Inventory rows.
- Dashboard streams card == Jobs stream status == `/api/streams`.
- A run triggered on Jobs shows up in Dashboard "Recent runs" and `/api/runs`.

**Challenge the numbers**, don't just check non-zero:
- Requested start `2026-05-28` but data starts `2026-05-10`? → the start isn't
  honoured (a real bug — pagination snapped to the window, not the bar).
- "Run all" → one run failed with "client has been closed"? → HTTP-client
  concurrency bug.
- Trades show N collected but stored << N? → first check you're reading **all**
  period files (`trades/<pair>/*.parquet`), then suspect the dedup key.

## 4. Look at the screenshots

Capture `full_page=True` screenshots of each page and **read them** (the Read
tool renders images). Several issues are only visible — stale labels, broken
layout, wrong copy — and never appear in the DOM text or console.

## 5. Auth path (if the UI may be token-protected)

Restart with `ui_auth_token: secret123` on another port and confirm: `/health`
open; `/api/*` 401 without token, 200 with `Authorization: Bearer secret123`;
the page injects `DCCD_TOKEN` and the SSE uses `?token=`.

## 6. Report

State plainly: pages clean (or which JS/console/HTTP errors), every action's
display-vs-disk result, cross-page consistency, and any number that didn't add
up. **Distinguish real bugs from benign noise** (the SSE abort) and from
by-design behaviour (inventory shows all on-disk data, not just jobs).

## 7. Clean up

Kill only your instance (`pkill -f "dccd ui -c /tmp/dccd-ui"`) and `rm -rf
/tmp/dccd-ui`. Never kill or touch the user's own running instance or real data.
