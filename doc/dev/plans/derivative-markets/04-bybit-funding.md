---
plan: derivative-markets/04-bybit-funding
kind: leaf
status: planned
complexity: medium
model: sonnet
depends: [03]
parallel: false
branch: feat/bybit-funding
pr: ""
---

# Bybit funding history (second exchange on the FUNDING framework)

## Goal

`backfill bybit "BTC/USDT:perp" funding` collects Bybit's realized funding
history, enabling the cross-exchange funding-spread family. Scan row 2 — the
framework exists (leaf 03); this leaf is one adapter with two documented
quirks: **paired time params** and **newest-first pages**, plus an
**empirical depth probe** before declaring `history`.

## Files to change

- `dccd/sources/bybit.py` — add `FundingHistory` to the class bases and
  implement:
  - `fetch_funding_page`: GET `{_BASE}/funding/history` with
    `{"category": "linear", "symbol": render_symbol, "startTime":
    start_ns // 1_000_000, "endTime": int(cursor) if cursor else
    end_ns // 1_000_000, "limit": min(limit, 200)}`.
    **Both `startTime` and `endTime` must always be sent** (Bybit errors on
    `startTime` alone — the scan-verified gotcha). Check `retCode == 0` like
    `fetch_ohlc_page`. `result.list` is **newest-first**:
    `{"fundingRate": str, "fundingRateTimestamp": str-ms}` →
    `FundingRate(ts=int(fundingRateTimestamp) * 1_000_000,
    rate=float(fundingRate))` (no mark price on this endpoint).
    Backward walk: `oldest_ms = int(result.list[-1] timestamp)`;
    `next_cursor = str(oldest_ms - 1)` (next call's `endTime`) when
    `len(list) == limit` and `oldest_ms > start_ms`, else `None`.
    This satisfies the leaf-03 cursor contract; `paginate_trades` filters the
    window and its `items[-1].ts < start_ns` early-break fires correctly on
    the oldest element of a newest-first page. Cross-page order is
    non-chronological — fine, `ParquetStore._merge` sorts on save.
  - `capabilities()`: `Capability(data_type=FUNDING, transport="rest",
    mode="historical", max_per_request=200, page_direction="backward",
    markets=["perp"], history=<result of the depth probe — see Steps>)`.
- `dccd/tests/v3/test_sources.py` — see Tests.

## Steps

1. Implement the adapter method + capability (leave `history` provisional).
2. **Depth probe (mandatory, before finalising the capability)**: run the
   real backward walk on BTCUSDT to exhaustion. If it reaches the contract's
   launch era (~2020-03) declare `history="full"`; if it stops earlier,
   declare `history="recent"` + `recent_window_s` set to the observed window,
   and record the observation in the PR description. The scan explicitly
   left Bybit's total funding depth unverified — dccd's honesty invariant
   requires the probe, not an assumption.
3. `pytest` + `ruff check dccd/`.

## Tests

- `test_sources.py`, stubbed client:
  - every call carries **both** `startTime` and `endTime` and
    `category="linear"`;
  - newest-first two-page walk: page 1 full (len==limit) → cursor
    `oldest-1`; page 2 short → `next_cursor is None`;
  - `retCode != 0` → empty page, no crash;
  - capability: `markets=["perp"]`, `max_per_request=200`.
- `test_application.py` — nothing new (branch covered in leaf 03).

## Verification on real data

Isolated store, real Bybit API:

1. Full backfill `bybit "BTC/USDT:perp" funding start=2020-01-01` — assert
   depth matches the probe result, zero duplicate TS, |rate| sane (≤ 1 %).
2. Interval check: modal TS spacing equals the symbol's declared
   `fundingInterval` from `GET /v5/market/instruments-info?category=linear`
   (per-symbol interval is the documented Bybit caveat).
3. Cross-exchange sanity: on 5 shared 8h timestamps, Bybit vs Binance
   funding (leaf 03 data) have the same order of magnitude and mostly the
   same sign — catches ms/ns or %-vs-fraction unit bugs.
4. Re-run `start=last` — idempotent (row count stable).

## Closeout

- CHANGELOG (`Added`): "Bybit perp funding-rate history (paired-params
  backward pagination); cross-exchange funding spread now computable (#NN)"
- ADR: only if the depth probe forces `history="recent"` (record the
  observed depth); otherwise none — mechanical second adapter.
- Status/roadmap: deferred to leaf 07.
