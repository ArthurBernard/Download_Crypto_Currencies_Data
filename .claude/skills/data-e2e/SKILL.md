---
name: data-e2e
description: End-to-end verification that dccd actually collects correct data from real exchanges. Use when asked to validate data collection, "check the data is right", verify a backfill/stream really works, or after touching pagination, adapters, or storage. Runs a real operation against a live exchange on an ISOLATED store, then proves what landed on disk matches what was requested (coverage, dedup, ordering, OHLC sanity).
---

# Real-data E2E for dccd

A green unit suite says nothing about whether a backfill wrote the right rows.
This skill runs the real thing and **challenges the output against the request**.
It found the worst v3 bugs (95 % of trades dropped, 58 % lost to dedup, start
date ignored). Always use an isolated `data_path` — never the user's real data,
and back up before any in-place migration.

## 1. Isolated store

```bash
rm -rf /tmp/dccd-data && mkdir -p /tmp/dccd-data
```
Use the Python `Client` (auto-wires all adapters) or `dccd backfill -c <cfg>`
pointed at `/tmp/dccd-data`.

## 2. OHLC backfill — verify range, count, sanity

```python
import asyncio, polars as pl, datetime as dt
from dccd import Client
async def main():
    async with Client() as c:
        c._store = c._store.__class__("/tmp/dccd-data")
        r = await c.backfill("binance", "BTC/USDT", "ohlc", span=3600, start="2026-05-28")
        df = c.read("binance", "BTC/USDT", "ohlc", span=3600)
        assert len(df) > 0
        mn = dt.datetime.fromtimestamp(int(df["TS"].min())/1e9, dt.UTC)
        # CHALLENGE: start must be honoured (not pulled back to a window boundary)
        assert mn >= dt.datetime(2026,5,28,tzinfo=dt.UTC), f"start not honoured: {mn}"
        assert df["TS"].is_sorted() and df["TS"].n_unique()==len(df)         # sorted, deduped
        assert df.filter((pl.col("low")>pl.col("open"))|(pl.col("high")<pl.col("close"))).height==0
        # native fields present where the exchange provides them (Binance: yes)
        assert df["quote_volume"].null_count()==0 and df["trades"].null_count()==0
asyncio.run(main())
```

## 3. Trades — prove the cursor drains the window AND no dedup loss

The class of bug: pagination only grabs the first capped page, or storage dedups
distinct trades that share a millisecond timestamp.

```python
from dccd.transport.paginate import paginate_trades
from dccd.domain.timeutils import ns_now, NS
# collect a short window and check it is multi-page + fully covered
cap = src.capability_for(DataType.TRADES, "rest", "historical")
end = ns_now() - 5*60*NS; start = end - 900*NS      # 15 min
pages = 0
async def fetch(s,e,l,c):
    nonlocal pages; pages += 1
    return await src.fetch_trades_page(sym, s, e, l, c)
out = [t async for t in paginate_trades(fetch, cap, start, end)]
assert pages > 1                                    # cursor was followed past page 1
assert len(out) > cap.max_per_request               # more than one capped page
assert (max(t.ts for t in out)-min(t.ts for t in out))/NS >= 0.8*900   # covers the window
# now store and CHALLENGE: distinct tids == stored rows (no TS-collision loss)
store.save(ds, out, Provenance(source="x"))
assert len(store.load(ds)) == len({t.tid for t in out})
```
For exchanges where `fetch_trades_page` returns `(items, next_cursor)`, confirm
the cursor advances and stops (`next_cursor is None`) at the window edge.

## 4. Order book — levels survive

One snapshot has many levels sharing one TS. After `save`, stored rows must equal
the number of levels (TS-only dedup would collapse them to 1).

## 5. Streams — capture cadence AND resource sanity

The class of bug: per-frame work whose output the consumer throws away. The
v3.3 production case: order-book snapshots built as pydantic objects on every
WS delta while `operations.stream` kept one per `snapshot_interval` → 97.7 %
CPU, starved event loop, remote UI unusable — and every unit test green.
After touching a WS adapter, `operations.stream` or the scheduler:

```bash
# drive 2-3 REAL order-book/trade streams on the isolated store for ~2 min:
ps -o pcpu=,rss= -p <daemon-pid>      # steady-state CPU must be < 10 %
```

Also challenge the capture cadence on disk: with `snapshot_interval=10`, about
6 snapshots (±2) per minute must land in Parquet, each truncated to the
subscribed depth, levels uncrossed (max bid < min ask), ts monotonic. A stream
that is "live" but writes nothing is a failure, not a wait state (rejected WS
subscriptions must surface as errors).

## 6. Per-exchange sweep

Smoke each of the 7 adapters for the type it supports (OHLC for all; trades for
binance/kraken/okx/bitfinex/bitmex; bybit spot has no trade history). Watch for
**silent 0-row** results (e.g. a wrong symbol mapping like Bitfinex USDT→UST that
returns HTTP 200 + `[]`).

## 7. Migration (only with a backup)

Before `dccd migrate --no-dry-run` on real data: `cp -a <data> <data>.backup-…`,
run `--dry-run`, apply, then verify **row-for-row no loss** vs the backup and that
`needs_migration` is now False (idempotent).

## 8. Report

For each dataset: requested vs stored (count, time range), pagination pages,
dedup integrity, OHLC sanity, and any silent-zero or coverage gap. Clean up the
isolated store.
