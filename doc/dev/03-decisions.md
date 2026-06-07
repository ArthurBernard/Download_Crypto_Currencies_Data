# 3 — Design decisions & rationale

The *why* behind the structure. Each entry is a choice that shapes the code; if
you're about to change one, read the rationale first.

## Core architecture

- **Hexagonal layers.** The domain is pure and synchronous so logic is trivially
  testable and I/O is swappable. The v2 codebase mixed I/O with logic; v3 inverts
  the dependencies. Cost: more indirection (`service_factory` wiring) — accepted
  for testability and the 3-interface parity.
- **One wiring source (`service_factory`).** `Client`, CLI and API must build the
  exact same registry/store, or the three modes drift. Never re-wire adapters ad
  hoc — call the factory.
- **Nanosecond UTC `int64` everywhere internal.** v2 had heterogeneous time
  units; ns int64 is exact, sortable, and dedup-friendly. Conversions happen only
  at the edges (adapter parse, UI display).

## Collection engine

- **Capability-driven, not exchange-special-cased.** Adapters *declare*
  `history`, `max_per_request`, `page_direction`, `spans`, `max_depth`; the engine
  reads them. This was the central remediation: the first v3 engine ignored
  capabilities and paginated every trade feed by a fixed time window, silently
  losing >95% of trades on 5/7 exchanges. The fix generalised pagination instead
  of patching each exchange.
- **Trades pagination is cursor-based.** Each adapter returns `(items,
  next_cursor)` (opaque: `fromId`, `since`-ts, `after`-ts, …); the paginator
  follows the cursor until the window drains. A fixed window can't work — page
  caps range from 60 (Bybit) to 10 000 (Bitfinex).
- **Honest capabilities + early `NoCapability`.** Better to reject Bybit *spot
  trades history* up front than to "run" and produce zero rows. A WS channel that
  isn't really implemented must not be declared (the silent-empty-stream bug).
- **Bounded first backfill.** A first `start=last` on an empty dataset uses a
  per-type default lookback (short for trades), so one click can't trigger a
  multi-million-row run from epoch 0.

## Storage

- **Per-data-type dedup keys.** OHLC=`TS`; trades=`tid` (else a composite);
  order book=`(TS, side, price)`. Using `TS` alone for trades collapsed 58% of
  rows in an early bug — `TS` is unique only for OHLC.
- **Defensive reads + provenance.** Legacy v2 frames are canonicalised (columns
  renamed/aligned) before any `concat`, so a stale file can never be silently
  overwritten or lose rows. Provenance is written into the Parquet footer (it was
  computed-but-dropped early on).
- **Annual OHLC files, daily trades files.** Matches access patterns and keeps
  files a sane size.

## Events & streaming

- **SSE, not browser WebSocket.** The browser only *receives* updates
  (server→client). SSE is one-way, auto-reconnecting (`EventSource`), works over
  plain HTTP, and supports `?token=` auth. A browser WS would add a bidirectional
  channel, new auth, and manual reconnect for zero benefit. (The WS *to exchanges*
  is a separate concern, handled by `transport/ws.py`.)
- **EventBus multi-queue fan-out.** Each SSE consumer (Live + Logs + Dashboard in
  separate tabs) registers its own queue; a single shared queue let the last
  connection steal events.
- **Liveness via a throttled `StreamSampleEvent`** (≤1/s), carrying *numeric*
  `value`/`bid`/`ask` (the client formats). Samples are **not persisted**.
- **Order-book liveness is tied to the snapshot save, not the WS frame.** The WS
  pushes the book continuously but only one snapshot per `snapshot_interval` is
  captured; emitting the sample on save keeps the liveness coherent with what's
  actually collected (the age counts up to the interval and resets).

## Web UI model (post-rework, PR #76)

- **Split by concern.** Read-only **Data** (what's on disk), **Historical**
  (backfill jobs), **Live** (streams) — instead of one page that both browsed and
  launched. Each is data-type tabs → per-exchange accordions → one row per
  dataset, with inline job create/edit/delete (no Config detour).
- **Config no longer manages jobs.** Jobs live on Historical/Live; the Config
  page keeps Settings/Alerts/Storage + a raw-JSON tab for bulk edits (it preserves
  the `jobs` array on save). This removed a duplicated job form.
- **Liveness freshness label rule.** A live relative "N ago" counter under 24h,
  an absolute date beyond; the last-run date-time when stopped. The dot's "fresh"
  window is span-aware (OHLC span / order-book `snapshot_interval` / short for
  trades) so a slow feed doesn't read "dead".
- **Two dropdown nav groups** (`Collect ▾`, `System ▾`) with `Dashboard`/`Data`
  flat — chosen over flat-7 or inline group labels.

## History in two paragraphs

v3 was a from-scratch hexagonal rewrite (phases P0–P8). The architecture landed
well, but the first cut shipped with data-correctness regressions that unit tests
missed: a GC'd backfill task writing 0 rows, trades pagination losing >95% of
data, dedup collapsing trades, provenance never written, the engine ignoring
declared capabilities, and a "Stop" button that did nothing. A remediation pass
fixed these, added cursor pagination, a one-shot v2→v3 migration (run and
verified on the existing ~120 files, zero loss, since removed), Bearer auth, and
network-marked E2E tests.

The recurring lesson — now baked into how we test — is **challenge every result
on real data**: a green unit suite said nothing about a backfill that wrote 0
rows or a store that lost half its trades. See `05-testing.md`. The most recent
round (the UI rework + order-book liveness fixes) followed the same method:
drive the real UI, read what renders, compare to the feed.
