# 7 — Roadmap / next steps

Planned work after the UI rework. The theme: **go from "runs on my machine" to
"runs unattended on a remote server, reachable from anywhere, with data backed up
off-box."** Much of the scaffolding already exists (`Dockerfile`,
`deploy/dccd.service`, `ui_host`/`ui_auth_token`/`ui_allow_origins`,
`storage/remote.py` rclone sync) — these steps are mostly *wiring, hardening, and
UX*, not greenfield.

Status legend: `[ ]` todo · `[~]` partially in place · `[x]` done.

This file is the **single source of truth** for open work — it is read by
`/pick-task` and updated by `/finish-task` / `/abandon-task`. Finished work is
*removed* from here (git log + `CHANGELOG.md` are authoritative for *what*
shipped; `03-decisions.md` for *why*). Keep it short and true.

---

_Epic A — Run the app on a remote server: **done.** Container image (digest-pinned,
`POLARS_VARIANT` for old CPUs) and systemd unit (venv + `StateDirectory`) verified on
a real server; restart/reboot safety, `HealthMonitor` alerts + `/health` healthcheck,
secret injection, and the `how-to/deploy` guide all shipped. See `06-status.md`._

_Epic B — View the UI remotely (PC + mobile): **done.** TLS/overlay exposure guide
(`how-to/expose-remote`), browser `/login` + `HttpOnly` cookie session (token no longer
templated into pages), API hardening (`ui_rate_limit`/`ui_readonly`/`ui_trusted_proxy` +
CORS-never-wildcard test), mobile-responsive pass, and a threat model — all verified
live over Tailscale. See `06-status.md`._

_Epic C — Sync data to a remote space: **done.** Scheduled rclone sync + UI,
coverage manifest, free-space purge, read-through restore, and the
`how-to/sync-remote` guide all shipped. See `06-status.md`._

---

## Suggested sequence

1. ~~**C (sync)**~~ — done.
2. ~~**A (remote run)**~~ — done.
3. ~~**B (remote access)**~~ — done.

The three "runs unattended on a remote server, reachable from anywhere, backed up
off-box" epics are complete. Next axes live under **Deferred — M3** below.

Each epic should ship with a `doc/source/` how-to and, where it touches data,
a pass of the `data-e2e` skill.

---

_Epic D — Performance & robustness (2026-06-10 production audit): **done.**
Order-book snapshots built only at capture time (97.7 % → 2 % CPU), inventory
from parquet footer stats + cache + off-thread, honest WS subscriptions
(declared depths, loud rejections), scheduler backoff/jitter + alert cooldown,
gzip + saner UI polling. PRs #118–#121 + the last leaf; ADR journal has the
rationale; see `06-status.md`._

_Epic E — Audit 2026-06-10 fixes: **done** (PRs #126–#134, 2026-06-11). Zombie
runs, startup purge, time-based stream flush, one HTTP pool per operation,
wired rate limiter, systemd limits, honest coverage metric, CLI + adapter test
suites, OpenAPI version + CI docs gate. ADR journal has the rationale; see
`06-status.md`._

_Prod ops: **complete** (2026-06-11). Off-box backup (hourly rclone sync to
the main PC over Tailscale), ntfy alert webhook (test delivered), systemd
limits, and the server upgraded to v3.5.0 — whose first boot marked 491
orphaned runs `stale`. See `06-status.md`._

## Hardening backlog (post-audit follow-ups, 2026-06-11)

Small, well-scoped items surfaced while operating v3.5.0 in production.

- [ ] **Config export / load (full file)** — round-trip the complete
  effective configuration (settings + alerts + storage + all jobs): a
  `dccd config export [-o file]` that dumps the validated running config as
  YAML, and `dccd config load <file>` that validates, persists, and applies
  it live (`sync_streams` + `sync_intervals`), plus the matching
  `GET`/`POST /api/config/file` for registry parity. Use case: back up or
  replicate a collector (e.g. arthurserver's 50 jobs) in one command. One
  leaf.

P2 (append+compaction writes) and P3 (filename-based pruning in `load()`)
stay parked as perf ideas until load demands them (see the audit doc).

## Research-driven data asks (fynance-research, 2026-07-06)

Filed after the allweather-wave campaign (E59–E74). Note: the **funding-rate ask is
resolved** — the derivative-markets epic's series (`<exchange>/funding/…`, 8 h,
Binance/Bybit/krakenfutures, 2019+) landed and the research repo now consumes it
directly. What remains:

- [ ] **P1 — Atomic parquet writes (write temp + `os.replace`).** The live collector's
  parquet writes are observably non-atomic: a concurrent reader caught **6 truncated
  files mid-write** (`PAR1` footer check fails; e.g. `binance/ohlc/UNI-USDT/1m/2026.parquet`,
  observed 2026-07-06 by fynance-research while backtesting against the live store —
  transient, self-heals on the next write, but any reader race can crash or silently
  read a partial year). Fix: write to `<file>.tmp` then atomic rename; cheap and
  store-wide.
- [ ] **P2 — Native-USD spot backfill for bybit/okx (BTC/ETH-USD).** bybit's USD
  legs only start 2026-01, okx's 2024-01, so every cross-venue premium/dispersion
  measurement (fynance-research E53/E61/E65/E71) substitutes the USDT legs under a
  USD≈USDT parity assumption. Backfilling deeper native-USD history (as far as the
  venues' REST archives allow — verify depth empirically per the honesty invariant)
  removes the proxy from an entire measurement family.
- [ ] **P3 — Quarterly futures klines: intraday granularity + pre-2021 backfill.**
  `BTC/ETH-USDT_QUARTER` exists only at 1d from 2021-02; fynance-research E60
  (basis-regime, null at that resolution/window) could not see the 2020–21 euphoria
  top and cannot study roll behaviour intraday. Cheap follow-on of the
  derivative-markets epic; opportunistic only.

## Deferred — M3 (post-3.0)

Larger axes intentionally parked until after the 3.0 release. Not started; do not
treat as bugs (see `06-status.md`).

- [ ] **MCP interface** — `interfaces/mcp/` mapped onto the operation registry
  (same parity contract as API/CLI).
- [ ] **Kraken deep OHLC from trades** — a `DerivedOHLCSource` wiring
  `domain/transforms.aggregate_ohlc` into the resolver (REST only gives 720 recent
  bars; the transform exists but isn't wired).
- [ ] **OKX funding / open interest** — cheap follow-ons on the shipped
  `FUNDING`/`OPEN_INTEREST` mixins (derivative-markets epic, PRs #183–#190);
  **verify history depth empirically before declaring capabilities** (scan rows
  3/6 left both unconfirmed; the honesty invariant requires a probe, not an
  assumption). Liquidations stay out of scope (WS-only, forward-only, lossy —
  see the 2026-07-05 ADR tombstone).
- [ ] **Metric-series sources (non-exchange)** — a second generalization: no
  `Symbol(base,quote)`, no OHLC shape; `(ts, entity, metric, value)` instead. From the
  same scan: **P0 CoinMetrics Community** (full daily history, free, non-commercial
  licence — the "on-chain" unblock named by fynance-research) and **P0 DefiLlama
  stablecoins** (supply/peg since ~2017 — a liquidity-proxy family nothing else covers);
  P1 Deribit DVOL (OHLC-shaped, public) and Fear & Greed (trivial single call). The
  first source pays for the schema; the rest are nearly free follow-ons. Also
  **Binance long/short + taker ratios** (scan row 7): same `(ts, metric, value)`
  shape, same 30-day cap as Binance OI — **time-sensitive**, forward collection
  should start as soon as the schema exists.
- [ ] **Auth/secrets for private endpoints** — credential injection into
  `transport/` for authenticated exchange endpoints.
