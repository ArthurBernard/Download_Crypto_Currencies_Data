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

## Deferred — M3 (post-3.0)

Larger axes intentionally parked until after the 3.0 release. Not started; do not
treat as bugs (see `06-status.md`).

- [ ] **MCP interface** — `interfaces/mcp/` mapped onto the operation registry
  (same parity contract as API/CLI).
- [ ] **Kraken deep OHLC from trades** — a `DerivedOHLCSource` wiring
  `domain/transforms.aggregate_ohlc` into the resolver (REST only gives 720 recent
  bars; the transform exists but isn't wired).
- [ ] **Derivative markets** — `DataType` for funding / open-interest /
  liquidations, `Symbol.market=perp`. **Sequencing informed by the 2026-07 data scan**
  ([`plans/data-sources-scan-2026-07.md`](plans/data-sources-scan-2026-07.md), 16 sources
  verified against official API docs):
  - **P0 — funding rates, Binance + Bybit first** (full history via public REST,
    simple pagination; OKX depth unconfirmed → P1). The `DataType.FUNDING` framework
    makes every other `/futures/data/*` endpoint a cheap follow-on.
  - **P0 — quarterly-futures klines (→ basis)**: NOT a new DataType — reuses the OHLC
    machinery verbatim (one adapter change). Best value-for-effort of the whole scan;
    can ship before the epic proper.
  - **P1, time-sensitive — open interest + long/short + taker ratios on Binance**:
    history capped at 30 days/1 month → every week of delay is data lost forever;
    Bybit OI has full history (backtestable). Start forward collection early.
  - **Descoped within this epic — liquidations**: WS-only, forward-only, throttled/lossy
    (no REST history exists); architecturally unlike the rest — park it.
- [ ] **Metric-series sources (non-exchange)** — a second generalization: no
  `Symbol(base,quote)`, no OHLC shape; `(ts, entity, metric, value)` instead. From the
  same scan: **P0 CoinMetrics Community** (full daily history, free, non-commercial
  licence — the "on-chain" unblock named by fynance-research) and **P0 DefiLlama
  stablecoins** (supply/peg since ~2017 — a liquidity-proxy family nothing else covers);
  P1 Deribit DVOL (OHLC-shaped, public) and Fear & Greed (trivial single call). The
  first source pays for the schema; the rest are nearly free follow-ons.
- [ ] **Non-crypto assets (ETF / equities / FX)** — a third generalization, filed
  2026-07-04 by fynance-research: its documented route past the crypto ceiling is
  *more uncorrelated assets*, and the first consumer is a ~50-name liquid **ETF
  universe, daily bars** (multi-asset trend/cross-section book). Same OHLC shape and
  storage layout; the delta is the `Symbol` domain (ticker/exchange/currency instead
  of a crypto pair) and a new venue class:
  - **P0 — bulk daily adapter, Stooq or Tiingo** (public REST, free, no auth/gateway —
    exactly the transport shape dccd already speaks) + the ETF universe. One trap the
    research side names as blocking: **dividend/split adjustment must be explicit**
    (adjusted vs raw close — an equity backtest needs total-return-consistent series;
    crypto never had this problem).
  - **P1 — FX daily/intraday** — IBKR IDEALPRO (deep history, no subscription) or
    Dukascopy tick data.
  - **Later — IBKR as a source** (deep 1-min equity history to ~2004, FX): transport
    is a stateful TWS/`ib_async` gateway session, the same "unlike the rest"
    class as the descoped liquidations WS — park until the Trading_Bot IBKR
    adapter exists (the gateway ops land there first) and intraday needs are proven.
  - **Non-goal — continuous futures**: IBKR drops expired contracts after ~2 years
    (kills deep backfill); if ever needed, that's databento/Norgate, a paid decision.
- [ ] **Auth/secrets for private endpoints** — credential injection into
  `transport/` for authenticated exchange endpoints.
