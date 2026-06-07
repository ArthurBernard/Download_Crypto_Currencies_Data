# dccd — developer brief (for Claude Code)

This folder is an **orientation pack written for Claude Code** (not end users). Its
only job is to give an agent a fast, faithful overview of the repository: what
exists, how it fits together, why it was built that way, and what is and isn't
done. End-user docs live in `doc/source/` (Sphinx); the authoritative working
rules live in the repo-root `CLAUDE.md`.

> **Relationship to `CLAUDE.md`**: `CLAUDE.md` is the source of truth for
> *commands, the layer map, and the hard invariants you must not regress*. This
> folder is the *narrative and depth* around it — rationale, per-area detail,
> current status, testing methodology. When the two disagree, trust `CLAUDE.md`
> and fix this folder.

## Read in this order

1. [`01-overview.md`](01-overview.md) — what dccd is, the current state, the repo
   map, the three usage modes.
2. [`02-architecture.md`](02-architecture.md) — the hexagonal layers in depth,
   the data flow, and where each responsibility lives.
3. [`03-decisions.md`](03-decisions.md) — the design choices and *why* (ns time,
   capability-driven engine, cursor pagination, dedup, SSE, the UI model), plus a
   short history of the v3 rewrite and its remediation.
4. [`04-exchanges.md`](04-exchanges.md) — the per-exchange capability matrix and
   the caveats that drive the code (Kraken OHLC, Bybit trades, order-book WS).
5. [`05-testing.md`](05-testing.md) — the testing layers, how to run each, the
   recipes, and the catalogue of real bugs found (and *how*).
6. [`06-status.md`](06-status.md) — what's done, what's pending, known gaps,
   tooling (skills, scripts, deploy), and deferred work.

## Tools kept here

- [`ui_smoke.py`](ui_smoke.py) — runnable headless-browser UI audit
  (`python doc/dev/ui_smoke.py http://127.0.0.1:8137`). See `05-testing.md`.

## Conventions for keeping this current

- This is descriptive, not aspirational: write what the repo **is**, not what it
  should become (put plans in [`07-roadmap.md`](07-roadmap.md) — the single source
  of open work — and history in git/CHANGELOG).
- A prior snapshot of these docs (the v3 planning/retrospective set) is archived
  under `doc/dev/_archive/` — **gitignored**, kept locally for reference only.
