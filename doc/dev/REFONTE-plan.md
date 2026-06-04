# Refonte dccd v3 — Plan d'implémentation

> Plan détaillé, ordonné, sans incertitude. S'appuie sur :
> [`REFONTE.md`](REFONTE.md) (archi), [`REFONTE-noyau.md`](REFONTE-noyau.md)
> (modèles/ports/jobs), [`REFONTE-capacites.md`](REFONTE-capacites.md) (exchanges).
> Le suivi opérationnel (cases à cocher) est dans `TODO.md`.

---

## 0. Décisions verrouillées (rappel)

**Archi** : hexagonale (domaine pur ← application/ports ← adapters).
**I/O** : async-first (httpx + websockets, une boucle ; domaine sync ; disque/polars
via `asyncio.to_thread` ; CLI via `asyncio.run`).
**Versioning** : dccd **v3, rupture nette** — l'ancien est supprimé au fur et à
mesure, pas de shim de compat.
**Python** : **3.11+** (TaskGroup natif).
**Temps** : **ns** partout (int64 UTC).
**Ports** : **protocoles fins** par (data_type × mode).
**DataType** : enum fermé `ohlc`/`trades`/`orderbook` + point d'extension.
**Order book** : **snapshots périodiques** top-K (flag delta en réserve).
**Curseur pagination** : `str` opaque.
**Transfos** : pures, dans le domaine.
**Opérations** : **`backfill` + `stream`** (+ `read`/`inventory`). `update`=`backfill(start="last")`.
**MCP** : différé (registre d'opérations prêt).
**UI↔API** : stricte (UI = pur client HTTP).
**Runs store** : **SQLite** (stdlib, append-only, requêtable), derrière `RunsStorePort`.
**Données existantes** : **migrées** (script one-shot `TS→ns` + reformat).
**Périmètre v3.0** : exchanges **pilotes Binance + Kraken + Coinbase** d'abord,
**fonctionnel complet** (backfill + stream, 3 types) ; puis les 4 autres.
**Kraken OHLC dérivé** : **différé** (M3) — en v3.0, OHLC profond Kraken = `NoCapability`.
**Branche** : **`feat/refonte-v3`** (intégration longue) ; sous-branches PRées dedans ;
une PR finale `refonte-v3 → develop`.

---

## 1. Structure cible du package

```
dccd/
  domain/                 # PUR, sync, zéro I/O
    symbol.py             # Symbol + normalisation (XBT→BTC)
    types.py              # DataType (enum + extension)
    records.py            # OHLCBar, Trade, OrderBookLevel, OrderBookSnapshot
    dataset.py            # DatasetId, Provenance
    capability.py         # Capability (+ history: full|recent)
    transforms.py         # aggregate_ohlc… (reprend process_data)
    timeutils.py          # helpers ns + alignement span (reprend date_time)
    errors.py             # NoCapability, CoverageError, …
  transport/              # async, driven
    http.py               # client httpx async (retry/backoff)
    ws.py                 # base WebSocket async (reconnect, checkpoint)
    ratelimit.py          # token-bucket par exchange
    paginate.py           # Paginator (forward/backward, piloté par capacité)
  sources/                # adapters exchange, driven
    base.py               # Source + protocoles fins + Registry
    resolver.py           # Resolver (capacité + history → Source)
    binance.py kraken.py coinbase.py bybit.py okx.py bitfinex.py bitmex.py
    derive.py             # DerivedOHLCSource (M3)
  storage/                # driven
    base.py               # StoragePort, RunsStorePort, DatasetInfo
    parquet.py            # impl Parquet (reprend DataStore ; ns + provenance)
    runs_sqlite.py        # runs/métriques (SQLite)
    remote.py             # sync rclone (reprend RemoteStorage)
    migrate.py            # migration one-shot des Parquet existants
  application/            # use-cases + orchestration
    config.py             # Config Pydantic (reprend daemon/config.py + JobSpecs)
    events.py             # Event, EventBus, EventPort
    operations.py         # backfill, stream, read, inventory
    jobs.py               # JobSpec, JobRun, Trigger, JobParams
    scheduler.py          # scheduler async + superviseur de streams
    registry.py           # Operation Registry (nom → schéma → callable)
    monitor.py            # santé/alertes (abonné EventPort)
  interfaces/             # driving (adaptateurs fins)
    cli/                  # Typer (asyncio.run)
    api/                  # FastAPI (routes 1:1 + SSE)
    ui/                   # templates/static (reprend l'UI, via API)
  __init__.py             # façade publique (Client async)
```

Anciens `histo_dl/`, `continuous_dl/`, `daemon/` : supprimés en fin de migration (P8).

---

## 2. Milestones

| Jalon | Contenu | Phases |
|---|---|---|
| **M1 — socle pilote** | Archi complète + Binance/Kraken/Coinbase, backfill+stream+3 types, UI/API/CLI, migration données | P0→P6 |
| **M2 — v3.0** | Bybit/OKX/Bitfinex/BitMEX + nettoyage + release | P7→P8 |
| **M3 — extensions** | MCP, dérivation Kraken OHLC, marchés dérivés (funding/OI), auth | (post-v3.0) |

Règle à chaque ticket : **`pytest` vert + `ruff` + `mypy` clean** ; pas de
régression sur ce qui est déjà porté.

---

## 3. Phases & tickets

Format : `ID [taille] — intitulé` · *dépend de* · **fichiers** · notes · ✅ critère.
Tailles : XS<2 h · S ½-1 j · M 1-3 j · L 3-7 j.

### P0 — Fondations (socle technique)

- **P0-1 [S] Branche + squelette de package** — · **`dccd/**` (arbo §1, modules vides
  + `__init__`)** · créer `feat/refonte-v3` depuis `develop` ; poser l'arbo
  hexagonale. ✅ import du package OK, arbo en place.
- **P0-2 [S] Dépendances & Python 3.11+** — · **`pyproject.toml`** · `requires-python>=3.11` ;
  ajouter `httpx` ; retirer `requests` du cœur ; garder `websockets`, `pydantic>=2`,
  `polars`/`pyarrow`, `apscheduler`, `typer`, `pyyaml`, `tqdm` ; revoir les extras
  (`io`, `daemon`, `dev`). ✅ `pip install -e ".[dev]"` OK sur 3.11–3.13.
- **P0-3 [S] CI & qualité** — *P0-2* · **`.github/workflows`, `pyproject.toml`** ·
  matrice 3.11/3.12/3.13 ; `ruff`, `mypy` (strict sur `domain/`), `pytest --cov`. ✅ CI verte sur la branche.

### P1 — Domaine (pur)

- **P1-1 [S] `Symbol` + normalisation** — · **`domain/symbol.py`** · `Symbol(base,quote,market)`,
  alias XBT→BTC ; `__str__`="BTC/USDT". Le **rendu par-exchange** vit dans les adapters
  (pas ici). ✅ tests parse/normalise (XBT, casse).
- **P1-2 [S] `DataType` + extension** — · **`domain/types.py`** · enum fermé +
  mécanisme d'enregistrement documenté (sans l'utiliser). ✅ tests.
- **P1-3 [M] Records canoniques** — *P1-1/2* · **`domain/records.py`** · `OHLCBar`,
  `Trade`, `OrderBookLevel`, `OrderBookSnapshot` (ts **ns**, `side` typé, `is_snapshot`).
  Reprend/raffine `models.py`. ✅ tests de validation.
- **P1-4 [S] `DatasetId` + `Provenance`** — *P1-1/2* · **`domain/dataset.py`** ·
  identité dataset + provenance (source, derived_from). ✅ tests (hashable/frozen).
- **P1-5 [S] `timeutils` ns** — · **`domain/timeutils.py`** · conversions ns↔datetime,
  alignement au span, reprend `tools/date_time.py` (incl. mappers d'intervalles). ✅ tests + doctests.
- **P1-6 [M] Transforms pures** — *P1-3/5* · **`domain/transforms.py`** ·
  `aggregate_ohlc(trades, span)` (reprend `process_data.set_ohlc`), agrégation book.
  **Zéro I/O.** ✅ tests trades→OHLC (cas connus).
- **P1-7 [XS] Erreurs** — · **`domain/errors.py`** · `NoCapability`, `CoverageError`, etc. ✅.

### P2 — Transport (async)

- **P2-1 [M] Client HTTP async** — *P0-2* · **`transport/http.py`** · wrapper `httpx.AsyncClient`,
  retry/backoff (reprend la logique `tenacity` de `_fetch`), timeouts, erreurs typées. ✅ tests (mock httpx).
- **P2-2 [M] Base WebSocket async** — · **`transport/ws.py`** · reprend `BasisWebSocket`
  en async natif : connexion, reconnexion exponentielle, checkpoint, hooks on_message. ✅ tests.
- **P2-3 [M] RateLimiter** — · **`transport/ratelimit.py`** · token-bucket par exchange,
  paramétré par capacité (req/s, poids). ✅ tests (débit respecté).
- **P2-4 [M] Paginator générique** — *P1, P2-3* · **`transport/paginate.py`** ·
  boucle forward (start/fromId/since) **et** backward (curseur), fenêtre =
  `max_per_request × span` ; pilote des `fetch_*_page`. **Supprime tout chunking
  par-exchange** (généralise le fix Coinbase). ✅ tests forward+backward+limites.

### P3 — Ports & stockage

- **P3-1 [S] Définition des ports** — *P1* · **`sources/base.py`, `storage/base.py`,
  `application/events.py`** · `Source` + protocoles fins (`OHLCHistory`,
  `TradesHistory`, `OrderBookSnapshotREST`, `OHLCLive`, `TradesLive`, `OrderBookLive`),
  `StoragePort`, `RunsStorePort`, `EventPort`. ✅ mypy OK, runtime_checkable.
- **P3-2 [L] StoragePort Parquet** — *P3-1* · **`storage/parquet.py`** · reprend
  `DataStore` ; **ns** ; provenance en métadonnées Parquet ; layout : OHLC annuel,
  trades/book **journalier** ; `write/read/missing_intervals/last_timestamp/inventory`.
  Dédup à l'écriture (`tid` sinon `(ts,price,amount,side)`). ✅ tests (round-trip, gaps, dédup).
- **P3-3 [M] RunsStore SQLite** — *P3-1* · **`storage/runs_sqlite.py`** · tables
  `runs` (JobRun) + `metrics` (par dataset) ; WAL ; requêtes (derniers runs, erreurs,
  métriques). ✅ tests (insert/append/requêtes/concurrence).
- **P3-4 [M] Migration des Parquet** — *P3-2* · **`storage/migrate.py`** + CLI
  `dccd migrate` · `TS(s)→ns`, ajoute provenance, reformat au nouveau layout ;
  idempotent, dry-run. ✅ test sur jeu d'exemple (vérif lignes/timestamps).
- **P3-5 [S] Adapter sync rclone** — · **`storage/remote.py`** · reprend `RemoteStorage`. ✅ tests (mock rclone).

### P4 — Sources pilotes + resolver

- **P4-1 [M] Registry + Resolver** — *P3-1* · **`sources/base.py`, `sources/resolver.py`** ·
  registre `exchange→adapter` ; resolver `(target, operation, params)→Source`
  tenant compte de `Capability.history` (backfill profond impossible → `NoCapability`
  tôt). ✅ tests (sélection, NoCapability).
- **P4-2 [L] Adapter Binance** — *P2, P4-1* · **`sources/binance.py`** · capacités
  (klines 1000, aggTrades full, depth, WS trade/depth/kline) ; `fetch_*_page`,
  `stream_*` ; rendu symbole `BTCUSDT`. Reprend les parsers de `histo_dl/continuous_dl`.
  ✅ contract tests (fixtures enregistrées).
- **P4-3 [L] Adapter Coinbase** — *P2, P4-1* · **`sources/coinbase.py`** · candles
  (300, granularités figées → **Paginator**), trades (curseur 100), book L2/L3, WS
  matches/level2/candles ; symbole `BTC-USD`. ✅ contract tests (dont pagination 300).
- **P4-4 [L] Adapter Kraken** — *P2, P4-1* · **`sources/kraken.py`** · OHLC **récent**
  (720, `history="recent"`), Trades `since=0` (full), Depth, WS v2 trade/book ; symbole
  `XBT`/clés réponse. **OHLC profond → `NoCapability`** (dérivation différée M3). ✅ contract tests.

### P5 — Application / service

- **P5-1 [M] Config** — *P1* · **`application/config.py`** · reprend `daemon/config.py` ;
  `settings` + `remotes` + `alerts` + **`jobs: list[JobSpec]`** (déplie multi-paires) ;
  `load_config`/validation. ✅ tests (load, dépliage, invalides).
- **P5-2 [M] EventBus** — *P3-1* · **`application/events.py`** · `Event`
  (Progress/Log/Status), bus pub-sub, `events.for_run(run)`. ✅ tests.
- **P5-3 [L] Operations** — *P2, P3, P4-1, P5-2* · **`application/operations.py`** ·
  `backfill` (paginate→store, `start` last/origin/ts, reprend retry/cancel/progress),
  `stream` (supervisé, reconnect, snapshots book périodiques), `read`, `inventory`. ✅ tests (mock source/store).
- **P5-4 [M] Modèle Job** — *P5-1* · **`application/jobs.py`** · `JobSpec`/`JobRun`/
  `Trigger`/`JobParams` (noyau §7bis) ; runs persistés via `RunsStorePort`. ✅ tests.
- **P5-5 [L] Scheduler + superviseur** — *P5-3/4* · **`application/scheduler.py`** ·
  orchestre les specs (`supervised`→supervisor stream ; `interval/cron`→planif ;
  `once`→immédiat) ; reprend APScheduler (mode async) + supervision stream
  (reprend `stream_manager`). ✅ tests (planif, supervision, stop/restart).
- **P5-6 [M] Operation Registry** — *P5-3* · **`application/registry.py`** · nom→(schéma in/out, callable) ;
  base des bindings interfaces + test de parité. ✅ tests.
- **P5-7 [M] Monitor/alertes** — *P5-2, P3-3* · **`application/monitor.py`** · abonné
  EventPort ; métriques→RunsStore ; alertes webhook sur N erreurs (reprend `HealthMonitor`). ✅ tests.
- **P5-8 [S] Façade publique** — *P5-3/5* · **`dccd/__init__.py`** · `Client` async
  (`async with Client(config) as c: await c.backfill(...)`) = mode « composants embarqués ». ✅ doctest/example.

### P6 — Interfaces (parité)

- **P6-1 [L] API HTTP** — *P5-6* · **`interfaces/api/`** · FastAPI, routes **1:1**
  sur le registre d'opérations + endpoints jobs/inventory/config + **SSE** pour les
  events. Reprend/adapte `daemon/api.py`. ✅ tests (TestClient, SSE).
- **P6-2 [M] UI sur l'API** — *P6-1* · **`interfaces/ui/`** · reprend templates/static
  (lots 1-3) ; **pur client HTTP** ; progression via SSE (règle TODO 2bis.1). ✅ pages rendent, actions OK.
- **P6-3 [M] CLI** — *P5-8* · **`interfaces/cli/`** · Typer, commandes 1:1 (asyncio.run) :
  `backfill`, `stream`, `start`, `status`, `inventory`, `migrate`, `validate`. ✅ tests CLI.
- **P6-4 [S] Test de parité** — *P6-1/3* · **`tests/`** · asserte : chaque opération
  du registre a un binding API + CLI. ✅ test rouge si drift.
- **P6-5 [S] `dccd start` (daemon async)** — *P5-5, P6-1* · **`interfaces/cli/`** ·
  boucle asyncio : scheduler + streams + sync + UI embarquée. ✅ démarre/arrête proprement.

### P7 — Exchanges restants

- **P7-1 [L] Bybit** — *P4* · **`sources/bybit.py`** · kline 1000, **trades spot 60
  récents** (`history="recent"`, pas d'histo profond), orderbook [1,200], WS
  publicTrade/orderbook/kline. ✅ contract tests (dont NoCapability trades histo).
- **P7-2 [L] OKX** — *P4* · **`sources/okx.py`** · candles 300 + history-candles 100,
  trades 500 + history-trades, books sz 400, WS trades/books/candle. ✅ contract tests.
- **P7-3 [L] Bitfinex** — *P4* · **`sources/bitfinex.py`** · candles/trades 10000,
  book P0-P4, symbole `tBTCUSD`, WS. ✅ contract tests.
- **P7-4 [L] BitMEX** — *P4* · **`sources/bitmex.py`** · trade/bucketed (1m/5m/1h/1d,
  1000), trade full, orderBookL2, WS. ✅ contract tests.

### P8 — Bascule & nettoyage

- **P8-1 [M] Suppression de l'ancien** — *P6, P7* · **`dccd/histo_dl,continuous_dl,daemon`** ·
  supprimer ; purger imports/tests obsolètes ; `__init__` propre. ✅ pytest vert sans l'ancien.
- **P8-2 [M] Docs** — · **`CLAUDE.md`, `README.rst`, `doc/`, `examples/`** ·
  réécrire pour l'archi v3 (3 modes, nouvelle CLI/API, config). ✅ `make html` OK.
- **P8-3 [S] Déploiement** — · **`Dockerfile`, `examples/dccd.service`** · adapter au
  daemon async (TODO §3). ✅ image build + `dccd start` en conteneur.
- **P8-4 [S] Release v3.0** — *tout* · PR `feat/refonte-v3 → develop` ; CHANGELOG ;
  tag à la release. ✅ revue + CI verte.

---

## 4. Différé (M3, post-v3.0)

- **MCP** : `interfaces/mcp/` mappé sur le registre d'opérations.
- **Dérivation Kraken OHLC** : `sources/derive.py` `DerivedOHLCSource` + activation
  resolver (OHLC profond Kraken depuis trades stockés).
- **Marchés dérivés** : `DataType` funding/OI/liquidations + marché `perp` sur `Symbol`.
- **Auth/secrets** : injection dans `transport` pour endpoints privés.

---

## 5. Risques & points de vigilance (rappel REFONTE §10)

- Async « viral » : garder le **domaine sync pur**, async confiné transport+application.
- Sens de pagination hétérogène : couvert par le Paginator (forward+backward).
- Dédup trades sans `tid` (Kraken/Bybit) : ns + clé `(ts,price,amount,side)`.
- Streams : reconnexion/backpressure/flush partiel → superviseur robuste + checkpoint.
- Double collecte (2 process) : lockfile single-owner + dédup store.
- Tests : **contract tests par capacité** avec **fixtures enregistrées** (zéro réseau).
