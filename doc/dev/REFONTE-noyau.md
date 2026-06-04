# Refonte dccd — Noyau : modèles canoniques & ports

> Annexe de [`REFONTE.md`](REFONTE.md) §4–§5 et §8. Esquisses de contrats (Python
> illustratif, pas figé). Objectif : poser une **base solide** confrontée aux
> irrégularités relevées dans [`REFONTE-capacites.md`](REFONTE-capacites.md).
> Réutilise les acquis dccd 2 (cf. REFONTE §8bis).

---

## 1. Conventions transverses

- **Temps interne** : epoch **nanosecondes**, `int64`, **UTC** (DÉCIDÉ ①). OHLC
  `ts` = *open time* alignée au span. ns = uniforme + lossless (capture la ns
  native Kraken, évite les collisions de trades sans `tid`) + **unité native
  pandas** (`datetime64[ns]`) ; coût stockage nul (int64 dans tous les cas).
  (dccd 2 mélange secondes/ms → migration one-shot des Parquet : `TS *= 1e9`.)
  Helpers de conversion repris de `tools/date_time.py`.
- **Span** : `int` en secondes en interne ; chaque adapter le mappe vers
  l'intervalle exchange (réutilise `binance_interval`, `bybit_interval`,
  `okx_interval`, `str_to_span`…).
- **Symbol** : `base`/`quote` normalisés (XBT→BTC) ; le **rendu par-exchange**
  vit dans l'adapter, jamais dans le modèle.
- **Provenance** : portée au niveau **dataset/fichier**, pas par enregistrement
  (garde les lignes légères) → métadonnées Parquet ou sidecar.

---

## 2. Modèles de domaine (purs, sync, zéro I/O)

```python
# domain/symbol.py
class Symbol(BaseModel):
    base: str                       # "BTC" (normalisé, XBT→BTC)
    quote: str                      # "USDT"
    market: Literal["spot"] = "spot"  # extensible: "perp", "future"
    def __str__(self) -> str: return f"{self.base}/{self.quote}"

# domain/types.py
class DataType(str, Enum):          # cœur v3 ; extensible (cf. §8)
    OHLC = "ohlc"
    TRADES = "trades"
    ORDERBOOK = "orderbook"

# domain/records.py  (raffinement des modèles dccd 2)
class OHLCBar(BaseModel):
    ts: int                         # open time, ns UTC, alignée au span
    open: float; high: float; low: float; close: float
    volume: float                   # base asset
    quote_volume: float | None = None
    trades: int | None = None       # nb de trades dans la barre, si dispo

class Trade(BaseModel):
    ts: int                         # ns UTC
    price: float
    amount: float                   # base asset
    side: Literal["buy", "sell"] | None = None
    tid: str | None = None          # str: certains exchanges non-entiers

class OrderBookLevel(BaseModel):
    price: float
    amount: float
    count: int | None = None        # nb d'ordres au niveau, si dispo

class OrderBookSnapshot(BaseModel): # un état complet daté (≠ niveau isolé)
    ts: int                         # ns UTC
    bids: list[OrderBookLevel]      # tri décroissant
    asks: list[OrderBookLevel]      # tri croissant
    is_snapshot: bool = True        # False = delta à appliquer à l'état local

# domain/dataset.py
class DatasetId(BaseModel, frozen=True):
    exchange: str
    symbol: Symbol
    data_type: DataType
    span: int | None = None         # requis si OHLC, sinon None

class Provenance(BaseModel):
    source: str                     # ex. "binance:rest", "kraken:ws"
    derived_from: DatasetId | None = None  # si dérivé (ex. ohlc←trades)
```

**Raffinements vs dccd 2** : `date:float`→`ts:int` (ns) ; `Trade.type`→`side`
typé ; `OrderBookEntry` (niveau isolé) → `OrderBookSnapshot` (état daté
groupé bids/asks + flag snapshot/delta) — indispensable pour stocker/rejouer le
carnet (cf. constat « pas d'histo book gratuit »).

---

## 3. Déclaration de capacités

```python
# domain/capability.py
class Capability(BaseModel):
    data_type: DataType
    transport: Literal["rest", "ws"]
    mode: Literal["historical", "live"]
    # contraintes (None = sans objet)
    max_per_request: int | None = None              # plafond pagination
    page_direction: Literal["forward", "backward"] | None = None
    spans: list[int] | None = None                  # spans OHLC supportés (s)
    max_depth: int | None = None                    # profondeur book
    auth_required: bool = False
```

Exemple (Coinbase) : `Capability(OHLC, rest, historical, max_per_request=300,
page_direction="forward", spans=[60,300,900,3600,21600,86400])`.

---

## 4. Ports *driven* (adapters async)

Deux principes clés :
1. **L'adapter ne pagine pas et ne limite pas le débit.** Il expose un *fetch
   d'UNE page* + déclare ses capacités ; le `Paginator`/`RateLimiter` (couche
   application) orchestrent → le chunking par-exchange disparaît (généralise le
   fix Coinbase 300).
2. **Protocoles fins par (data_type × mode)** (DÉCIDÉ ②) : un adapter
   n'implémente que ce qu'il sait faire, **mypy l'enforce**, et la présence de
   méthode ne peut pas diverger des capacités. Pas de `NotSupported` runtime.

```python
# application/ports.py
@runtime_checkable
class Source(Protocol):                      # base commune, tout adapter l'a
    exchange: str
    def capabilities(self) -> list[Capability]: ...   # contraintes (limites, spans, depth)
    def render_symbol(self, s: Symbol) -> str: ...

# --- protocoles de capacité (l'adapter compose ceux qu'il supporte) ---
@runtime_checkable
class OHLCHistory(Protocol):
    async def fetch_ohlc_page(self, s, span, start, end, limit) -> list[OHLCBar]: ...
@runtime_checkable
class TradesHistory(Protocol):
    async def fetch_trades_page(self, s, start, end, cursor, limit) -> Page[Trade]: ...
@runtime_checkable
class OrderBookSnapshotREST(Protocol):
    async def fetch_orderbook(self, s, depth) -> OrderBookSnapshot: ...
@runtime_checkable
class OHLCLive(Protocol):
    def stream_ohlc(self, s, span) -> AsyncIterator[OHLCBar]: ...
@runtime_checkable
class TradesLive(Protocol):
    def stream_trades(self, s) -> AsyncIterator[Trade]: ...
@runtime_checkable
class OrderBookLive(Protocol):
    def stream_orderbook(self, s, depth) -> AsyncIterator[OrderBookSnapshot]: ...

class Page(BaseModel, Generic[T]):           # curseur opaque (DÉCIDÉ ⑤)
    items: list[T]
    next_cursor: str | None = None           # encodé/décodé DANS l'adapter
```

Exemple (cf. `REFONTE-capacites.md`) — Bybit spot n'a pas d'histo trades :

```python
class Bybit(Source, OHLCHistory, TradesLive, OrderBookLive):  # PAS TradesHistory
    ...

# le resolver vérifie par type, pas à l'exécution :
if isinstance(src, OHLCHistory): bars = paginate_ohlc(src, ...)
else: raise NoCapability(...)   # tôt, pas un NotSupported tardif
```

`capabilities()` reste nécessaire pour les **contraintes numériques**
(`max_per_request`, `spans`, `max_depth`) ; les **protocoles** portent la
présence de capacité. Les deux ne peuvent plus se contredire silencieusement.

```python
class StoragePort(Protocol):
    async def write(self, ds: DatasetId, records: Sequence[Record], prov: Provenance) -> int: ...
    async def read(self, ds: DatasetId, start: int, end: int) -> AsyncIterator[Record]: ...
    def missing_intervals(self, ds, start, end) -> list[tuple[int, int]]: ...
    def last_timestamp(self, ds) -> int | None: ...
    def inventory(self) -> list["DatasetInfo"]: ...
# → implémenté par DataStore (dccd 2), déjà solide.

class EventPort(Protocol):
    def emit(self, ev: "Event") -> None: ...
# Event ∈ { Progress(done,total,unit), Log(level,msg), Status(job_id,state) }
# Adapters : API→SSE/WS, CLI→tqdm, UI←API. (règle le doublon tqdm/console)
```

---

## 5. Paginator & RateLimiter (couche application)

```python
async def paginate_ohlc(src, ds, start, end, *, events) -> AsyncIterator[OHLCBar]:
    cap = capability_for(src, ds.data_type, "rest", "historical")
    step = cap.max_per_request * ds.span * 1_000_000_000  # fenêtre en ns
    cur = align(start, ds.span)
    total = (end - cur) // step + 1; done = 0
    while cur < end:
        chunk_end = min(cur + step, end)
        async with rate_limiter(src.exchange):           # débit centralisé
            bars = await src.fetch_ohlc_page(ds.symbol, ds.span, cur, chunk_end,
                                             cap.max_per_request)
        for b in bars: yield b
        cur = chunk_end; done += 1
        events.emit(Progress(done, total, "windows"))
```

- `page_direction` détermine forward (start→end) vs backward (curseur).
- `RateLimiter` paramétré par exchange (poids/req, req/s) — à chiffrer (capacités).
- **Un seul** code de pagination pour tous les exchanges/types.

---

## 6. La dérivation est une `Source`

```python
class DerivedOHLCSource:                 # implémente Source + OHLCHistory
    """ OHLC dérivé des Trades stockés. Cas Kraken (pas d'histo OHLC REST). """
    def capabilities(self): return [Capability(OHLC, "rest", "historical", spans=[...])]
    async def fetch_ohlc_page(self, s, span, start, end, limit):
        trades = self.store.read(DatasetId(self.exchange, s, TRADES), start, end)
        return aggregate_ohlc(trades, span)   # réutilise process_data.set_ohlc
```

→ uniforme : le resolver traite `DerivedOHLCSource` comme n'importe quelle source.
**Ce n'est pas une opération** (§7) : un `backfill` OHLC dérive de façon
transparente quand l'histo natif manque (Kraken) ou pour un span non natif.
Le **store lui-même** est exposable en source historique (relecture des flux WS
déjà collectés → seul moyen d'avoir de l'histo carnet).

---

## 7. Operations (couche service) — esquisse

**Deux opérations seulement** (DÉCIDÉ) + des requêtes de lecture. Toutes prennent
un `EventPort` et n'utilisent que les ports ci-dessus.

```python
async def backfill(req: BackfillReq, *, src, store, events) -> Report  # historique → store
async def stream(req: StreamReq, *, src, store, events) -> Handle      # live, supervisé, reconnect
async def read(req: ReadReq, *, store) -> AsyncIterator[Record]
def inventory(*, store) -> list[DatasetInfo]
```

`backfill`/`stream` = les *kinds* d'un **Job** (§7bis), mappés 1:1 par CLI/API/MCP (§8).

- **`update` n'est pas une opération** : c'est `backfill(start="last")`. Le bouton
  « Update » de l'UI et un histo job planifié appellent `backfill`.
- **`derive` n'est pas une opération** : la dérivation est une **`Source`** (§6)
  que le resolver choisit pendant un `backfill` (ex. OHLC Kraken). Transparent.

---

## 7bis. Jobs : spec, run, exécution

Le point qui unifie scheduler / backfill / histo job / stream job (REFONTE §7).
On sépare **ce qui est voulu** (`JobSpec`) de **ce qui se passe** (`JobRun`).

### a) JobSpec — la définition (déclarative)

```python
# application/jobs.py
class Trigger(BaseModel):                       # union discriminée par 'kind'
    kind: Literal["once", "interval", "cron", "supervised"]
    at: int | None = None        # once   : départ planifié (ns) ; None = maintenant
    every: int | None = None     # interval : période en secondes (≈ span pour update)
    cron: str | None = None      # cron   : expression
    # supervised : aucun champ — always-on tant que enabled (streams)

class JobTarget(BaseModel):                     # = un DatasetId résolu
    exchange: str
    symbol: Symbol
    data_type: DataType
    span: int | None = None      # requis si OHLC

class JobParams(BaseModel):                     # spécifiques à l'opération (tous optionnels)
    start: int | Literal["last", "origin"] = "last"  # "last"=depuis dern. bougie (défaut/update),
                                                      # "origin"=début, <ts ns>=date choisie
    parallel: bool = False                # backfill : paires en parallèle
    depth: int | None = None              # book : nb de niveaux (top-K)
    snapshot_interval: int | None = None  # stream book : N s entre snapshots stockés
    derive_from: DataType | None = None   # derive : type source (ex. trades)
    transport: Literal["rest", "ws"] | None = None  # épingle la méthode ; sinon resolver

class JobSpec(BaseModel):
    id: str                                # slug stable : f"{op}:{exchange}:{sym}:{type}[:{span}]"
    operation: Literal["backfill", "stream"]
    target: JobTarget
    trigger: Trigger
    params: JobParams = JobParams()
    enabled: bool = True                   # toggle (remplace le CRUD add/remove)
    origin: Literal["config", "runtime"] = "config"
```

Un **multi-paires** du config (ex. `histo_job` avec 6 pairs) se **déplie en N
JobSpecs** au chargement (un par dataset) → un run = un dataset, exécution simple.

### b) JobRun — une exécution

```python
class RunState(str, Enum):
    PENDING = "pending"; RUNNING = "running"; RECONNECTING = "reconnecting"
    SUCCEEDED = "succeeded"; FAILED = "failed"; CANCELLED = "cancelled"

class JobRun(BaseModel):
    run_id: str                  # f"{spec_id}@{ts}"
    spec_id: str
    operation: str
    target: JobTarget
    state: RunState
    started_at: int | None = None; ended_at: int | None = None   # ns
    progress: dict | None = None # {done, total, unit} (ops finies)
    rows_written: int = 0
    error: str | None = None
    log_tail: list[str] = []     # borné (cf. BackfillTracker actuel)
```

Finie (backfill/update/derive) : `PENDING→RUNNING→SUCCEEDED|FAILED|CANCELLED`.
Stream (supervisé) : `RUNNING↔RECONNECTING`, jusqu'à `CANCELLED` (stop) ou `FAILED`.

### c) Correspondance avec dccd 2

| dccd 2 | JobSpec équivalent |
|---|---|
| `histo_job` (config) | `backfill(start="last")`, `trigger=interval(every≈span)`, origin=config |
| rattrapage profond | `backfill(start="origin")`, `trigger=once` |
| `stream_job` (config) | `stream`, `trigger=supervised`, `params.depth/snapshot_interval` |
| backfill ad-hoc (UI) | `backfill(start=<date>)`, `trigger=once`, `origin=runtime` |
| `KrakenBackfill` | `backfill` ohlc + le **resolver** choisit `DerivedOHLCSource` |

→ Une **seule liste de jobs** (les specs) ; l'« activité backfill » du dashboard
= les `JobRun` d'`operation=backfill` ; l'état scheduler = les specs planifiées
actives. Plus de 4 notions distinctes.

### d) Exécution (scheduler = orchestrateur de specs)

```python
def build(specs, *, scheduler, supervisor):
    for spec in (s for s in specs if s.enabled):
        if spec.trigger.kind == "supervised":          # streams
            supervisor.ensure_running(spec)            # démarre + maintient en vie
        elif spec.trigger.kind in ("interval", "cron"):
            scheduler.add(spec.id, when=spec.trigger, fire=lambda s=spec: run_job(s))
        elif spec.trigger.kind == "once":
            run_job(spec)                              # immédiat (ou à 'at')

async def run_job(spec) -> JobRun:
    run = runs.create(spec)                            # PENDING→RUNNING
    src = resolver.resolve(spec.target, spec.operation, spec.params)  # choisit Source/dérivée
    op  = OPERATIONS[spec.operation]                   # backfill | stream
    try:
        report = await op(spec, src=src, store=store, events=events.for_run(run))
        runs.finish(run, SUCCEEDED, report)
    except Cancelled: runs.finish(run, CANCELLED)
    except Exception as e: runs.finish(run, FAILED, error=e)
    return run
```

### e) Rôle du resolver (où la capacité rencontre l'opération)

`resolver.resolve(target, operation, params)` choisit la `Source` qui implémente
le protocole requis (§4), **en tenant compte de la portée historique** :

- `backfill` OHLC **Kraken** (deep) → l'adapter Kraken n'offre que du récent (720)
  → bascule sur `DerivedOHLCSource` (depuis les trades). Si pas de `TradesHistory`
  non plus → `NoCapability` **tôt** (pas un échec tardif).
- `update` OHLC Kraken (delta court) → l'OHLC natif récent suffit.
- `backfill` trades **Bybit spot** → ni `TradesHistory` ni dérivation possible
  → `NoCapability` explicite (seul le `stream` alimente l'histo trades Bybit).

→ La `Capability` doit donc porter la **portée** (`history: "full" | "recent"`)
en plus des limites numériques — à ajouter (cf. §8 « reste à affiner »).

### f) Persistance

- **JobSpecs `origin=config`** : dans `config.yml` (état stable, versionnable).
  L'édition passe par la page Config (pas de CRUD dédié — cf. décision Web UI).
- **JobRuns** (+ specs `origin=runtime`) : un **runs store** qui unifie
  `metrics.json` + `backfill_jobs.json` actuels. *(JSON aujourd'hui ; SQLite si on
  veut requêter l'historique des runs — à trancher, faible enjeu.)*

---

## 8. Décisions actées

1. **Temps interne : ns partout** (`int64`, UTC). Uniforme + lossless +
   pandas-native ; migration Parquet OHLC `TS *= 1e9` (one-shot).
2. **Ports : protocoles fins** par (data_type × mode) — `OHLCHistory`,
   `TradesHistory`, `OrderBookSnapshotREST`, `OHLCLive`, `TradesLive`,
   `OrderBookLive`. mypy-enforced, pas de `NotSupported` runtime. `capabilities()`
   porte les contraintes numériques.
3. **`DataType` : enum fermé** (`ohlc`/`trades`/`orderbook`) + point d'extension
   documenté. Le registry ouvert viendra avec les dérivés (funding/OI), pas avant.
4. **Order book : snapshots périodiques** top-K à intervalle N (≈ comportement
   dccd 2, volume borné, relecture triviale). Le flag `is_snapshot` est conservé
   dans le modèle → les deltas horodatés (fidélité totale) restent ajoutables
   plus tard sans casser le schéma.
5. **Curseur de pagination : `str` opaque**, encodé/décodé dans l'adapter ;
   l'application reste agnostique.
6. **Transformations pures** : `aggregate_ohlc`/`process_data.*` = compute seul
   (polars OK), zéro I/O, dans le domaine — réutilisées par adapters ET dérivation.
7. **Opérations : `backfill` + `stream`** uniquement (+ requêtes `read`/`inventory`).
   `update` = `backfill(start="last")` ; `derive` = une `Source`, pas une
   opération. Pas d'objets/méthodes redondants.

### Reste à affiner (itération suivante)
- Ajouter `history: "full" | "recent"` à `Capability` (le resolver en a besoin
  pour basculer backfill→dérivation, cf. §7bis.e).
- Table complète des spans/intervalles natifs par exchange (pour `capabilities`).
- Rate limits chiffrés par endpoint (pour le `RateLimiter`).
- Runs store : JSON (actuel) vs SQLite (requêtable) — faible enjeu.
- Stratégie de migration des Parquet existants (script `TS *= 1e9`).
