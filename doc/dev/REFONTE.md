# Refonte dccd — Plan d'architecture (v0, synthétique)

> Document de travail. Objectif de cette première version : **valider
> l'architecture globale**. Les détails (par exchange, schémas précis,
> signatures) seront affinés ensuite. On garde volontairement court et net.

---

## 1. Objectifs

1. **Quatre interfaces iso-fonctionnelles** : UI, CLI, API HTTP, MCP (ce dernier
   **différé**, mais l'archi le prévoit — §11.3). Une feature ajoutée une fois
   doit être accessible depuis toutes. → **mutualiser la logique métier dans une
   seule couche**, les interfaces ne sont que des adaptateurs fins.
2. **Données harmonisées** : une seule façon canonique de représenter et de
   récupérer une donnée pour un exchange donné. Fini les deux chemins parallèles
   (`histo_dl` vs `continuous_dl`) qui font « presque » la même chose.
3. **Trois types de données** (extensible) : `ohlc`, `trades`, `orderbook`.
4. **Plusieurs méthodes d'acquisition par exchange** : REST vs WebSocket,
   historique vs flux continu — modélisées explicitement, pas dispersées.
5. **Dérivation** : quand c'est possible, reconstruire une donnée à partir d'une
   autre (ex. OHLC dérivé des trades) et **relire l'historique déjà collecté**
   comme une source.

### Non-objectifs (pour l'instant)
- Trading / exécution d'ordres.
- Endpoints privés / authentifiés (à anticiper dans le design, pas à livrer).
- Big-bang : la migration sera progressive (voir §9).

---

## 2. Le problème actuel (dccd "2") en une phrase

La logique métier est **dupliquée et dispersée** : deux arbres (`histo_dl`,
`continuous_dl`), des règles propres à chaque exchange réimplémentées (pagination,
chunking — cf. le bug Coinbase 300), et des interfaces (CLI, daemon, UI) qui
**réimplémentent chacune** des bouts de logique au lieu d'appeler un noyau commun.
Résultat : incohérences, features partielles selon l'interface, et un modèle
mental flou (scheduler vs backfill vs histo job vs stream job).

---

## 3. Principe directeur : architecture hexagonale (ports & adapters)

Cadre adopté. Correspondance des couches :
- **Domaine** (pur, sync, zéro I/O) : modèles canoniques, `Symbol`, règles de
  capacité, logique de dérivation.
- **Application** : les *Operations* (use-cases) + **ports** *driven* :
  `SourcePort`, `StoragePort`, `EventPort`.
- **Adapters** : *driving* (CLI, API, MCP, UI-via-API) ; *driven* (sources
  exchange, store Parquet, transport HTTP/WS). L'async vit ici + dans
  l'application ; le domaine reste pur.


```
            ┌──────────────────────────────────────────────┐
 Interfaces │   CLI      API HTTP      MCP         UI(*)     │  ← adaptateurs fins
            └────┬─────────┬────────────┬───────────┬────────┘
                 │         │            │           │ (HTTP)
                 ▼         ▼            ▼           ▼
            ┌──────────────────────────────────────────────┐
 Service    │  OPERATIONS (use-cases) : contrat unique      │  ← TOUTE la logique
            │  backfill · stream  (+ read · inventory)       │     applicative
            │  + Job model + Event/Progress bus              │
            └────┬───────────────┬───────────────┬──────────┘
                 ▼               ▼               ▼
            ┌─────────┐    ┌────────────┐   ┌──────────┐
 Domaine    │ Sources │    │  Derive    │   │ Storage  │
            │ (adapt. │    │ (graphe de │   │  (port)  │
            │ exchange)│   │ transfo.)  │   │          │
            └────┬────┘    └────────────┘   └──────────┘
                 ▼
            ┌─────────────────────────────────────┐
 Transport  │ HTTP client · WS client · RateLimiter│
            │ · Paginator (chunking générique)     │
            └─────────────────────────────────────┘
```

(*) **L'UI passe par l'API HTTP**, elle n'appelle jamais le service en direct.
CLI, API et MCP appellent les **Operations** en process. → la parité des 4
interfaces se réduit à : « chaque opération a un binding dans chaque interface ».

**Règle d'or** : aucune logique métier dans les interfaces. Si on est tenté d'en
mettre, elle descend dans une Operation.

---

## 4. Couche domaine : modèles canoniques

Une seule représentation interne, quelle que soit la source.

- `Symbol(base, quote)` — normalisation centralisée (alias `XBT→BTC`, rendu
  par-exchange `BTC/USDT`↔`BTC-USDT`↔`XBTUSD`).
- `DataType` — **cœur v3** : `ohlc`, `trades`, `orderbook` (+ éventuellement
  `ticker`). **Enum fermé + point d'extension documenté** (déc. noyau §8.3) : ajouter
  plus tard funding/OI/liquidations = nouveau modèle canonique + capacité
  d'adapter, **sans rearchitecturer**. NB : ces dernières impliquent de passer en
  marché **dérivés** (dccd est spot-only aujourd'hui) → extension de périmètre.
- Modèles Pydantic (raffinés depuis `models.py`) : `OHLCBar`, `Trade`,
  `OrderBookSnapshot` (état daté groupé). Détail : noyau §2.
- **Provenance** au niveau **dataset/fichier** (pas par enregistrement, pour
  garder les lignes légères) : `source` (exchange+méthode) et `derived_from`
  (si dérivé). Évite de mélanger natif et reconstruit.

Tout adaptateur de source **doit** émettre ces modèles canoniques — c'est le
point d'harmonisation des données.

> 🧩 **Esquisses concrètes** des modèles canoniques, des ports
> (`SourcePort`/`StoragePort`/`EventPort`), du `Paginator` et de la dérivation :
> voir [`REFONTE-noyau.md`](REFONTE-noyau.md).

---

## 5. Sources & matrice de capacités

Au lieu de classes par (exchange × histo/stream), **un adaptateur par exchange**
qui **déclare ses capacités** :

| Champ | Valeurs |
|------|---------|
| `data_type` | ohlc / trades / orderbook |
| `transport` | rest / websocket |
| `mode` | historical / live |
| `constraints` | max items/page, spans supportés, profondeur max, auth requise, rate limits |

Exemple (illustratif) :

| Exchange | ohlc rest histo | trades rest histo | trades ws live | book ws live |
|---|---|---|---|---|
| Binance | ✅ (1000/page) | ✅ | ✅ | ✅ |
| Coinbase | ✅ (300/page) | recent only | ✅ | ✅ |
| Kraken | ⚠️ 720 récents (→dérivé) | ✅ | ✅ | ✅ |

Un **Registry** mappe `exchange → adaptateur + capacités`. Un **Resolver** prend
une *requête* (exchange, symbol, data_type, mode/transport souhaités, fenêtre) et
choisit l'adaptateur, ou répond « impossible nativement → dérivation possible via
X » (cf. §6). L'utilisateur ne choisit plus une classe : il décrit ce qu'il veut.

> 📋 **Matrice de capacités réelle** (7 exchanges, croisée code + doc API
> officielle juin 2026) : voir [`REFONTE-capacites.md`](REFONTE-capacites.md).
> Constats clés qui en sortent :
> - **Aucun historique d'order book gratuit** → le store des flux WS devient la
>   source historique (alimente §6).
> - **Kraken : pas d'histo OHLC REST** (720 récents) → dérivation Trades→OHLC.
> - **Bybit spot : pas d'histo trades** (60 récents) → forward-collect WS only.
> - **Plafonds pagination 60 → 10000** + sens hétérogène → Paginator central
>   paramétré par capacité (généralise le fix Coinbase 300).

---

## 6. Dérivation & relecture de l'historique

- **Graphe de dérivation** déclaratif : `trades → ohlc` (agrégation par span),
  extensible (ex. `trades → volume bars`, `book → spread series`).
- Le **store lui-même est une source historique** : une donnée déjà collectée
  (y compris streamée) peut être relue, et servir de base à une dérivation.
  → « récupérer l'histo depuis une data » = (a) lire le store, (b) dériver.
- Garde-fou : une dérivation exige une **couverture suffisante** de la source
  (pas de trous), sinon on marque le résultat incomplet plutôt que de produire un
  OHLC faux. Provenance `derived_from` obligatoire.

---

## 7. Modèle unifié des "jobs" (résout l'incohérence scheduler/backfill/stream)

Aujourd'hui : scheduler, backfill, histo job, stream job semblent 4 choses
distinctes. **Proposition : un seul modèle.**

- **Dataset** = `(exchange, symbol, data_type)` — la donnée voulue.
- **Operation** sur un dataset (= le contrat du service) — **2 seulement** :
  - `backfill` — historique → store (chunké, reprenable). `start="last"` = l'ancien
    « update » (poll du delta) ; `start="origin"` ou date = rattrapage profond.
  - `stream` — collecte continue temps réel (supervisée).
  - + requêtes : `read`, `inventory`.
  - `update` et `derive` **ne sont pas** des opérations : `update`=`backfill(start="last")`,
    `derive`=une `Source` que le resolver choisit pendant un backfill (§6).
- **Job** = une Operation + un déclenchement :
  - one-shot (`backfill` manuel),
  - planifié (`backfill(start="last")` périodique = l'« histo job » d'aujourd'hui),
  - supervisé long-running (`stream` = le « stream job » d'aujourd'hui).
- Le **scheduler** n'est qu'un orchestrateur de Jobs planifiés/supervisés.

→ « histo job » et « stream job » deviennent deux *kinds* du même objet Job ;
toute la collecte historique passe par **une** opération `backfill`. Vocabulaire
et UX cohérents, sans objets redondants.

> 🧩 **Schéma détaillé** (`JobSpec`/`JobRun`, triggers, états, exécution,
> correspondance dccd 2, rôle du resolver) : voir
> [`REFONTE-noyau.md`](REFONTE-noyau.md) §7bis.

---

## 8. Interfaces : garantir la parité

- **Operation Registry** : chaque opération a un nom, un schéma d'entrée et de
  sortie (Pydantic). Source de vérité unique.
- **Bindings fins** :
  - **API** : routes FastAPI générées/mappées 1:1 sur les opérations.
  - **CLI** : commandes Typer mappées 1:1.
  - **MCP** : tools MCP mappés 1:1.
  - **UI** : consomme l'API (pas d'accès direct au service).
- **Test de parité** : un test asserte que *chaque* opération du registry possède
  un binding API + CLI + MCP. Empêche le drift (« la feature existe en CLI mais
  pas dans l'UI »).
- **Bus d'événements / progression** : les opérations longues émettent des
  événements (progress, log). Chaque interface les rend à sa façon :
  - API : SSE / WebSocket (et l'UI s'y abonne → règle le doublon tqdm/console,
    TODO 2bis.1),
  - CLI : barre tqdm,
  - MCP : handle + tool de statut.
  La progression n'est plus écrite « en dur » vers stdout.

---

## 8bis. Capital existant à réutiliser (dccd 2 n'est pas à jeter)

La refonte **restructure**, elle ne réécrit pas tout. Beaucoup de logique solide
de dccd 2 se replace telle quelle derrière les nouveaux ports :

| Asset dccd 2 | Réemploi dans la v3 |
|---|---|
| `models.py` (`OHLCBar`, `Trade`, `OrderBookEntry`) | Base des **modèles canoniques** (§4), à raffiner. |
| `storage.py` `DataStore` (Parquet annuel/journalier, `missing_intervals`, `last_timestamp`) | Implémentation de **`StoragePort`** — déjà robuste, à garder. |
| `process_data.py` (`set_ohlc/trades/marketdepth/orders`) | Logique de transfo réutilisée dans adapters + **dérivation**. |
| `tools/date_time.py` (`binance_interval`, `bybit_interval`, `okx_interval`, `str_to_span`…) | Mappers spans↔intervalles **par exchange**, réutilisables direct. |
| `daemon/backfill.py` (fenêtrage, retry/backoff, `KrakenBackfill` trades→OHLC, progress/stop callbacks) | À **extraire** en `Paginator` générique + dérivation + `EventPort`. |
| `daemon/scheduler.py`, `health.py`, `stream_manager.py` | Orchestration des **Jobs** (§7), supervision streams, monitoring. |
| `daemon/api.py` + `ui/` | Adapters **API** + **UI** (à rebrancher sur le service). |
| `daemon/storage.py` `RemoteStorage` (rclone) | Adapter de sync distant. |
| `tools/websocket.py` `BasisWebSocket` | Base des adapters WS. |
| **Parsers par exchange** (`histo_dl/*`, `continuous_dl/*`) | **Cœur des nouveaux adapters** : la connaissance des réponses API est déjà là. |
| `tests/conftest.py` + mocks par exchange | Fixtures **précieuses** à reprendre pour les tests de contrat. |

→ Le travail neuf porte surtout sur : les **ports/contrats**, le **Paginator/
RateLimiter** centralisés, le **registry de capacités**, la couche **service
(Operations + Jobs + EventBus)**, et le **rebranchement** des interfaces dessus.

## 8ter. Sujets transverses (placement dans l'hexagone)

Pour ne rien oublier — où vivent les concerns au-delà de la collecte :

- **Synchro distante (rclone)** : un *driven adapter* (réutilise `RemoteStorage`).
  **Pas** une opération de données : un **service de maintenance** planifié
  (intervalle) à côté des Jobs ; émet `last_sync` via `EventPort`.
- **Monitoring / alertes / santé** : un *driven adapter* **abonné à l'`EventPort`**
  (consomme les `JobRun`/events) — reprend `HealthMonitor` : métriques dans le
  runs store, alertes webhook sur N erreurs consécutives. Hors domaine.
- **Config** : un modèle Pydantic `Config` (reprend `daemon/config.py`) = settings
  (`data_path`, tz, UI/auth) + remotes + alerts + **JobSpecs déclaratifs**
  (`origin=config`). Chargé/validé par l'application ; édité via la page Config
  (`PUT /api/config`) — pas de CRUD jobs dédié (cf. décision Web UI).
- **Logging** : via `EventPort` (events `Log`) + logger standard ; plus de
  `tqdm.write` vers stdout en dur (règle TODO 2bis.1).
- **Déploiement** : principe inchangé (Docker/systemd, cf. TODO §3), mais
  `dccd start` lance la **boucle asyncio** (scheduler + streams + sync + UI
  embarquée) au lieu des threads.
- **Auth / secrets** : point d'injection prévu dans `transport` pour endpoints
  privés futurs (§10.13) ; non câblé en v3 initiale.

## 9. Stratégie de migration (anti big-bang)

**Strangler fig (interne)** — on construit le noyau à côté et on bascule par
tranches ; le vieux code est **supprimé au fur et à mesure** qu'il est remplacé.
Pas d'API publique parallèle ni de compat ascendante (rupture nette — §11.2) :

1. Poser `domain/` (modèles canoniques + Symbol) et `transport/` (HTTP/WS +
   RateLimiter + **Paginator générique** qui supprime tout chunking par-exchange).
2. Définir l'interface `Source` + Registry ; **porter 1 exchange** (Binance) en
   adaptateur unique déclarant ses capacités.
3. Écrire la couche `service/` (Operations + Job + Event bus) au-dessus.
4. Rebrancher **l'API** sur le service (+ adapters transverses : monitoring/
   alertes et sync rclone, §8ter), puis l'**UI** sur l'API, puis la **CLI**.
5. Porter les exchanges restants un à un (tests de contrat par capacité).
6. Ajouter **MCP** (dernier, car il réutilise tel quel le registry d'opérations).
7. Migration one-shot du config + des Parquet (`TS *= 1e9`) ; **suppression** des
   anciens chemins `histo_dl`/`continuous_dl` (rupture nette, pas de shim — §11.2).

À chaque étape : suite verte, pas de régression fonctionnelle visible.

---

## 10. Difficultés anticipées (ne pas refaire les erreurs de dccd 2)

| # | Difficulté | Parade proposée |
|---|-----------|-----------------|
| 1 | **Sync (REST/requests) vs async (WS/asyncio)** cohabitent mal | **DÉCIDÉ : async-first.** REST via httpx async, WS async, une boucle. Domaine **pur sync** (zéro I/O) ; disque/polars déportés via `asyncio.to_thread`. CLI fait le pont avec `asyncio.run`. |
| 2 | **Capacités irrégulières & limites par-exchange** (le 300 Coinbase) | Capacités déclaratives + **Paginator/RateLimiter centralisés** : le chunking n'est plus jamais réimplémenté par exchange. |
| 3 | **Normalisation des symboles** (XBT, quotes, séparateurs) | Type `Symbol` central + rendu/parse par-exchange testés. |
| 4 | **Dérivation fausse si trous** dans la source | Vérif de couverture + provenance `derived_from` + marquage « incomplet ». |
| 5 | **Cycle de vie des streams** (reconnect, backpressure, flush partiel, dédup) | Superviseur de Job stream robuste ; dédup à l'écriture (sur `TS`/`tid`). |
| 6 | **Drift de parité** entre interfaces | Operation Registry + **test de parité** automatique. |
| 7 | **MCP vs long-running** (tools stateless ↔ flux) | MCP déclenche un Job, renvoie un handle ; statut/résultats via tool dédié. |
| 8 | **Migration config + données** | Migration one-shot (config → nouveau schéma ; Parquet `TS *= 1e9`). Rupture nette, pas de double-run ancien/nouveau (§11.2). |
| 9 | **Explosion combinatoire des tests** (exchange × type × transport × mode) | Tests de **contrat** par capacité + fixtures enregistrées (pas d'appels réseau). |
| 10 | **Temps / fuseaux / epochs** par exchange + alignement au span | Helpers centralisés (déjà amorcés dans `tools/date_time.py`), normalisation systématique en UTC interne. |
| 11 | **Double collecte** (deux process sur le même `data_path`) | Single-owner / lockfile + dédup store ; à documenter. |
| 12 | **Écriture Parquet amplifiée** (beaucoup de streams) | Politique de flush/rollup ; granularité fichier par type. |
| 13 | **Secrets/auth** (endpoints privés futurs) | Prévoir un point d'injection d'auth dans `transport`, sans le câbler maintenant. |

---

## 11. Décisions actées

1. **Concurrence I/O : async-first.** httpx async + websockets, une seule boucle.
   Domaine pur sync ; disque/polars déportés (`asyncio.to_thread`) ; CLI via
   `asyncio.run`. (cf. §10.1)
2. **Versioning : dccd v3, rupture nette.** On **abandonne ce qui est déprécié**
   — pas de shim de compat ascendante. Même package, mais les anciens chemins
   `histo_dl`/`continuous_dl` sont remplacés, pas maintenus en parallèle.
3. **MCP : différé.** On ne le construit pas maintenant (éviter la complexité).
   Le **registre d'opérations** (§8) est conçu pour l'accueillir sans refonte le
   jour où on l'ajoute.
4. **data_types : cœur = ohlc/trades/orderbook**, modèle **extensible** pour
   funding/OI/liquidations plus tard (= passage dérivés, hors périmètre v3 initial).
   (cf. §4)
5. **Frontière UI↔API : stricte.** L'UI est un pur client HTTP de l'API publique,
   aucun chemin in-process privilégié. → l'API est dogfoodée par l'UI ⇒ parité
   garantie.

### Reste à affiner (itérations suivantes)
Modèles, ports, capacités, Jobs/Operations et sujets transverses : **faits** (voir
[`REFONTE-noyau.md`](REFONTE-noyau.md) §8 et [`REFONTE-capacites.md`](REFONTE-capacites.md)).
Restent des **paramètres d'implémentation** (non bloquants) :
- `Capability.history` (full/recent), table des spans natifs, rate limits chiffrés.
- Format précis des événements de progression + schéma du runs store (JSON/SQLite).
- Politique de flush/rollup Parquet + dédup (§10.5, §10.12) ; layout de stockage
  des snapshots order book + provenance.
- Script de migration `TS *= 1e9`.

---

## 12. Prochaines étapes

Architecture **complète et cohérente** (REFONTE + 2 annexes) : décisions
structurantes et contrats (modèles, ports, capacités, Jobs, opérations, sujets
transverses) figés. Reste :
1. Découper §9 en tickets dans `TODO.md` (section dédiée « Refonte v3 »).
2. Trancher au fil de l'eau les paramètres d'implémentation (§11, faible enjeu).
