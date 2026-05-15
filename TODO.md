# TODO

> Les numéros servent à se repérer en conversation — ils n'apparaissent
> jamais dans les commits ou le CHANGELOG. Traçabilité historique via le
> numéro de PR uniquement.
>
> Une tâche terminée = ligne supprimée (pas de section Done). La trace
> vit dans `CHANGELOG.md` et `git log`.
>
> Workflow : `/pick-task` → plan mode → implémentation → `/finish-task`.

---

## 2. Robustesse & cohérence

> Améliorations structurelles importantes, pas urgentes mais impactantes
> sur la maintenabilité et la fiabilité.

### 2.1 Snapshots `continuous_dl`

- [ ] **Crash recovery** : après chaque `time_step`, persister l'état de `self.d`
  (le dict book courant) dans un fichier de reprise (JSON ou parquet), distinct des
  données de marché ; au redémarrage, recharger ce fichier pour éviter le trou entre
  le crash et le prochain "partial/snapshot" envoyé par l'exchange
- [ ] **Horodatage précis** : enrichir `_data[t]` d'un timestamp réel (UTC,
  précision milliseconde) au moment de la prise de snapshot, pas seulement le slot
  entier `self.t` — nécessaire pour la jointure avec les données histo
- [ ] **Séparer trades et book** : aujourd'hui les deux types sont dans le même
  `_data[t]` alors que leur structure est fondamentalement différente (liste ordonnée
  vs dict) ; distinguer les deux canaux au niveau du saver

### 2.2 Formatage des paires

- [ ] Créer une fonction `format_pair(crypto, fiat)` par exchange pour remplacer la
  logique inline dans chaque `__init__` — facilite les tests et isole les règles
  spécifiques (ex. Kraken : `BTC→XBT`, préfixes `X`/`Z`, cas `BCH`/`DASH`)
- [ ] Ajouter des tests pour les cas particuliers Kraken : `BTC/USD`, `BCH/EUR`,
  `DASH/USD`, crypto avec fiat non-standard

### 2.3 Réduction de la duplication dans `continuous_dl`

- [ ] `parser_trades()` et `parser_book()` ont le même squelette dans Binance, Bybit,
  Kraken, OKX — remonter le pattern dans la base `ContinuousDownloader`
- [ ] Valider les données WebSocket avec les modèles Pydantic `Trade` et
  `OrderBookEntry` déjà définis dans `models.py` (inutilisés à ce jour)
- [ ] Définir un protocole commun pour `__call__()` dans les continuous downloaders :
  Binance prend `pair: str`, Bitfinex `channel: str, **kwargs`, Bitmex `*args`,
  Bybit/Kraken/OKX n'en ont pas

---

## 4. Daemon autonome (déploiement serveur)

> Grand chantier fonctionnel. Dépend de la stabilité de la section 1 et 2.

**Vision :** `dccd` déployé sur un serveur collecte de la data en continu
(histo REST + streams WebSocket), la stocke localement, et la pousse vers
un espace de stockage dédié configurable (NAS, SFTP, S3…). Une interface
CLI permet de tout contrôler ; une Web UI (phase 2) offrira un dashboard
de monitoring.

**Architecture :**
```
dccd/daemon/
├── config.py         ← schéma YAML + validation Pydantic
├── storage.py        ← abstraction push vers remote
├── scheduler.py      ← APScheduler pour histo_dl
├── stream_manager.py ← gestion des streams continuous_dl
└── health.py         ← métriques JSON + alertes webhook
```

Config YAML de référence :
```yaml
storage:
  local_path: /data/crypto/
  remote:              # optionnel
    provider: rclone   # rclone | none
    remote: mynas:crypto/

histo_jobs:
  - exchange: binance
    pairs: [BTC/USDT, ETH/USDT]
    span: 3600          # secondes
    format: parquet
    by_period: Y

stream_jobs:
  - exchange: binance
    pairs: [BTC/USDT]
    channels: [trades, book]
    time_step: 60

alerts:
  webhook_url: https://hooks.slack.com/...  # optionnel
  max_consecutive_errors: 3
```

### 4.1 Configuration déclarative

- [ ] Schéma Pydantic : `CollectorConfig`, `HistoJob`, `StreamJob`, `StorageConfig`,
  `AlertConfig` — avec validation (exchange connu, span ≥ 60, format supporté,
  channels valides)
- [ ] Loader `dccd/daemon/config.py` : `load_config(path) -> CollectorConfig`
  — lire le YAML, valider, retourner le modèle
- [ ] `examples/config.example.yml` documenté (commentaires inline)
- [ ] Tests : config valide, exchange inconnu, span invalide, channel invalide,
  fichier absent

### 4.2 Storage abstraction

Déléguer à **rclone** via `subprocess` (50+ providers : S3, GCS, SFTP, SMB…).
Absent → warning et collecte locale seulement.

- [ ] `dccd/daemon/storage.py` : `RemoteStorage.push(local_path)` — appel
  `rclone copy` si configuré, no-op sinon ; capturer stderr pour le logging
- [ ] Vérification de la présence de rclone au démarrage du daemon
- [ ] Tests : mock subprocess, vérifier commande construite, comportement si absent

### 4.3 Collecte historique (histo_dl scheduler)

- [ ] `dccd/daemon/scheduler.py` : `build_histo_scheduler(config)` → APScheduler
  `BackgroundScheduler`, un job par `(exchange, pair)` avec `interval=span`,
  `coalesce=True`, `max_instances=1`
- [ ] `run_histo_job(job, pair, storage)` : appelle
  `FromXxx(path, crypto, span, fiat).import_data('last', 'now').save()` puis
  `storage.push()` — exceptions loguées isolément, les autres jobs continuent
- [ ] `run_once(config)` : exécute tous les histo_jobs une fois et quitte
- [ ] Tests : mock des classes histo_dl, vérifier chaîne d'appels, isolation
  des erreurs, run_once

### 4.4 Collecte temps-réel (continuous_dl manager)

- [ ] `dccd/daemon/stream_manager.py` : `StreamManager` qui démarre un thread par
  `(exchange, pair, channel)` via les classes `DownloadXxxData` existantes
- [ ] Persistence : snapshots toutes les `time_step` secondes puis `storage.push()`
  via les callbacks `set_process_data` / `set_saver`
- [ ] Reconnexion : le manager relance le thread si le stream meurt définitivement
  (max_retries épuisés)
- [ ] Tests : mock des classes continuous_dl, vérifier démarrage/arrêt, relance

### 4.5 Health & monitoring

- [ ] `dccd/daemon/health.py` : `RotatingFileHandler` (10 MB × 5 fichiers)
- [ ] Métriques JSON après chaque job : `last_run_at`, `last_success_at`,
  `rows_collected`, `errors_count` par `(exchange, pair)`
- [ ] Alertes webhook optionnelles si `errors_count ≥ max_consecutive_errors`
  (format Slack/Discord)
- [ ] Tests : écriture métriques, envoi webhook mocké

### 4.6 CLI (typer)

- [ ] Entrypoint `dccd` dans `pyproject.toml [project.scripts]`
- [ ] `dccd validate --config config.yml` : valider sans lancer
- [ ] `dccd run --once --config config.yml` : exécuter les histo_jobs une fois
- [ ] `dccd start --config config.yml` : daemon en foreground
- [ ] `dccd status` : afficher les métriques de façon lisible
- [ ] `dccd add --exchange binance --pair ETH/USDT --span 3600` : ajouter un job
- [ ] Tests CLI : `typer.testing.CliRunner` pour chaque commande

### 4.7 Déploiement

- [ ] `Dockerfile` : `python:3.12-slim`, rclone, `pip install dccd`, volumes
  `/data` et `/config`, entrypoint `dccd start`
- [ ] `docker-compose.yml` : service dccd avec volumes mappés + `DCCD_CONFIG`
- [ ] `examples/dccd.service` : unité systemd (bare-metal / VM)
- [ ] Section déploiement dans `README.rst` (Quick start Docker + systemd)

### 4.8 Web UI — phase 2 (FastAPI + htmx)

- [ ] `dccd/daemon/api.py` : API REST FastAPI — métriques, config active, CRUD jobs
- [ ] Frontend minimal htmx + Alpine.js : tableau de bord, ajout/suppression de
  paires, graphique de disponibilité
- [ ] Authentification basique (token Bearer ou HTTP Basic)
- [ ] Déploiement : service séparé dans `docker-compose.yml` ou thread intégré

---

## 5. Nouveaux exchanges & données

> Évolutions futures, aucune dépendance bloquante avec les sections précédentes.

### 5.1 Hyperliquid

DEX perps on-chain (L1 custom), API publique REST + WebSocket sans auth.

- [ ] Identifier les endpoints OHLCV (`/info` avec `type: candleSnapshot`) et trades
- [ ] `FromHyperliquid` dans `dccd/histo_dl/hyperliquid.py` — timestamps en ms,
  paires au format `BTC` (pas `BTC/USDT`)
- [ ] `DownloadHyperliquidData` dans `dccd/continuous_dl/hyperliquid.py`
  (WebSocket `wss://api.hyperliquid.xyz/ws`)
- [ ] Tests + doc RST associés

### 5.2 Données on-chain

- [ ] Évaluer **The Graph** (GraphQL) pour Uniswap v3 — OHLCV/trades reconstituables
  depuis les events de swap ?
- [ ] Évaluer **Dune Analytics API** (REST, quota gratuit limité)
- [ ] Prototype `dccd/onchain/` si un provider offre données stables (< 1h latence)
