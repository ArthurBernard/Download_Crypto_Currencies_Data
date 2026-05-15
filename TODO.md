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

## 2. Mode autonome (déploiement serveur)

Objectif : `dccd` déployé sur un serveur collecte de la data en continu, sans
intervention manuelle, et pousse vers un stockage distant configurable.
Architecture : une couche d'orchestration (`dccd/daemon/`) par-dessus les modules
exchange existants — ceux-ci n'ont pas à changer.

### 2.1 Configuration déclarative

Un fichier YAML décrit tout ce que le daemon doit collecter. Exemple :

```yaml
storage:
  local_path: /data/crypto/
  remote:
    backend: s3          # s3 | gcs | sftp | none
    bucket: my-bucket
    prefix: dccd/

jobs:
  - exchange: binance
    pairs: [BTC/USDT, ETH/USDT]
    span: 3600
    format: parquet

  - exchange: bybit
    pairs: [BTC/USDT]
    span: 86400
    format: csv
```

- [ ] Schéma Pydantic `CollectorConfig` + `ExchangeJob` + `StorageConfig`
  avec validation (exchange connu, span valide, format supporté)
- [ ] Loader `dccd/daemon/config.py` : `load_config(path) -> CollectorConfig`
- [ ] Fichier `config.example.yml` documenté dans `examples/`

### 2.2 Scheduler histo_dl

Le scheduler appelle `import_data(start='last', end='now')` pour chaque job à
intervalle régulier. Bibliothèque retenue : **APScheduler 3.x** (plus mature que
`schedule`, supporte les expressions cron et un job store persistant).

- [ ] `dccd/daemon/scheduler.py` : instancier un `BackgroundScheduler`, ajouter
  un job par `ExchangeJob` avec `interval=span` (collecter au rythme des bougies)
- [ ] Gestion des erreurs par job : logger l'exception et continuer les autres jobs
  (un exchange en panne ne doit pas tuer le daemon)
- [ ] Mode one-shot `dccd run --once` : exécuter chaque job une fois et quitter
  (utile pour tester la config ou pour un cron système externe)

### 2.3 Push vers stockage distant

Après chaque `save()`, le daemon peut pousser le fichier vers un stockage distant.
Alternative à considérer : déléguer entièrement à **rclone** (outil externe qui
supporte S3, GCS, SFTP, Dropbox, etc. en une commande) plutôt que de coder des
backends Python — plus simple à maintenir.

- [ ] Choisir l'approche : backends Python natifs vs appel `subprocess rclone` —
  trancher avant d'implémenter
- [ ] Si backends Python : interface abstraite `RemoteStorage.push(local, remote)`,
  implémentations S3 (`boto3`, optionnel `dccd[s3]`) et SFTP (`paramiko`,
  optionnel `dccd[sftp]`)
- [ ] Si rclone : vérifier la présence de rclone au démarrage, construire la
  commande depuis la config, capturer stderr pour le logging
- [ ] Intégration dans le scheduler : `remote.push()` après chaque save réussi

### 2.4 CLI + packaging

- [ ] Entrypoint `dccd` (déclaré dans `pyproject.toml` `[project.scripts]`) avec
  `typer` : `dccd start --config config.yml`, `dccd run --once`,
  `dccd validate --config config.yml` (valide la config sans lancer de collecte)
- [ ] `Dockerfile` : image `python:3.12-slim`, copier le code, `pip install dccd`,
  volume `/data` et `/config`
- [ ] `docker-compose.yml` exemple avec le volume mappé vers un NAS/répertoire hôte
- [ ] `examples/dccd.service` : unité systemd pour démarrage au boot

### 2.5 Monitoring & health

- [ ] Logging vers fichier rotatif : `RotatingFileHandler` (10 MB × 5 fichiers),
  format `%(asctime)s %(levelname)s %(name)s — %(message)s`
- [ ] Fichier de métriques JSON mis à jour après chaque job : `last_ts`,
  `rows_collected`, `errors_count`, `last_run_at` — lisible par un script externe
  ou un dashboard minimal
- [ ] Alertes optionnelles : si N erreurs consécutives, poster sur un webhook
  (URL configurable en YAML, format compatible Slack et Discord)

---

## 3. Real-time manquant pour les exchanges existants

Les exchanges ci-dessous ont déjà un module `histo_dl` fonctionnel mais pas de
module `continuous_dl`. Chacun expose une API WebSocket publique.

### 3.1 Binance real-time

L'hôte `wss://stream.binance.com:9443/ws` est déjà enregistré dans
`ContinuousDownloader._parser_exchange` — il manque juste le module dédié.

- [ ] Implémenter `dccd/continuous_dl/binance.py` — `DownloadBinanceData` avec
  parsers pour `<symbol>@kline_<interval>` (OHLCV), `<symbol>@trade` (trades),
  `<symbol>@depth` (order book)
- [ ] Enregistrer les fonctions haut-niveau `get_data_binance`, `get_trades_binance`,
  `get_orderbook_binance` dans `dccd/continuous_dl/__init__.py`
- [ ] Tests + doc RST `continuous_dl.binance.rst`

### 3.2 Kraken real-time

Kraken propose une API WebSocket v2 (`wss://ws.kraken.com/v2`) avec des canaux
`ticker`, `ohlc`, `trade`, `book` — authentification non requise pour les données
publiques.

- [ ] Implémenter `dccd/continuous_dl/kraken.py` — `DownloadKrakenData` ; attention
  au format de souscription v2 (`{"method": "subscribe", "params": {...}}`) différent
  de l'ancienne v1
- [ ] Parsers pour `ohlc` (OHLCV), `trade` (trades), `book` (order book)
- [ ] Tests + doc RST `continuous_dl.kraken.rst`

### 3.3 OKX real-time

OKX WebSocket v5 (`wss://ws.okx.com:8443/ws/v5/public`) — même version que
le module `histo_dl/okx.py` existant, cohérence garantie.

- [ ] Implémenter `dccd/continuous_dl/okx.py` — `DownloadOKXData` ; souscription
  via `{"op": "subscribe", "args": [{"channel": "candle1H", "instId": "BTC-USDT"}]}`
- [ ] Parsers pour candles, trades, order book
- [ ] Tests + doc RST `continuous_dl.okx.rst`

---

## 4. Nouveaux exchanges & données

### 4.1 Hyperliquid

DEX perps on-chain (L1 custom), forte liquidité, API publique REST + WebSocket
sans authentification pour les données de marché.

- [ ] Lire la doc API (`https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers`)
  et identifier les endpoints OHLCV (`/info` avec `type: candleSnapshot`) et trades
- [ ] Implémenter `FromHyperliquid` dans `dccd/histo_dl/hyperliquid.py` — attention
  aux timestamps en millisecondes et aux paires au format `BTC` (pas `BTC/USDT`)
- [ ] Implémenter `DownloadHyperliquidData` dans `dccd/continuous_dl/hyperliquid.py`
  (WebSocket `wss://api.hyperliquid.xyz/ws`)
- [ ] Tests + doc RST associés

### 4.2 Données on-chain

- [ ] Évaluer **The Graph** (GraphQL, gratuit jusqu'à un certain quota) pour
  Uniswap v3 — vérifier si les données OHLCV/trades sont reconstituables depuis
  les events de swap
- [ ] Évaluer **Dune Analytics API** (REST, quota gratuit limité) — plus simple
  d'accès mais moins temps-réel
- [ ] Prototype d'un module `dccd/onchain/` si un provider offre une API stable
  et des données suffisamment granulaires (< 1h de latence)
