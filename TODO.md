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

- [ ] **`time_step` indépendant par canal** : permettre de configurer un `book_time_step`
  distinct du `time_step` des trades (ex. `book_time_step=1` pour des snapshots à la
  seconde sans augmenter la fréquence de flush des trades) — évite la perte d'états
  intermédiaires du carnet quand `time_step` est large

---

## 3. Stockage unifié des données

> Refactor structurel du système de sauvegarde. Section 2 n'est pas bloquante.

### 3.2 Backfill intelligent — détection et remplissage des lacunes

Remplacer `current = max(user_start, last_saved)` par un scan des périodes
manquantes via `DataStore.missing_intervals`.

**Algorithme :**
1. Énumérer toutes les années entre `start` et maintenant
2. Année passée, fichier complet (nb lignes == attendu) → **skip**
3. Année passée, fichier absent ou incomplet → télécharger la queue manquante
   (`last_ts + span` → fin d'année) sans re-télécharger l'existant
4. Année courante → toujours étendre depuis `last_ts + span`
5. Aucun fichier pour une année → téléchargement complet

*Trous internes (données manquantes au milieu d'un fichier existant) : non
traités en v1 — cas très rare en pratique (nécessite une panne exchange + échec
des 3 retries simultanément).*

- [ ] `DataStore.missing_intervals(start, end)` — retourne la liste des
  intervalles `(datetime, datetime)` à télécharger, en skippant les années
  complètes
- [ ] `DataStore.is_period_complete(year)` — vérifie le nombre de lignes vs
  attendu (`365|366 * 24 * 60` pour 1m, etc.)
- [ ] Refactorer `_BackfillBase.run()` pour itérer sur
  `missing_intervals` plutôt que sur une fenêtre glissante depuis `last_saved`
- [ ] Tests : scénarios lacune en début, milieu et fin de plage

### 3.3 Clarification `dccd run` vs `dccd backfill`

Les deux commandes touchent la même donnée OHLC, la distinction n'est pas
évidente.

- `backfill` = remplissage historique complet avec gap-filling (usage one-shot)
- `run` = un tick du scheduler, incrémental depuis le dernier point connu
  (usage cron / daemon interne)
- `start` = daemon continu qui appelle `run` en boucle

- [ ] Renommer `dccd run` → `dccd collect` pour lever l'ambiguïté
- [ ] Mettre à jour help text, docstrings et doc RST pour expliquer clairement
  la différence entre `collect` (un tick) et `backfill` (historique complet)

---

## 4. Daemon autonome (déploiement serveur)

> Grand chantier fonctionnel. Dépend de la stabilité de la section 2.

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

stream_jobs:
  - exchange: binance
    pairs: [BTC/USDT]
    channels: [trades, book]
    time_step: 60

alerts:
  webhook_url: https://hooks.slack.com/...  # optionnel
  max_consecutive_errors: 3
```

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

## 6. Migration Polars

> Remplacement de pandas par polars dans toute la couche de stockage et de
> traitement. Motivations : stubs bundlés (mypy stable), API expression-based
> (moins de friction de types), meilleures perfs sur gros fichiers parquet.
>
> Dépendance : 3.1 doit être stable (DataStore en place) avant de migrer.

- [ ] Remplacer pandas par polars dans `dccd/storage.py` — `DataStore.save`,
  `load`, `last_timestamp` : réécrire avec l'API polars (expressions, `pl.scan_parquet`)
- [ ] Migrer `dccd/process_data.py` — `set_ohlc`, `set_trades`, `set_marketdepth`,
  `set_orders` : retourner `pl.DataFrame` au lieu de `pd.DataFrame`
- [ ] Adapter `dccd/histo_dl/exchange.py` — `_sort_data`, `save`, `save_trades`,
  `save_orderbook` : accepter/retourner `pl.DataFrame`
- [ ] Adapter `dccd/continuous_dl/` si des DataFrames y transitent
- [ ] Mettre à jour `pyproject.toml` : remplacer `pandas` par `polars`,
  retirer `pandas-stubs` des dépendances de dev
- [ ] Tests : adapter les fixtures et assertions (pl.DataFrame n'a pas d'index)
- [ ] Vérifier la compatibilité mypy end-to-end avec polars bundled stubs

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
