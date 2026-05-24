# TODO

> Les numéros servent à se repérer en conversation — ils n'apparaissent
> jamais dans les commits ou le CHANGELOG. Traçabilité historique via le
> numéro de PR uniquement.
>
> Une tâche terminée = ligne supprimée (pas de section Done). La trace
> vit dans `CHANGELOG.md` et `git log`.
>
> Workflow : `/pick-task` → plan mode → implémentation → `/finish-task`.
>
> Tailles indicatives : `[XS]` < 2 h · `[S]` ½–1 j · `[M]` 1–3 j · `[L]` 3–7 j · `[XL]` > 1 sem

---

## 1. Petits gains immédiats

Tâches courtes, peu de risque, impact direct sur l'utilisabilité du daemon.

- [ ] **`dccd status --json`** `[XS]` — exporter les métriques en JSON brut sur stdout pour intégration avec Grafana / outils externes.

- [ ] **Couverture CLI edge cases** `[S]` — `test_daemon_cli.py` existe ; compléter avec les cas limites : config manquante, exchange inconnu, pair absente, `--dry-run` parallèle.

- [ ] **Retry/back-off configurable par job** `[S]` — les erreurs réseau transitoires sont juste loguées ; ajouter un retry exponentiel (ex. 3 tentatives, délai 2×) configurable dans le YAML (`max_retries`, `retry_delay`). Évite les gaps silencieux lors d'une instabilité réseau ponctuelle.

---

## 2. Fondations techniques

Changements structurels à fort impact sur la maintenabilité et les perfs.

### 2.1 Migration Polars `[L]`

Remplacement de pandas par polars dans toute la couche de stockage et de
traitement. Motivations : stubs bundlés (mypy stable), API expression-based,
meilleures perfs sur gros fichiers parquet. Polars est déjà utilisé dans
`dccd inventory`.

- [ ] Remplacer pandas par polars dans `dccd/storage.py` — `DataStore.save`,
  `load`, `last_timestamp` : réécrire avec l'API polars (`pl.scan_parquet`)
- [ ] Migrer `dccd/process_data.py` — `set_ohlc`, `set_trades`, `set_marketdepth`,
  `set_orders` : retourner `pl.DataFrame` au lieu de `pd.DataFrame`
- [ ] Adapter `dccd/histo_dl/exchange.py` — `_sort_data`, `save`, `save_trades`,
  `save_orderbook` : accepter/retourner `pl.DataFrame`
- [ ] Adapter `dccd/continuous_dl/` si des DataFrames y transitent
- [ ] Mettre à jour `pyproject.toml` : remplacer `pandas` par `polars` en dépendance core,
  retirer `pandas-stubs` des dépendances de dev
- [ ] Tests : adapter les fixtures et assertions (`pl.DataFrame` n'a pas d'index)
- [ ] Vérifier la compatibilité mypy end-to-end avec polars bundled stubs

### 2.2 `time_step` indépendant par canal `[M]`

- [ ] Permettre de configurer un `book_time_step` distinct du `time_step` des
  trades dans `continuous_dl` (ex. `book_time_step=1` pour des snapshots à la
  seconde sans augmenter la fréquence de flush des trades) — évite la perte
  d'états intermédiaires du carnet quand `time_step` est large

---

## 3. Déploiement production `[M]`

Rendre `dccd` déployable sur un serveur sans friction.

- [ ] `Dockerfile` : `python:3.12-slim`, rclone, `pip install dccd[daemon]`,
  volumes `/data` et `/config`, entrypoint `dccd start`
- [ ] `docker-compose.yml` : service dccd avec volumes mappés + variable `DCCD_CONFIG`
- [ ] `examples/dccd.service` : unité systemd (bare-metal / VM)
- [ ] Section déploiement dans `README.rst` (Quick start Docker + systemd)

---

## 4. Documentation `[L]`

- [ ] **Refonte complète de la doc** — la structure actuelle (toctree plate, notion
  obsolète de "high/low level API") est confuse.
  - Mettre le Quickstart en avant dans l'index (démon vs Python API vs CLI).
  - Distinguer clairement les trois modes d'usage : CLI daemon, API Python
    (`histo_dl` / `continuous_dl`), intégration avancée.
  - Regrouper les pages par section dans la toctree (Daemon, Historical, Continuous, Reference).
  - Supprimer ou archiver les pages obsolètes.
  - Viser un thème moderne (Furo + sphinx-design cards pour le quickstart).

---

## 5. Nouveaux exchanges & données

### 5.1 Hyperliquid `[M]`

DEX perps on-chain (L1 custom), API publique REST + WebSocket sans auth.

- [ ] Identifier les endpoints OHLCV (`/info` avec `type: candleSnapshot`) et trades
- [ ] `FromHyperliquid` dans `dccd/histo_dl/hyperliquid.py` — timestamps en ms,
  paires au format `BTC` (pas `BTC/USDT`)
- [ ] `DownloadHyperliquidData` dans `dccd/continuous_dl/hyperliquid.py`
  (WebSocket `wss://api.hyperliquid.xyz/ws`)
- [ ] Tests + doc RST associés

### 5.2 Données on-chain `[exploratory]`

- [ ] Évaluer **The Graph** (GraphQL) pour Uniswap v3 — OHLCV/trades reconstituables
  depuis les events de swap ?
- [ ] Évaluer **Dune Analytics API** (REST, quota gratuit limité)
- [ ] Prototype `dccd/onchain/` si un provider offre données stables (< 1 h latence)

---

## 6. Phase 2 — Web UI `[XL]`

Interface de monitoring et de contrôle du daemon (FastAPI + htmx).

- [ ] `dccd/daemon/api.py` : API REST FastAPI — métriques, config active, CRUD jobs
- [ ] Frontend minimal htmx + Alpine.js : tableau de bord, ajout/suppression de
  paires, graphique de disponibilité
- [ ] Authentification basique (token Bearer ou HTTP Basic)
- [ ] Déploiement : service séparé dans `docker-compose.yml` ou thread intégré
