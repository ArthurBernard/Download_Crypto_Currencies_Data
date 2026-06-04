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

## 2. Fondations techniques

Changements structurels à fort impact sur la maintenabilité et les perfs.

### 2.2 `time_step` indépendant par canal `[M]`

- [ ] Permettre de configurer un `book_time_step` distinct du `time_step` des
  trades dans `continuous_dl` (ex. `book_time_step=1` pour des snapshots à la
  seconde sans augmenter la fréquence de flush des trades) — évite la perte
  d'états intermédiaires du carnet quand `time_step` est large

---

## 2bis. Web UI — suite `[M]`

Retours après le lot 3 (PR #68). L'UI marche mais demande une passe de
cohérence et de feedback temps réel.

### 2bis.1 Rediriger la progression backfill vers le front `[S]`

- [ ] Quand l'UI est active, les barres de progression tqdm des backfills
  s'affichent encore dans la console (stdout) ET dans le front → doublon. Router
  la progression vers le front uniquement (ou supprimer la sortie console tqdm
  quand un `message_callback`/`progress_callback` est branché), pour que la
  "Backfill activity" du Dashboard montre les barres en cours plutôt que la
  console.

### 2bis.2 Repenser la cohérence scheduler / backfill / histo jobs / stream jobs `[M]`

- [ ] Le modèle conceptuel manque de cohérence entre : le **scheduler** (qui
  lance les histo jobs), le **backfill** (rattrapage historique), les **histo
  jobs** (définition de collecte REST) et les **stream jobs** (WebSocket).
  Clarifier les relations et l'UX : qu'est-ce qui démarre quoi, où on configure,
  où on déclenche, comment l'état se reflète. Probable refonte de la navigation
  et/ou du vocabulaire (Update vs Backfill vs scheduler vs streams).

---

## 3. Déploiement production `[M]`

Rendre `dccd` déployable sur un serveur sans friction.

- [ ] `Dockerfile` : `python:3.12-slim`, rclone, `pip install dccd[daemon]`,
  volumes `/data` et `/config`, entrypoint `dccd start`
- [ ] `docker-compose.yml` : service dccd avec volumes mappés + variable `DCCD_CONFIG`
- [ ] `examples/dccd.service` : unité systemd (bare-metal / VM)
- [ ] Section déploiement dans `README.rst` (Quick start Docker + systemd)

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
