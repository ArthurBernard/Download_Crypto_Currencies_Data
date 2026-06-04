# Plan de remédiation dccd v3

> Écrit le 2026-06-03. Fait suite à `RETROSPECTIVE-v3.md`.
> Basé sur un audit ligne-à-ligne du code (`feat/refonte-v3`), pas sur des
> impressions. Chaque défaut est tracé à `fichier:ligne` et, quand c'est
> possible, **reproduit** (marqué ✔ vérifié).
>
> Objectif : amener v3 de « l'archi compile et 143 tests passent » à
> « v3.0 livrable, données correctes et vérifiées, doc à jour ».

---

## Avancement (2026-06-03) — M1+M2+M3 faits, reste la release

Tout consolidé sur `feat/refonte-v3` (10 commits). Gate : **162 tests verts ·
`ruff` clean · `mypy` Success (45 fichiers) · doc Sphinx 0 warning**.

- ✅ **WS-A** — pagination trades par curseur (D1, D4). 7 adapters + garde `history`.
- ✅ **WS-B** — migration v2→v3 complète, fusion défensive, provenance (D2, D3, D5).
- ✅ **G2** — migration réelle FAITE et vérifiée (120 fichiers, 0 perte ; backup
  `crypto.backup-pre-v3-2026-06-03`).
- ✅ **WS-C** — date custom, inventory, `parallel` retiré, refresh config, streams
  honnêtes (D6, D7, D8, D10, D11).
- ✅ **WS-D** — auth Bearer `/api/*` + CORS restreint (D9).
- ✅ **WS-E** — Client complété/dédupliqué, code mort retiré, OHLC honnête, purge
  tasks (D12, D13, D15, D16). *(E3 RunsStore laissé : non-bug.)*
- ✅ **WS-F** — mypy runnable+green ; doc Sphinx v3 ; CHANGELOG ; TODO ; matrice
  fidélité (D17, D18, D19 + outillage).
- ✅ **WS-G** — validation réseau réelle + tests `@network` (G3, G4).
- ✅ **WS-H/P8-3** — Dockerfile + unit systemd.
- ⬜ **WS-H/H1,H3** — merge `feat/refonte-v3 → develop`, puis `develop → master`
  + tag `v3.0.0` + push : **actions de release, en attente de l'utilisateur**.

---

## 0. Méthode & principe directeur

Le problème central n'est **pas** l'architecture (elle est bonne) mais que
**les capacités déclarées ne sont pas honorées par le moteur**. Les adapters
déclarent `history` (`full`/`recent`) et `page_direction` (`forward`/`backward`),
mais :

```
grep -rn '.history|page_direction' dccd/application dccd/transport
→ une seule occurrence, dans une docstring. Jamais utilisé en exécution.
```

Conséquence : le moteur pagine **toujours forward par fenêtre de temps fixe**,
ce qui casse les trades sur 5/7 exchanges et fait passer du Kraken « recent »
pour de l'historique profond. **La remédiation consiste d'abord à rendre le
moteur conforme aux capacités**, puis à corriger les fuites de données du
stockage, puis la migration/vérification, puis la doc/release.

Règle de travail : **chaque correctif P0/P1 doit être couvert par un test**
(unitaire + au moins un E2E réseau marqué `@pytest.mark.network`). Pas de
correctif sans test de non-régression.

---

## 1. Registre des défauts (par sévérité, avec preuve)

### P0 — Corruption / perte / fausses données (bloquant release)

| # | Défaut | Preuve | Impact |
|---|--------|--------|--------|
| **D1** | **Trades backfill cassé sur 5/7 exchanges.** Le paginator forward avance d'une fenêtre fixe (`_DEFAULT_TRADES_WINDOW_S=86400`) ; chaque adapter ne renvoie qu'**une page plafonnée** (kraken/okx/coinbase 100–1000, bitfinex/bitmex 1000–10000) sans sous-pagination. Seul Binance sous-pagine en interne. | `paginate.py:149-177` ; `kraken.py:157-199` (docstring l'admet : « caller advances the window »), `okx.py:109-139`, `coinbase.py:121-152`, `bitfinex.py:111-143`, `bitmex.fetch_trades_page`. ✔ logique confirmée | Sur paire liquide, >95 % des trades perdus. Données inexploitables. |
| **D2** | **Perte de données à la fusion sur fichiers legacy v2.** `_merge` fait `pl.concat([existing, new])` ; si `existing` a le schéma v2 (`quoteVolume`/`weightedAverage`) et `new` le schéma v3, le concat lève → `except` → « overwriting » → écrit **uniquement** les nouvelles lignes, **détruisant** les lignes v2 du fichier. | `parquet.py:379-393`. Données réelles : 120 fichiers OHLC en schéma v2 sur disque. ✔ schémas confirmés incompatibles | Un backfill v3 sur un fichier d'année courante v2 efface l'historique existant. |
| **D3** | **Migration incomplète et non idempotente.** `migrate_parquet_to_ns` convertit seulement `TS` secondes→ns ; il **ne renomme pas** les colonnes v2 (`quoteVolume`→`quote_volume`, `weightedAverage`→`trades`). Pire : les fichiers déjà en ns mais colonnes v2 (cas réel okx) ont `needs_migration=False` → **ignorés à jamais**. | `migrate.py:16-72`. okx 2025 : `TS` en ns + colonnes v2. ✔ confirmé | La donnée existante reste illisible par v3 même après `dccd migrate`. |
| **D4** | **Le moteur ignore `history` et `page_direction`.** `operations.backfill` récupère l'adapter et appelle `paginate_ohlc`/`paginate_trades` (forward) sans consulter la capacité. Kraken OHLC `history="recent"` (720 bars) n'est pas rejeté pour l'historique profond → renvoie du récent dupliqué/erroné. Coinbase trades `history="recent"` idem. Les trades `page_direction="backward"` (coinbase/okx/bitfinex/bitmex) passent dans le paginator forward. | `operations.py:188-258` ; `paginate.py` ; `kraken.py:91-106`, `coinbase.py:55-73`. ✔ confirmé | Données fausses, silencieuses, sans erreur. |
| **D5** | **Provenance jamais écrite.** `_write_parquet` construit `meta["dccd.provenance"]` puis appelle `df.write_parquet(file_path, compression="snappy")` **sans passer `meta`**. | `parquet.py:395-404`. ✔ confirmé | Feature de traçabilité morte ; tout le système `Provenance` est un no-op au stockage. |

### P1 — Fonctionnalités cassées

| # | Défaut | Preuve | Impact |
|---|--------|--------|--------|
| **D6** | **Backfill date custom plante.** `JobParams.start: int \| Literal["last","origin"]` rejette toute date ISO. L'UI propose « Custom date… », `operations` documente « ISO date », mais la création du `JobSpec` lève `ValidationError` non rattrapée. | `jobs.py:46` ; `api/app.py:271` ; `inventory.html:198`. ✔ reproduit (`'2024-06-01' → REJECTED ValidationError`) | 500 API / crash CLI / Client. Option UI inutilisable. |
| **D7** | **`dccd inventory` plante sur tout dataset OHLC.** `parts.append(d["span"])` ajoute un `int`, puis `' / '.join(parts)` exige des `str` → `TypeError`. | `cli/main.py:210-214`. ✔ logique confirmée | Commande CLI inutilisable dès qu'il y a de l'OHLC. |
| **D8** | **Streams Coinbase OHLC & orderbook = générateurs vides** (`return; yield`) alors que les capacités WS live OHLC + orderbook sont déclarées. | `coinbase.py:207-217`, capacités `coinbase.py:70-72`. ✔ confirmé | Un stream « tourne » (état running) sans produire une seule ligne. |
| **D9** | **Aucune authentification.** `ui_auth_token` n'est lu nulle part dans le code ; `CORS allow_origins=["*"]`. Le CHANGELOG annonce pourtant « Bearer-token auth ». | `grep ui_auth_token` → seulement `config.py:43` ; `api/app.py:168-173`. ✔ confirmé | API locale grande ouverte (CSRF depuis n'importe quel site). Régression vs daemon v2. |
| **D10** | **Flag `parallel` mort.** Présent dans `BackfillRequest`, `JobParams`, CLI, registry — **jamais** utilisé dans `operations.backfill`. | `operations.py` (aucune réf. `parallel`). ✔ confirmé | Promesse UI non tenue (« parallel backfill »). |
| **D11** | **`PUT /api/config` ne rafraîchit pas l'état runtime.** `app.state.all_specs` et les stream workers enregistrés ne sont pas régénérés après sauvegarde. | `api/app.py:438-454`. | Jobs/streams affichés périmés jusqu'au redémarrage. |

### P2 — Dette d'architecture / code mort

| # | Défaut | Preuve |
|---|--------|--------|
| **D12** | **`Client` duplique le câblage** de `build_registry()` au lieu de l'appeler ; et il manque `read`/`stream` (façade incomplète vs P5-8). | `__init__.py:49-76` vs `service_factory.py:15-39`. |
| **D13** | **Code mort** : `Page` (base.py:29) jamais utilisé ; `htmx.min.js` chargé mais templates en `fetch()` ; `OperationRegistry` « nom→callable » est en réalité décoratif (schémas = `dict[str,str]`, pas de callable). | `base.py:29-37`, `registry.py`. |
| **D14** | **RunsStore** : connexion SQLite ouverte/fermée à chaque appel ; `append_log` fait read-modify-write non atomique. | `runs_sqlite.py:67-78,125-139`. |
| **D15** | **Approximations OHLC silencieuses** : `quote_volume` = `close*volume` (coinbase), `volume*vwap` (kraken) ; `trades` (count) absent partout sauf binance ; bitfinex/okx/kraken WS OHLC sans `quote_volume`. | `coinbase.py:115`, `kraken.py:152`. |
| **D16** | **Fuite mémoire mineure** : `Scheduler.run_now`/`once` empilent dans `_interval_tasks` sans purge à la fin. | `scheduler.py:120-123,142-143`. |

### P3 — Doc / process / release

| # | Défaut | Preuve |
|---|--------|--------|
| **D17** | **Doc Sphinx périmée** : `histo_dl*.rst`, `continuous_dl*.rst`, `daemon.rst`, `tools.*.rst` référencent des modules supprimés → autodoc cassé. | `doc/source/` ; modules supprimés en `e055d38`. |
| **D18** | **CHANGELOG** `[Unreleased]` décrit le web UI du daemon **v2** ; aucune entrée v3. | `CHANGELOG.md`. |
| **D19** | **TODO.md : 0/48 cases cochées** alors que ~85 % fait → pas de Definition of Done. | `grep -c '\[ \]'`. |
| **D20** | **Donnée réelle jamais collectée/vérifiée en v3** ; migration jamais exécutée. | 120 fichiers v2 / 4 v3 sur disque. |
| **D21** | **Jamais releasé** : tout sur `feat/refonte-v3`, jamais mergé dans `develop`/`master`. | `git log master..HEAD`. |

---

## 2. Workstreams (ordonnés, dimensionnés, avec critères d'acceptation)

Tailles : `[XS]`<2 h · `[S]` ½–1 j · `[M]` 1–3 j · `[L]` 3–7 j.
Chaque WS = une branche `fix/...`/`feat/...` PRée dans `feat/refonte-v3`
(ou directement `develop` si on décide de merger d'abord — cf. §4).

---

### WS-A — Pagination conforme aux capacités (cœur correctness) — D1, D4

**Objectif** : que le paginator honore `page_direction` et que les trades soient
collectés **par curseur**, pas par fenêtre fixe.

- **A1 [M]** Introduire un **paginator par curseur** générique pour les trades :
  l'adapter renvoie `(items, next_cursor)` ; le paginator avance tant que
  `next_cursor` n'est pas `None` et que `last_ts < end_ns`. Remplacer la
  sous-pagination interne ad-hoc de Binance par cette mécanique commune.
  - Contrat adapter trades unifié : `fetch_trades_page(symbol, since_cursor, limit) -> (list[Trade], next_cursor)`.
  - Curseur = `fromId` (binance/bybit), `since`-ts (kraken/bitfinex), `after`-ts (okx), `startTime+last_ts` (bitmex/coinbase).
- **A2 [M]** Réécrire `fetch_trades_page` des 6 adapters concernés
  (kraken, okx, coinbase, bitfinex, bitmex, + binance refactoré) sur le nouveau
  contrat curseur. Coinbase REST trades = `history="recent"` → soit curseur
  `before`/`after` réel, soit déclarer **NoCapability historique** si l'API ne
  le permet pas (à trancher après test réel).
- **A3 [S]** `operations.backfill` : router OHLC/trades selon
  `cap.page_direction` (forward vs cursor) et **lever `NoCapability`/`CoverageError`
  tôt** si `cap.history == "recent"` et que la fenêtre demandée dépasse la
  profondeur dispo (Kraken OHLC profond, Coinbase trades profonds).
- **A4 [S]** Dimensionner la fenêtre OHLC forward sur `span*max_per_request`
  (déjà fait) **et** vérifier l'ordre de tri renvoyé par chaque exchange
  (okx/coinbase renvoient DESC) — trier avant yield si besoin.

**Acceptation** : test E2E réseau par exchange qui backfill ~10 min de trades
BTC et vérifie que `nb_lignes ≈ nb_attendu` (pas un seul page-plafond), sans
trou > 1 s. `pytest -m network` vert.

---

### WS-B — Stockage robuste & schéma unifié — D2, D3, D5

- **B1 [M]** **Migration complète v2→v3** dans `migrate.py` :
  - Détecter le schéma par **colonnes** (pas seulement l'échelle TS).
  - Renommer `quoteVolume`→`quote_volume` ; mapper/!abandonner `weightedAverage`
    (pas d'équivalent v3 — soit drop, soit recalcul `trades=null`).
  - Convertir `TS` s→ns **uniquement** si nécessaire (idempotent).
  - `--dry-run` exhaustif (liste fichiers + transformation prévue) ; rapport
    par fichier `{path, from_schema, to_schema, rows, action}`.
- **B2 [S]** **Lecture défensive** : `ParquetStore.load`/`_merge`/`last_timestamp`
  normalisent un fichier legacy à la volée (rename colonnes) **avant** tout
  `concat`, pour ne jamais perdre de données même si la migration n'a pas tourné.
  `_merge` doit **aligner les schémas** (`select`/`with_columns` colonnes
  manquantes en null) avant `concat`, et ne JAMAIS « overwrite » silencieusement
  sur erreur de schéma — en cas d'échec réel, lever et logguer le fichier.
- **B3 [XS]** **Écrire la provenance** : passer `metadata=meta` à
  `df.write_parquet(...)` (Polars supporte `metadata=` via Arrow) **ou** stocker
  un `.json` sidecar. Test : relire la provenance après écriture.
- **B4 [S]** Test round-trip : créer un fichier v2 synthétique, lancer migrate,
  vérifier schéma v3 + zéro perte de ligne ; puis backfill incrémental dessus
  → vérifier fusion sans perte.

**Acceptation** : `dccd migrate` rend les 120 fichiers réels lisibles par
`ParquetStore.load` (colonnes v3) ; un backfill sur année courante v2 **ne perd
aucune ligne** (test).

---

### WS-C — Bugs fonctionnels — D6, D7, D8, D10, D11

- **C1 [XS]** D6 : élargir `JobParams.start` à `int | str` (valider le format
  dans `operations`/un validateur dédié), retirer les `# type: ignore[arg-type]`.
  Test : backfill `start="2024-01-01"` ne lève pas.
- **C2 [XS]** D7 : `cli inventory` → `str(d["span"])` (et `d.get("span")`).
  Test CLI sur un store OHLC.
- **C3 [S]** D8 : implémenter réellement les streams Coinbase OHLC + orderbook,
  **ou** retirer ces capacités WS si non supportées proprement (ne pas mentir
  sur la capacité). Idem audit des autres `return; yield`.
- **C4 [XS]** D10 : implémenter `parallel` (backfill de plusieurs périodes en
  parallèle borné par un sémaphore) **ou** le retirer de l'API/CLI/JobParams.
  Décision par défaut : **retirer** (le gain réel est côté curseur, WS-A).
- **C5 [S]** D11 : après `PUT /api/config`, recharger `all_specs` et
  re-`register_streams` ; invalider/rafraîchir l'état exposé par `/api/jobs`.

**Acceptation** : tests API pour C1/C5 ; test CLI pour C2 ; capacités WS
cohérentes avec l'implémentation (C3).

---

### WS-D — Sécurité & durcissement — D9

- **D-1 [S]** Middleware d'auth : si `settings.ui_auth_token` est défini, exiger
  `Authorization: Bearer <token>` sur `/api/*` (sauf `/health`). 401 sinon.
- **D-2 [XS]** Restreindre CORS : par défaut `allow_origins` = `[]` (même origine),
  configurable. Pas de `["*"]` avec mutations d'état.
- **D-3 [XS]** Documenter le modèle de menace (UI locale, bind 127.0.0.1 par
  défaut) dans la doc déploiement.

**Acceptation** : test API 401 sans token / 200 avec token ; CORS testé.

---

### WS-E — Qualité interne & dette — D12, D13, D14, D15, D16

- **E1 [S]** D12 : `Client.__aenter__` appelle `build_registry()` ; ajouter
  `Client.read()` et `Client.stream()` pour une vraie parité Python API.
- **E2 [XS]** D13 : supprimer `Page` si inutilisé ; retirer `htmx.min.js` si non
  utilisé ; soit câbler réellement `OperationRegistry`→callables, soit le
  documenter comme simple catalogue et le sortir du discours « 1:1 callable ».
- **E3 [S]** D14 : `RunsStore` garde une connexion persistante (ou pool) ;
  `append_log` en une seule requête (`json_set`/UPDATE atomique) pour éviter la
  course read-modify-write.
- **E4 [S]** D15 : documenter explicitement les champs approximés/absents par
  exchange (matrice de fidélité OHLC), et remplir `trades`/`quote_volume` quand
  l'API les fournit (binance les a déjà).
- **E5 [XS]** D16 : purger `_interval_tasks` via `add_done_callback`.

**Acceptation** : `ruff`/`mypy` clean ; pas de symbole mort (vulture/grep) ;
tests existants verts.

---

### WS-F — Documentation v3 — D17, D18, D19

- **F1 [M]** Purger `doc/source/` des `.rst` v2 ; régénérer l'autodoc sur
  `dccd/{domain,transport,sources,storage,application,interfaces}` ; réécrire
  `index`, `quickstart`, `installation`, `cli`, `configuration`. `make html`
  **sans warning** de module manquant.
- **F2 [S]** Réécrire `README` (déjà partiellement v3) : modes d'usage, matrice
  exchanges×capacités×fidélité (issue de E4), avertissements (Kraken OHLC recent,
  trades curseur).
- **F3 [XS]** CHANGELOG : remplacer `[Unreleased]` par l'entrée **v3.0.0** réelle
  (réécriture hexagonale, 7 exchanges, CLI/API/UI, migration, breaking changes).
- **F4 [XS]** Cocher `TODO.md` au fil des PRs ; archiver les tâches faites.

**Acceptation** : doc buildée sans warning ; CHANGELOG reflète la réalité.

---

### WS-G — Validation sur données réelles — D20

- **G1 [S]** Script reproductible `scripts/verify_data.py` : pour un dataset,
  vérifie continuité temporelle (pas de trou > 1 span), monotonicité TS, dédup,
  cohérence OHLC (`low ≤ open,close ≤ high`), couverture trades.
- **G2 [S]** Exécuter la **migration réelle** sur `/home/arthur/data/crypto`
  (`--dry-run` d'abord), archiver le rapport.
- **G3 [M]** Lancer un **backfill v3 réel** : 1 paire OHLC + 1 paire trades par
  exchange, sur ~7 jours ; passer `verify_data.py` ; consigner les résultats
  (ligne attendue vs obtenue) dans `doc` ou un rapport versionné.
- **G4 [XS]** Tests E2E réseau marqués `@pytest.mark.network` (exclus du CI par
  défaut, lançables à la demande) issus de WS-A.

**Acceptation** : rapport de vérification vert pour ≥1 dataset/exchange ;
les 120 fichiers legacy lisibles post-migration.

---

### WS-H — Release v3.0 — D21

- **H1 [XS]** Merge `feat/refonte-v3 → develop` (après WS-A..G), PR review.
- **H2 [S]** Déploiement (P8-3) : Dockerfile + unit systemd pour `dccd start`.
- **H3 [XS]** `develop → master`, tag `v3.0.0`, publication.

---

## 3. Séquencement & jalons

```
M1 — « Données correctes » (bloquant)      : WS-A, WS-B, WS-C(C1,C2), WS-G(G1,G2)
M2 — « Sûr & propre »                       : WS-C(reste), WS-D, WS-E
M3 — « Livrable »                           : WS-F, WS-G(G3,G4), WS-H
```

Ordre strict recommandé : **A → B → G2 (migration réelle) → C → D → E → F → G3 → H.**
Raison : inutile de migrer/vérifier (B/G) avant que la pagination (A) soit
correcte, sinon on valide de la donnée encore trompeuse.

Estimation grossière : M1 ≈ 6–9 j, M2 ≈ 4–6 j, M3 ≈ 4–6 j → **~3 semaines**
de travail focalisé.

---

## 4. Décisions tranchées (2026-06-03)

1. **Branches** : rester sur `feat/refonte-v3` pour M1 (WS-A/B + migration réelle),
   **merger dans `develop` une fois M1 fini**, puis empiler M2/M3. ✅ tranché
2. **Coinbase trades historiques** : à décider **empiriquement en A2** (curseur réel
   possible ? sinon NoCapability assumé et documenté).
3. **`weightedAverage`** : **drop pur** à la migration (pas de reconstruction). ✅
4. **`parallel`** : **retirer** (BackfillRequest/JobParams/CLI/registry). ✅
5. **Champs OHLC non natifs** (`quote_volume` coinbase/kraken, `trades` count) :
   **mettre à `null`** quand non fournis nativement + **matrice de fidélité**
   documentée. Supprimer les approximations `close×volume` / `volume×vwap`. ✅

Chaque sous-branche de travail : `fix/<topic>` PRée dans `feat/refonte-v3`.

---

## 5. Definition of Done (à appliquer à chaque PR)

- [ ] Correctif couvert par test (unitaire ; + E2E réseau si chemin réseau).
- [ ] `pytest` vert, `ruff check dccd/` clean, `mypy dccd/` clean.
- [ ] Pas de capacité déclarée sans implémentation honnête.
- [ ] CHANGELOG mis à jour ; case `TODO.md` cochée.
- [ ] Si la donnée est touchée : `verify_data.py` passé sur un échantillon réel.
```
