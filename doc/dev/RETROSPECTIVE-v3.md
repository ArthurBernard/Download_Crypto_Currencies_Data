# Rétrospective — Refonte dccd v3

> Document d'analyse honnête de l'exécution de la refonte v3, écrit le 2026-06-03.
> Branche au moment de l'écriture : `feat/refonte-v3` (jamais mergée dans `develop`).
> Sources : `TODO.md` (plan P0→P8), `REFONTE*.md`, l'historique git, l'état réel
> du code et **les données réellement présentes dans `/home/arthur/data/crypto`**.
>
> Objectif : tirer les leçons, pas se féliciter. Les régressions et les angles
> morts sont listés sans complaisance, avec preuves vérifiables.

---

## 0. Résumé exécutif

La **réécriture architecturale est largement réussie** : l'archi hexagonale
(domain pur → transport → sources → application → interfaces) est en place,
7 exchanges sont adaptés, 143 tests passent, `ruff` et `mypy` sont clean, le
v2 a été supprné proprement. C'est une vraie montée de gamme structurelle.

Mais **trois manquements graves** entachent l'exécution, et ils correspondent
exactement aux soupçons de l'utilisateur :

1. **La donnée réelle n'a jamais été collectée ni vérifiée en v3.** Sur 124
   fichiers OHLC présents sur disque, **120 sont encore au schéma v2**
   (`quoteVolume`, `weightedAverage`) et **seulement 4 au schéma v3**
   (`quote_volume`) — ces 4 ayant été écrits pendant le debug du web UI.
   La migration `P3-4` (`dccd migrate`) **n'a jamais tourné** sur ces données.
2. **La documentation n'a pas été mise à jour** (`P8-2` non fait). Sphinx
   référence encore `histo_dl`, `continuous_dl`, `daemon` — des modules
   **supprimés**. Le CHANGELOG `[Unreleased]` décrit le web UI du daemon **v2**
   et ne mentionne même pas la réécriture.
3. **Le suivi de plan a décroché.** `TODO.md` : **48 cases, 0 cochée**, alors
   que ~85 % du travail est fait. Le plan et la réalité ont divergé en silence.

Le travail sur le web UI (objet de cette session) a corrigé de vrais bugs
critiques, mais une bonne partie de ces bugs étaient des **régressions
introduites par la refonte elle-même** (cf. §3), donc du temps passé à réparer
ce qu'on venait de casser plutôt qu'à avancer.

---

## 1. Ce qui a été fait vs le plan (P0→P8)

| Phase | Plan | État réel | Note |
|------|------|-----------|------|
| **P0** Fondations | squelette hexagonal, deps 3.11+/httpx, CI 3.11–3.13 + ruff + mypy strict | ✅ Fait | Conforme. |
| **P1** Domaine | Symbol, DataType, records ns, dataset, timeutils, transforms, errors | ✅ Fait | Modules présents et testés. |
| **P2** Transport | http retry, ws reconnect, ratelimit, paginate | ✅ Fait | Paginator générique en place. |
| **P3** Stockage | ports, parquet ns, runs SQLite, **migrate**, rclone | 🟡 Partiel | `migrate.py` **existe mais jamais exécuté/validé** sur la vraie donnée (cf. §4). Reste 120 fichiers v2 illisibles par le chemin de lecture v3. |
| **P4** Sources pilotes | Binance, Coinbase, Kraken + **contract tests** | 🟡 Partiel | Adapters OK. « Contract tests » réels (round-trip réseau) absents — seulement déclaration de capacités + protocole. |
| **P5** Application | config, events, operations, jobs, scheduler, registry, monitor, **Client** | ✅ Fait | Tout présent. |
| **P6** Interfaces | API FastAPI + SSE, UI, CLI, **test de parité**, `dccd start` | ✅ Fait | Parité registre testée. UI a nécessité un long debug (§3). |
| **P7** Exchanges restants | Bybit, OKX, Bitfinex, BitMEX | ✅ Fait | Capacités déclarées, `NoCapability` tôt où il faut. |
| **P8** Bascule & nettoyage | suppr. v2, **docs**, déploiement, **release** | 🔴 Largement non fait | P8-1 (suppr. v2) ✅ ; **P8-2 docs ❌** ; P8-3 déploiement ❌ ; **P8-4 release ❌** (jamais mergé, CHANGELOG périmé). |
| **M3** Différé | MCP, OHLC dérivé Kraken, marchés dérivés, auth privée | ⏸ Non commencé | Attendu (post-v3.0). |

**Verdict** : le **cœur technique (P0–P7) est à ~90 %**, mais la **phase de
bascule (P8) — celle qui transforme « ça marche sur ma machine » en « v3.0
livrable » — est à ~25 %.** Or c'est elle qui crée la valeur perçue.

---

## 2. Améliorations vs v2

Ce sont de vraies avancées, à reconnaître :

- **Architecture testable** : domaine pur sans I/O, dépendances inversées,
  `service_factory` comme source unique de câblage. La v2 mélangeait I/O et
  logique.
- **Timestamps nanosecondes UTC int64** partout en interne (vs schéma v2
  hétérogène).
- **Async-first** (httpx + websockets) au lieu du mix `requests`/threads.
- **Paginator générique** piloté par les capacités → 1 seul algo de pagination
  pour 7 exchanges, au lieu de code dupliqué par exchange.
- **Capacités déclaratives** : `NoCapability` levé tôt (ex. trades spot Bybit)
  plutôt qu'un échec réseau opaque.
- **3 interfaces 1:1** sur un registre d'opérations (CLI / API / UI) → parité
  garantie par test.
- **RunsStore SQLite (WAL)** : historique des runs requêtable, vs logs plats.
- **Qualité** : `mypy` strict sur `domain/`, matrice CI 3.11–3.13, 143 tests.

---

## 3. Régressions vs v2 (inacceptables — à corriger)

La session de debug du web UI a révélé que **la refonte a réintroduit des bugs
que la v2 n'avait pas**, ou a livré des fonctions cassées dès le départ :

- **Backfill silencieusement tué** : `asyncio.create_task()` sans garder de
  référence forte → la tâche était collectée par le GC en plein run (0 ligne
  écrite, aucune erreur). La v2 (threads daemon) n'avait pas ce piège.
  *Corrigé* (`_spawn` + `app.state.bg_tasks`).
- **Backfill trades Binance à ~0 % de couverture** : le paginator avançait par
  fenêtres d'un jour alors qu'aggTrades plafonne à 1000/appel → quelques
  secondes captées par jour. *Corrigé* (sous-pagination interne `fromId`).
- **Barre de progression jamais alimentée** : `progress`/`log_tail` renvoyés
  comme chaînes JSON brutes par SQLite, jamais décodés côté API. *Corrigé*
  (`_parse_run`).
- **Logs invisibles dans l'UI** : les logs n'allaient que sur l'EventBus, pas
  persistés. *Corrigé* (`_emit_log` → RunsStore).
- **Span OHLC invalide avalé** : `binance_interval(span)` retournait `[]`
  silencieusement (« No Binance interval for span=1 »). *Corrigé* (validation
  `span in cap.spans` + select au lieu d'un champ libre).
- **Span non transmis à l'inventaire** : `inventory()` rendait le span en label
  string (`"1m"`) au lieu de l'entier. *Corrigé*.
- **Endpoint `POST /api/jobs/{job_id}/run`** cassé par les `/` dans les IDs
  (BTC/USDT). *Corrigé* (passage en body).

**Leçon** : ces bugs auraient dû être attrapés **avant** la livraison à
l'utilisateur, par un **test end-to-end réel** (lancer un vrai backfill court et
vérifier les lignes écrites). Les 143 tests unitaires ne couvraient aucun de ces
chemins — d'où le ping-pong de debug manuel. C'est le symptôme central : **on a
testé les pièces, jamais la chaîne complète sur données réelles.**

---

## 4. La donnée : jamais collectée ni vérifiée en v3 (soupçon confirmé)

Preuve directe (`/home/arthur/data/crypto`, 1.7 Go) :

```
Fichiers OHLC au schéma v2 (quoteVolume / weightedAverage) : 120
Fichiers OHLC au schéma v3 (quote_volume)                  :   4
```

- Les 120 fichiers v2 (binance, bybit, coinbase, kraken, okx) sont d'**anciennes
  données v2** posées là, jamais re-collectées ni migrées vers le schéma v3.
  Exemple : `okx/ohlc/BTC-USDT/1m/2025.parquet` = 525 600 lignes (année 1m
  complète) mais colonnes `TS, open, high, low, close, volume, quoteVolume,
  weightedAverage` (`weightedAverage` 100 % null).
- Les **4 seuls fichiers v3** ont été produits **pendant le debug du web UI**
  (ex. `binance/trades/BTC-USDT/2026-06-03.parquet`, journée partielle).
- **`dccd migrate` (P3-4) n'a jamais tourné** sur ce dossier, sinon les 120
  fichiers seraient au schéma v3.

**Conséquence concrète** : le chemin de lecture v3 (`ParquetStore.read`,
`inventory`) attend `quote_volume` ; sur les fichiers v2 il ne lit que `TS`
(qui existe) → l'inventaire « marche » par accident, mais une vraie lecture
OHLC de ces fichiers est incohérente. **L'utilisateur ne peut pas, aujourd'hui,
relire en v3 la donnée qu'il possède.**

**Ce qu'il aurait fallu faire** :
1. Lancer `dccd migrate --dry-run` puis réel sur `/home/arthur/data/crypto`.
2. Lancer un **backfill v3 réel** sur au moins 1 paire/exchange et **vérifier**
   nb de lignes, continuité temporelle (pas de trous), dédup sur `TS`.
3. Documenter le résultat (script de vérif reproductible).

---

## 5. Fait en plus du plan / ce qui aurait dû l'être en plus

**Fait en plus du plan initial** (lots web UI 1→3, PRs #65–#69) :
- Branding, regroupement par exchange, backfill all/par-exchange, formulaire
  Config, logs de backfill dans l'UI, contrôle start/stop des streams,
  dashboard live, console standalone. C'est utile — mais ça a **mobilisé
  l'essentiel de l'effort récent au détriment de P8 (docs + release + vérif
  données)**.

**Ce qui aurait dû être fait en plus (et ne l'a pas été)** :
- **Tests end-to-end réseau** (au moins 1 par exchange, en CI optionnelle/marquée
  `@pytest.mark.network`) — auraient évité §3.
- **Script de vérification de données** (continuité, trous, dédup) livré avec la
  v3, pas seulement des tests unitaires.
- **Migration exécutée et documentée** sur la donnée existante.
- **Nettoyage du code mort** (cf. §7).

---

## 6. Ce qui reste à faire (priorisé)

**P0 — bloquant pour parler de « v3.0 »**
1. **Documentation v3** (P8-2) : purger les `.rst` v2 (`histo_dl*`,
   `continuous_dl*`, `daemon.rst`, `tools.*` obsolètes), régénérer l'autodoc sur
   `dccd/{domain,application,sources,...}`, réécrire `quickstart`, `cli`,
   `configuration`. Vérifier `cd doc && make html` **sans warning** de module
   manquant.
2. **CHANGELOG** : remplacer la section `[Unreleased]` (qui décrit le daemon v2)
   par l'entrée v3.0 réelle.
3. **Migration + collecte réelle vérifiées** (cf. §4).

**P1 — qualité/fiabilité**
4. **Tests end-to-end** par exchange (réseau, marqués).
5. Lever les incohérences backfill restantes (résumé/`rows` affichés, reprise
   `start=last` vs `origin`, comportement trades vs OHLC homogène).
6. Supprimer le code mort (§7).

**P2 — livraison**
7. **Release v3.0** (P8-4) : merge `feat/refonte-v3 → develop`, puis `develop →
   master`, tag `v3.0.0`.
8. Déploiement (P8-3) : Dockerfile + systemd pour le daemon async.

**Différé** : M3 (MCP, OHLC dérivé Kraken, marchés dérivés, auth privée).

---

## 7. Code mort / incohérences à nettoyer

- **Docs fantômes** : `doc/source/{histo_dl,continuous_dl,daemon}*.rst`,
  `tools.*.rst` pointent vers des modules supprimés.
- **`config.yml` racine** non versionné contient un job orderbook ajouté en
  test — à clarifier (exemple vs local).
- **Incohérence de schéma** v2/v3 sur disque (cf. §4) — soit migrer, soit que
  `ParquetStore` détecte/normalise les colonnes legacy à la lecture.
- **`import time` locaux** répétés dans `binance.py` (`fetch_orderbook`,
  `_BinanceDepthWS.parse_message`) au lieu d'un import en tête — micro, mais
  symptomatique.
- **Branches locales mortes** : `feat/web-ui*`, `feat/refonte-v3`,
  `docs/sphinx-refonte`, multiples `fix/*` — à élaguer après release.
- Vérifier l'absence de helpers dupliqués entre lots UI (escapeHtml, fmtNs,
  showProgress) entre `dashboard.html`/`inventory.html`.

---

## 8. Ce qui manquait dans la planification

- **Aucune phase « validation sur données réelles »** dans le plan P0→P8. Le
  plan validait le *code*, jamais la *donnée produite*. C'est la cause racine
  du §4. → Ajouter systématiquement un jalon « E2E + vérif données » par
  exchange avant de cocher un adapter.
- **« Contract tests » (P4) trop vagues** : interprétés comme « déclaration de
  capacités » alors qu'ils auraient dû inclure un round-trip réseau réel.
- **P8 (docs/release) sous-dimensionnée** et reléguée en fin de plan, donc
  sacrifiée quand le web UI a débordé. La doc et le CHANGELOG auraient dû être
  mis à jour **au fil de l'eau**, pas en big-bang final.
- **Pas de critère de sortie explicite** (« Definition of Done ») par phase :
  d'où un `TODO.md` à 0 case cochée alors que tout est fait — personne ne savait
  formellement quand cocher.
- **Gestion du suivi** : le plan détaillé existait (`REFONTE*.md`) mais n'a pas
  été tenu à jour pendant l'exécution → perte de traçabilité entre plan, PRs et
  CHANGELOG.

---

## 9. Trois leçons à retenir

1. **Tester la chaîne, pas que les pièces.** 143 tests verts n'ont pas empêché
   un backfill qui écrivait 0 ligne. Un seul test E2E réel l'aurait vu.
2. **La donnée est le livrable, pas le code.** Une refonte « réussie » qui
   laisse 120 fichiers illisibles et 0 collecte vérifiée n'a pas atteint son but.
3. **La doc et le suivi se font au fil de l'eau.** Repoussés en P8, ils ont été
   sacrifiés. Cocher le TODO et mettre à jour le CHANGELOG doivent faire partie
   de la *Definition of Done* de chaque PR.
