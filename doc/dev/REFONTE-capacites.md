# Refonte dccd — Matrice de capacités des exchanges

> Annexe de [`REFONTE.md`](REFONTE.md) §5. Croise **ce que le code actuel
> implémente** (`histo_dl/`, `continuous_dl/`) avec **la doc API officielle**
> (juin 2026). Sert de base factuelle aux modèles canoniques (§4) et au
> resolver/capacités (§5).
>
> Légende : ✅ dispo · ⚠️ partiel/contraint · ❌ indisponible (gratuitement)
> · *(code)* = déjà implémenté dans dccd · `n` = max items/requête.

---

## 1. OHLC (candles / klines)

| Exchange | Endpoint REST | max/req | Historique | Intervalles | Canal WS candle |
|---|---|---|---|---|---|
| Binance | `/api/v3/klines` *(code)* | **1000** | ✅ complet (`startTime`/`endTime`) | 1s,1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M | ✅ `@kline_{i}` |
| Bybit | `/v5/market/kline` *(code)* | **1000** | ✅ (`start`/`end`) | 1,3,5,15,30,60,120,240,360,720 min, D,W,M | ✅ `kline.{i}` |
| Coinbase | `/products/{}/candles` *(code)* | **300** | ✅ mais **fenêtrage obligatoire** (le bug 400) | 60,300,900,3600,21600,86400 s (figés) | ✅ `candles` (Advanced Trade) |
| Kraken | `/0/public/OHLC` *(code)* | **720** | ⚠️ **720 récents only** — l'ancien est irrécupérable | 1,5,15,30,60,240,1440,10080,21600 min | ✅ `ohlc` (v2) |
| OKX | `/market/candles` + `/market/history-candles` *(code)* | ~**300** récents / **100** histo¹ | ✅ via `history-candles` (pagination `after`/`before`) | 1m…1Y | ✅ `candle{i}` |
| Bitfinex | `/v2/candles` | **10000** | ✅ complet (`start`/`end`) | 1m,5m,15m,30m,1h,3h,6h,12h,1D,1W,14D,1M | ✅ `candles` |
| BitMEX | `/api/v1/trade/bucketed` | **1000** | ✅ complet (`startTime`, `reverse`) | **1m,5m,1h,1d uniquement** | ✅ `tradeBin{1m,5m,1h,1d}` |

¹ OKX : valeurs établies, **à reconfirmer sur doc** (page SPA non extractible
automatiquement). `candles` sert le récent, `history-candles` l'ancien.

**⚠️ Conséquence majeure — Kraken** : pas d'historique OHLC via REST. L'histo
OHLC Kraken doit être **dérivé des Trades** (qui, eux, remontent à l'origine).
C'est déjà ce que fait `KrakenBackfill`. → valide le besoin de **dérivation**
(§6 de REFONTE).

---

## 2. Trades

| Exchange | Endpoint(s) REST | max/req | Historique profond | Pagination | Canal WS trades |
|---|---|---|---|---|---|
| Binance | `/aggTrades` *(code)*, `/trades`, `/historicalTrades` | **1000** | ✅ complet via `aggTrades` (`fromId`/`startTime`, sans auth) | avant (fromId croissant) | ✅ `@trade` / `@aggTrade` |
| Kraken | `/0/public/Trades` *(code)* | ~**1000** | ✅ **complet** (`since=0`, curseur `last`) | avant (`since`→`last`) | ✅ `trade` |
| Bitfinex | `/v2/trades` | **10000** | ✅ complet (`start`/`end`) | bidirectionnel | ✅ `trades` |
| BitMEX | `/api/v1/trade` | **1000** | ✅ complet (`startTime`, `reverse`) | avant/arrière | ✅ `trade` |
| OKX | `/market/trades` + `/market/history-trades` *(code: trades only)* | **500** récents / **100** histo¹ | ⚠️ via `history-trades` (paginé) | arrière (`after`/`before`) | ✅ `trades` |
| Coinbase | `/products/{}/trades` *(code)* | **100** | ⚠️ curseur `before`/`after` (récent d'abord, remontée lente) | curseur (headers CB-BEFORE/AFTER) | ✅ `matches`/`market_trades` |
| Bybit | `/v5/market/recent-trade` *(code, limit=1000)* | **spot : 60** | ❌ **récent only** (pas d'histo profond spot) | aucune | ✅ `publicTrade` |

**⚠️ Bug latent actuel** : le code Bybit demande `limit=1000` mais le spot est
plafonné à **60** → on ne récupère que les 60 derniers trades, jamais l'histo.
Pour l'histo Bybit : OHLC via `kline` (a l'histo), trades → **collecte forward
via WS uniquement**.

---

## 3. Order book

| Exchange | REST snapshot (max prof.) | Canal WS | Modèle WS |
|---|---|---|---|
| Binance | `/api/v3/depth` *(code)* — **5000** | `@depth` / `@depth{5,10,20}@100ms` | diff updates (à resync sur snapshot) |
| Kraken | `/0/public/Depth` *(code)* — **500** | `book` {10,25,100,500,1000} | snapshot + updates |
| Coinbase | `/products/{}/book` *(code)* — L1/L2(top 50 agg.)/L3(full) | `level2` | snapshot + `l2update` |
| Bybit | `/v5/market/orderbook` *(code)* — spot **[1,200]** | `orderbook.{1,50,200}` | snapshot + delta |
| OKX | `/market/books` (**sz ≤ 400**) + `/market/books-full` (≤ 5000)¹ | `books` / `books5` / `books-l2-tbt` | snapshot + update |
| Bitfinex | `/v2/book` — précision P0–P4 + R0(raw), len {1,25,100,250} | `book` | snapshot + updates |
| BitMEX | `/orderBook/L2` (depth param) | `orderBookL2` / `orderBookL2_25` / `orderBook10` | full L2 par id |

**⚠️ Constat transverse n°1** : **aucun historique d'order book gratuit**, nulle
part. Le snapshot REST = état instantané ; le WS = flux live. → pour avoir de
l'histo carnet, il faut **enregistrer soi-même le flux WS** puis le relire.
C'est exactement le cas d'usage « récupérer l'histo depuis une data déjà
collectée » (§6) : le store WS devient la source historique.

---

## 4. Formats de symboles (→ besoin d'un `Symbol` central)

| Exchange | Spot | Particularités |
|---|---|---|
| Binance | `BTCUSDT` | concaténé, pas de séparateur |
| Bybit | `BTCUSDT` | idem |
| Coinbase | `BTC-USD` | tiret |
| OKX | `BTC-USDT` | tiret ; perp = `…-SWAP` |
| Kraken | `XBTUSD` (req) / `XXBTZUSD` (clés réponse) | **alias XBT=BTC**, clés réponse non triviales ; WS v2 = `BTC/USD` |
| Bitfinex | `tBTCUSD` | préfixe `t` (trading) / `f` (funding) |
| BitMEX | `XBTUSD` | XBT=BTC, instruments perp/futures |

→ La normalisation `Symbol(base, quote)` + rendu/parse par-exchange est
**incontournable** (§4). Cas durs : Kraken (alias + clés réponse), Bitfinex
(préfixe), XBT↔BTC partout.

---

## 5. Constats transverses (entrées de design)

1. **Pas d'histo order book gratuit** → le store des flux WS EST la source
   historique du carnet. Modéliser le store comme une `Source` (§6).
2. **Histo OHLC Kraken inexistant via REST** (720 récents) → **dérivation
   Trades→OHLC obligatoire**. Idem utile partout pour combler des trous.
3. **Histo trades Bybit spot inexistant** (60 récents) → seul le forward-collect
   WS alimente l'histo trades Bybit ; pour l'OHLC Bybit, utiliser `kline`.
4. **Plafonds de pagination de 60 à 10000** selon exchange/type → le **Paginator
   central** doit être paramétré par la capacité déclarée (généralise le fix
   Coinbase 300). Jamais de chunking en dur par exchange.
5. **Sens de pagination hétérogène** : avant (`fromId`/`startTime`/`since`) vs
   arrière (curseur `after`/`before`/`reverse`). Le Paginator doit gérer les deux.
6. **Order book WS = snapshot + deltas** partout (sauf BitMEX full-by-id) →
   logique commune « appliquer un delta à un état local + checkpoint » (déjà
   amorcée dans `continuous_dl`), à mutualiser dans un adapter générique.
7. **Granularités OHLC contraintes** : Coinbase (6 valeurs figées), BitMEX
   (1m/5m/1h/1d only) → la capacité doit déclarer les spans supportés, et le
   resolver proposer la **dérivation** (ex. 1h dérivé de 1m) quand un span natif
   manque.

---

## 6. Synthèse capacités (vue compacte)

| Exchange | OHLC histo | Trades histo | Book histo | Trades live | Book live | OHLC live |
|---|---|---|---|---|---|---|
| Binance | ✅ REST | ✅ REST | ❌ (→WS) | ✅ | ✅ | ✅ |
| Bybit | ✅ REST | ❌ (→WS) | ❌ (→WS) | ✅ | ✅ | ✅ |
| Coinbase | ✅ REST⚠️300 | ⚠️ REST lent | ❌ (→WS) | ✅ | ✅ | ✅ |
| Kraken | ❌→dérivé trades | ✅ REST | ❌ (→WS) | ✅ | ✅ | ✅ |
| OKX | ✅ REST | ⚠️ REST | ❌ (→WS) | ✅ | ✅ | ✅ |
| Bitfinex | ✅ REST | ✅ REST | ❌ (→WS) | ✅ | ✅ | ✅ |
| BitMEX | ✅ REST | ✅ REST | ❌ (→WS) | ✅ | ✅ | ✅ |

« ❌ (→WS) » = pas d'historique gratuit ; alimenté uniquement en collectant le
flux WS dans le temps (puis relu depuis le store).

---

## 7. À reconfirmer / approfondir

- OKX : valeurs exactes `limit`/`sz` (doc SPA non extractible auto) — confirmer
  candles 300, history-candles 100, trades 500, history-trades 100, books 400.
- Coinbase : profondeur réelle atteignable des trades via curseur (coût/temps).
- Spans natifs exacts par exchange (table d'intervalles complète) pour §4.
- Rate limits chiffrés par endpoint (poids/req, req/s) → dimensionner le
  RateLimiter central.
- Endpoints dérivés/futures (funding, OI, mark) si extension de périmètre (§4).
