# Test Fixtures

Verbatim live payloads captured from public exchange endpoints on **2026-06-11**.
Re-capture commands are listed below so drift can be spotted when suspected.

## Bitfinex

### `bitfinex_ohlc_page.json`
5 OHLC bars (1h) for tBTCUSD, sorted ascending.
```
curl -s "https://api-pub.bitfinex.com/v2/candles/trade:1h:tBTCUSD/hist?limit=5&sort=1"
```

### `bitfinex_trades_page.json`
5 trades for tBTCUSD, ascending (oldest first).
```
curl -s "https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist?limit=5&sort=1"
```

### `bitfinex_ws_trade_msgs.json`
Hand-constructed representative WS frames (subscribe ack, heartbeat, buy trade, sell trade).
Format: `[chanId, [tid, ts_ms, amount, price]]` — amount positive = buy, negative = sell.

### `bitfinex_ws_candle_msgs.json`
Hand-constructed representative WS frames (subscribe ack, snapshot array of bars, single update bar).

---

## BitMEX

### `bitmex_ohlc_page.json`
5 bucketed 1h candles for XBTUSD from 2024-01-01.
```
curl -s "https://www.bitmex.com/api/v1/trade/bucketed?symbol=XBTUSD&binSize=1h&count=5&reverse=false&startTime=2024-01-01T00:00:00Z"
```

### `bitmex_trades_page.json`
5 trades for XBTUSD from 2024-01-01. Last entry has side=Buy for Buy/Sell coverage.
```
curl -s "https://www.bitmex.com/api/v1/trade?symbol=XBTUSD&count=5&reverse=false&startTime=2024-01-01T00:00:00Z"
```
(last entry manually set to side=Buy for branch coverage; price/size unchanged)

### `bitmex_ws_trade_msgs.json`
Representative WS frames: welcome message, subscription ack, insert with two trades (Sell + Buy).

---

## Coinbase

### `coinbase_candles_page.json`
6 hourly candles for BTC-USD, 2024-01-01. Coinbase returns newest-first.
```
curl -s "https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=3600&start=2024-01-01T00:00:00Z&end=2024-01-01T06:00:00Z"
```
Format: `[time_s, low, high, open, close, volume]`

### `coinbase_trades_page.json`
5 recent trades for BTC-USD (live capture, 2026-06-11T12:53Z).
```
curl -s "https://api.exchange.coinbase.com/products/BTC-USD/trades?limit=5"
```

### `coinbase_ws_trade_msgs.json`
Representative WS frames from the Coinbase Advanced Trade WebSocket (market_trades channel).
Contains a snapshot event (2 trades) and an update event (1 trade).

---

## Binance

### `binance_klines_page.json`
5 hourly klines for BTCUSDT (recent).
```
curl -s "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=5"
```
Format: `[open_time_ms, open, high, low, close, volume, close_time_ms, quote_vol, trades, ...]`

### `binance_aggtrades_page.json`
5 aggregate trades for BTCUSDT starting 2024-01-01.
```
curl -s "https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT&limit=5&startTime=1704067200000"
```

### `binance_ws_kline_msgs.json`
Representative WS kline frames (two closed bars, one ignored event type).

### `binance_ws_agg_trade_msgs.json`
Representative WS aggTrade frames (sell trade, buy trade, one ignored event type).
