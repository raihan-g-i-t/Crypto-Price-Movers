# Binance 5% Monitor

Live monitor for Binance USDT spot pairs. It watches every USDT trading pair's
1-minute and 5-minute candles in real time, highlights the ones moving fast on
a web dashboard, and (optionally) sends Telegram alerts when a move crosses a
configurable percentage threshold.

## Requirements

- [Node.js](https://nodejs.org/) 18 or newer (uses the built-in `fetch` API)
- npm (comes with Node.js)

## Install

```bash
npm install
```

## Configure

Copy the example environment file and edit it as needed:

```bash
cp .env.example .env
```

| Variable | Description | Default |
| --- | --- | --- |
| `PORT` | Port the web dashboard is served on | `3000` |
| `THRESHOLD_PERCENT_1M` | % move on a 1-minute candle that triggers a highlight + Telegram alert | `2` |
| `THRESHOLD_PERCENT_5M` | % move on a 5-minute candle that triggers a highlight + Telegram alert | `5` |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token (optional, needed for alerts) | *(empty)* |
| `TELEGRAM_CHAT_ID` | Telegram chat id to send alerts to (optional, needed for alerts) | *(empty)* |

Telegram alerts are optional — leave `TELEGRAM_BOT_TOKEN` and
`TELEGRAM_CHAT_ID` blank to just use the web dashboard.

### Setting up Telegram alerts (optional)

1. Message [@BotFather](https://t.me/BotFather) on Telegram, send `/newbot`,
   and follow the prompts to create a bot. It will give you a bot token —
   put it in `TELEGRAM_BOT_TOKEN`.
2. Send any message to your new bot, then open
   `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates` in a browser.
   Find `"chat":{"id": ...}` in the response — that number is your chat id.
   Put it in `TELEGRAM_CHAT_ID`.

## Run

```bash
npm start
```

Then open [http://localhost:3000](http://localhost:3000) (or the port you set
in `PORT`) in your browser to see the live dashboard.

On startup, the server fetches all active USDT spot pairs from Binance,
subscribes to their 1m/5m kline streams over WebSocket, and pushes updated
snapshots to the dashboard every second. The symbol list is refreshed
automatically every 6 hours to pick up new listings/delistings.
