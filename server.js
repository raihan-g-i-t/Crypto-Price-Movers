require('dotenv').config();

const express = require('express');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';

const THRESHOLD_1M = Number(
  process.env.THRESHOLD_PERCENT_1M || process.env.THRESHOLD_PERCENT || 2
);
const THRESHOLD_5M = Number(process.env.THRESHOLD_PERCENT_5M || 5);
const THRESHOLD_15M = Number(process.env.THRESHOLD_PERCENT_15M || 8);

const BINANCE_REST = 'https://api.binance.com';
const BINANCE_WS = 'wss://stream.binance.com:9443/ws';

const INTERVALS = [
  { key: '1m', stream: 'kline_1m', threshold: THRESHOLD_1M },
  { key: '5m', stream: 'kline_5m', threshold: THRESHOLD_5M },
  { key: '15m', stream: 'kline_15m', threshold: THRESHOLD_15M },
];

// ---------- Telegram ----------

async function sendTelegramAlert(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    const res = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text,
        parse_mode: 'Markdown',
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      console.error('Telegram send failed:', res.status, body);
    }
  } catch (err) {
    console.error('Telegram send error:', err.message);
  }
}

// ---------- Binance symbol discovery ----------

async function fetchUsdtSymbols() {
  const res = await fetch(`${BINANCE_REST}/api/v3/exchangeInfo`);
  if (!res.ok) throw new Error(`exchangeInfo failed: ${res.status}`);
  const data = await res.json();
  return data.symbols
    .filter(
      (s) =>
        s.quoteAsset === 'USDT' &&
        s.status === 'TRADING' &&
        s.isSpotTradingAllowed
    )
    .map((s) => s.symbol);
}

// ---------- One monitor per interval (its own WebSocket + state) ----------

function createMonitor(cfg, symbols) {
  const state = new Map(); // symbol -> { symbol, price, open, pct, volume, candleStart, closed }
  const alertedCandle = new Map(); // symbol -> candleStart already alerted for
  let ws = null;
  let reconnectDelay = 1000;
  let stopped = false;

  function connect() {
    if (stopped) return;
    ws = new WebSocket(BINANCE_WS);

    ws.on('open', () => {
      console.log(`[${cfg.key}] connected to Binance stream`);
      reconnectDelay = 1000;

      const CHUNK_SIZE = 50;
      let idCounter = 1;
      let i = 0;

      const subscribeNextChunk = () => {
        if (i >= symbols.length) return;
        const chunk = symbols
          .slice(i, i + CHUNK_SIZE)
          .map((s) => `${s.toLowerCase()}@${cfg.stream}`);
        ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: chunk, id: idCounter++ }));
        i += CHUNK_SIZE;
        setTimeout(subscribeNextChunk, 250);
      };
      subscribeNextChunk();
    });

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }
      if (!msg.e || msg.e !== 'kline' || !msg.k) return;

      const k = msg.k;
      const symbol = msg.s;
      const open = parseFloat(k.o);
      const close = parseFloat(k.c);
      const volume = parseFloat(k.v);
      const pct = ((close - open) / open) * 100;

      state.set(symbol, {
        symbol,
        price: close,
        open,
        pct,
        volume,
        candleStart: k.t,
        closed: k.x,
      });

      if (Math.abs(pct) >= cfg.threshold) {
        const already = alertedCandle.get(symbol);
        if (already !== k.t) {
          alertedCandle.set(symbol, k.t);
          const direction = pct > 0 ? 'UP' : 'DOWN';
          const emoji = pct > 0 ? '🟢' : '🔴';
          sendTelegramAlert(
            `${emoji} *${symbol}* moved *${pct.toFixed(2)}%* ${direction} on its ${cfg.key} candle\nOpen: ${open}\nNow: ${close}`
          );
        }
      }
    });

    ws.on('close', () => {
      if (stopped) return;
      console.warn(`[${cfg.key}] stream closed, reconnecting in ${reconnectDelay}ms`);
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 30000);
    });

    ws.on('error', (err) => {
      console.error(`[${cfg.key}] stream error:`, err.message);
      ws.close();
    });
  }

  connect();

  return {
    state,
    stop() {
      stopped = true;
      if (ws) ws.close();
    },
  };
}

let monitors = {};

async function refreshMonitors() {
  let symbols;
  try {
    symbols = await fetchUsdtSymbols();
  } catch (err) {
    console.error('Failed to fetch symbol list, retrying in 5s:', err.message);
    setTimeout(refreshMonitors, 5000);
    return;
  }

  console.log(`Monitoring ${symbols.length} USDT pairs on ${INTERVALS.map((i) => i.key).join(' + ')} candles`);

  const old = monitors;
  monitors = {};
  INTERVALS.forEach((cfg, i) => {
    setTimeout(() => {
      monitors[cfg.key] = createMonitor(cfg, symbols);
    }, i * 1000);
  });
  Object.values(old).forEach((m) => m.stop());
}

refreshMonitors();

// Refresh symbol list occasionally (new listings/delistings) by fully reconnecting
setInterval(refreshMonitors, 6 * 60 * 60 * 1000);

// ---------- Web server + frontend push ----------

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = app.listen(PORT, () => {
  console.log(`Dashboard running at http://localhost:${PORT}`);
});

const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws) => {
  const thresholds = {};
  for (const cfg of INTERVALS) thresholds[cfg.key] = cfg.threshold;
  ws.send(JSON.stringify({ type: 'config', thresholds }));
});

function snapshotFor(key) {
  const m = monitors[key];
  if (!m) return [];
  return Array.from(m.state.values()).sort((a, b) => b.pct - a.pct);
}

function broadcastSnapshot() {
  const data = {};
  for (const cfg of INTERVALS) data[cfg.key] = snapshotFor(cfg.key);
  const payload = JSON.stringify({ type: 'snapshot', data });
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

setInterval(broadcastSnapshot, 1000);
