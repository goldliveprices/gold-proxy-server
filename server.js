'use strict';

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

// ─── ENV ─────────────────────────────────────────────────────────
const PORT              = process.env.PORT              || 3000;
const SELF_URL          = process.env.SELF_URL          || '';
const DHAN_CLIENT_ID    = process.env.DHAN_CLIENT_ID    || '';
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN || '';
const DHAN_API_BASE     = 'https://api.dhan.co/v2';

// ─── FEED CODES (from Dhan docs) ──────────────────────────────────
// 2=Ticker, 4=Quote, 5=OI, 6=PrevClose, 8=Full, 50=Disconnect
const FC = { TICKER: 2, QUOTE: 4, OI: 5, PREV_CLOSE: 6, FULL: 8, DISCONNECT: 50 };

// ─── TOKENS ───────────────────────────────────────────────────────
const TOKENS = {
  '436177': { key: 'gold',       name: 'GOLD-JUN2026-MCX' },
  '436178': { key: 'goldNext',   name: 'GOLD-AUG2026-MCX' },
  '436197': { key: 'silver',     name: 'SILVER-JUL2026-MCX' },
  '436198': { key: 'silverNext', name: 'SILVER-SEP2026-MCX' },
};

// ─── LIVE STATE ───────────────────────────────────────────────────
const liveTick = {
  gold:       { ltp: 0, bid: 0, ask: 0, high: 0, low: 0, open: 0, prevClose: 0, ts: 0 },
  goldNext:   { ltp: 0, bid: 0, ask: 0, high: 0, low: 0, open: 0, prevClose: 0, ts: 0 },
  silver:     { ltp: 0, bid: 0, ask: 0, high: 0, low: 0, open: 0, prevClose: 0, ts: 0 },
  silverNext: { ltp: 0, bid: 0, ask: 0, high: 0, low: 0, open: 0, prevClose: 0, ts: 0 },
};

const sessionHL = {
  gold:   { high: 0, low: Infinity },
  silver: { high: 0, low: Infinity },
};

const WS = {
  ws: null, status: 'disconnected',
  reconnectCount: 0, reconnectTimer: null,
  lastConnectAt: null, lastDisconnectAt: null,
  lastTickAt: null, lastDisconnectCode: null,
  lastRawBufHex: '', lastTextMsg: '',
  packetsReceived: 0,
};

let lastKnownRates = null;

// ─── FOREX CACHE ──────────────────────────────────────────────────
const forexCache = {
  usdInr: 94.5, xauUsd: 3310, xagUsd: 32.8,
  updatedAt: null, src: 'init',
};

async function refreshForex() {
  let usdInr = 0, src = '';
  const fxSrc = [
    ['frankfurter', async () => {
      const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 5000 });
      return r.data.rates.INR;
    }],
    ['open.er-api', async () => {
      const r = await axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 5000 });
      return r.data.rates.INR;
    }],
  ];

  for (const [name, fn] of fxSrc) {
    if (usdInr) break;
    try {
      const v = await fn();
      if (v > 70 && v < 110) { usdInr = v; src = name; }
    } catch (e) { console.warn('[FOREX]', name, e.message); }
  }

  if (!usdInr) { usdInr = forexCache.usdInr; src = 'cached'; }

  let xauUsd = 0, xagUsd = 0;
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout: 6000 });
    if (Array.isArray(r.data)) {
      const g = r.data.find(x => x.gold); const s = r.data.find(x => x.silver);
      if (g && g.gold > 3000) xauUsd = g.gold;
      if (s && s.silver > 20) xagUsd = s.silver;
    }
  } catch (e) { console.warn('[SPOT] metals.live', e.message); }

  if (!xauUsd) {
    try {
      const [g, s] = await Promise.all([
        axios.get('https://www.gold-api.com/price/XAU', { timeout: 7000 }),
        axios.get('https://www.gold-api.com/price/XAG', { timeout: 7000 }),
      ]);
      if (g.data.price > 3000) xauUsd = g.data.price;
      if (s.data.price > 20)   xagUsd = s.data.price;
    } catch (e) { console.warn('[SPOT] gold-api', e.message); }
  }

  forexCache.usdInr    = usdInr    || forexCache.usdInr;
  forexCache.xauUsd    = xauUsd    || forexCache.xauUsd;
  forexCache.xagUsd    = xagUsd    || forexCache.xagUsd;
  forexCache.updatedAt = new Date().toISOString();
  forexCache.src       = src;
  console.log('[FOREX] usdInr=%s xauUsd=%s xagUsd=%s src=%s',
    forexCache.usdInr, forexCache.xauUsd, forexCache.xagUsd, src);
}

function spotDerived() {
  const { usdInr, xauUsd, xagUsd } = forexCache;
  const F = 1.103;
  return {
    goldPer10g:  Math.round((xauUsd / 31.1035) * 10   * usdInr * F),
    silverPerKg: Math.round((xagUsd / 31.1035) * 1000 * usdInr * F),
  };
}

// ─── HELPERS ──────────────────────────────────────────────────────
function getIST() {
  const d = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  return { dow: d.getUTCDay(), h: d.getUTCHours(), m: d.getUTCMinutes() };
}

function isMCXOpen() {
  const { dow, h, m } = getIST();
  if (dow === 0) return false;
  const t = h * 60 + m;
  return dow === 6 ? (t >= 540 && t < 840) : (t >= 540 && t < 1435);
}

function tickAge() {
  return WS.lastTickAt ? Math.floor((Date.now() - WS.lastTickAt) / 1000) : Infinity;
}

function isDhanLive() {
  return WS.status === 'connected' && tickAge() < 10 && liveTick.gold.ltp > 0;
}

function updateHL(sym, ltp, high, low) {
  if (ltp > 0) {
    if (ltp > sessionHL[sym].high) sessionHL[sym].high = ltp;
    if (ltp < sessionHL[sym].low)  sessionHL[sym].low  = ltp;
  }
  if (high > 0 && high > sessionHL[sym].high) sessionHL[sym].high = high;
  if (low  > 0 && low  < sessionHL[sym].low)  sessionHL[sym].low  = low;
}

// ─── BINARY PARSER — EXACT DHAN DOCS BYTE OFFSETS ─────────────────
/*
  Header (8 bytes):
    byte[0]    = Feed Response Code (uint8)
    byte[1-2]  = Message Length (int16 LE)
    byte[3]    = Exchange Segment (uint8)
    byte[4-7]  = Security ID (int32 LE)

  TICKER (code=2): total 16 bytes
    [8-11]  float32 LE  = LTP
    [12-15] int32 LE    = LTT

  PREV_CLOSE (code=6): total 16 bytes
    [8-11]  float32 LE  = Prev Close Price
    [12-15] int32 LE    = OI prev day

  QUOTE (code=4): total 50 bytes
    [8-11]  float32 LE  = LTP
    [12-13] int16 LE    = LTQ
    [14-17] int32 LE    = LTT
    [18-21] float32 LE  = ATP
    [22-25] int32 LE    = Volume
    [26-29] int32 LE    = Total Sell Qty
    [30-33] int32 LE    = Total Buy Qty
    [34-37] float32 LE  = Open
    [38-41] float32 LE  = Close (post market)
    [42-45] float32 LE  = High
    [46-49] float32 LE  = Low

  FULL (code=8): total 162 bytes
    [8-11]  float32 LE  = LTP
    [12-13] int16 LE    = LTQ
    [14-17] int32 LE    = LTT
    [18-21] float32 LE  = ATP
    [22-25] int32 LE    = Volume
    [26-29] int32 LE    = Total Sell Qty
    [30-33] int32 LE    = Total Buy Qty
    [34-37] int32 LE    = OI
    [38-41] int32 LE    = OI High
    [42-45] int32 LE    = OI Low
    [46-49] float32 LE  = Open
    [50-53] float32 LE  = Close
    [54-57] float32 LE  = High
    [58-61] float32 LE  = Low
    [62-161] Market Depth (5 x 20 bytes)
      each depth packet:
        [0-3]   int32 LE  = Bid Qty
        [4-7]   int32 LE  = Ask Qty
        [8-9]   int16 LE  = Bid Orders
        [10-11] int16 LE  = Ask Orders
        [12-15] float32 LE = Bid Price
        [16-19] float32 LE = Ask Price
*/
function parseBuf(buf) {
  try {
    if (!buf || buf.length < 8) return null;
    const fc    = buf.readUInt8(0);
    const secId = buf.readInt32LE(4).toString();

    if (fc === FC.DISCONNECT) {
      const code = buf.length >= 10 ? buf.readInt16LE(8) : 0;
      WS.lastDisconnectCode = code;
      console.warn('[WS] Server disconnect packet code=', code);
      return null;
    }

    if (fc === FC.PREV_CLOSE && buf.length >= 16) {
      const prevClose = buf.readFloatLE(8);
      return { type: 'prevClose', secId, prevClose: Math.round(prevClose) };
    }

    if (fc === FC.TICKER && buf.length >= 16) {
      const ltp = buf.readFloatLE(8);
      const ltt = buf.readUInt32LE(12);
      if (!Number.isFinite(ltp) || ltp <= 0) return null;
      return { type: 'ticker', secId, ltp: Math.round(ltp), ltt };
    }

    if (fc === FC.QUOTE && buf.length >= 50) {
      const ltp  = buf.readFloatLE(8);
      const ltt  = buf.readUInt32LE(14);
      const atp  = buf.readFloatLE(18);
      const vol  = buf.readUInt32LE(22);
      const open = buf.readFloatLE(34);
      const high = buf.readFloatLE(42);
      const low  = buf.readFloatLE(46);
      if (!Number.isFinite(ltp) || ltp <= 0) return null;
      return {
        type: 'quote', secId,
        ltp: Math.round(ltp), bid: Math.round(ltp), ask: Math.round(ltp),
        open: Math.round(open), high: Math.round(high), low: Math.round(low),
        atp, vol, ltt,
      };
    }

    if (fc === FC.FULL && buf.length >= 162) {
      const ltp  = buf.readFloatLE(8);
      const ltt  = buf.readUInt32LE(14);
      const open = buf.readFloatLE(46);
      const high = buf.readFloatLE(54);
      const low  = buf.readFloatLE(58);

      // Market depth — 5 levels starting at byte 62
      let bestBid = 0, bestAsk = 0;
      for (let i = 0; i < 5; i++) {
        const base = 62 + i * 20;
        const bidP = buf.readFloatLE(base + 12);
        const askP = buf.readFloatLE(base + 16);
        if (i === 0) { bestBid = bidP; bestAsk = askP; }
      }

      if (!Number.isFinite(ltp) || ltp <= 0) return null;
      return {
        type: 'full', secId,
        ltp:  Math.round(ltp),
        bid:  bestBid > 0 ? Math.round(bestBid) : Math.round(ltp),
        ask:  bestAsk > 0 ? Math.round(bestAsk) : Math.round(ltp),
        open: Math.round(open), high: Math.round(high), low: Math.round(low),
        ltt,
      };
    }

    return null;
  } catch (e) {
    console.warn('[PARSE] err', e.message);
    return null;
  }
}

// ─── WEBSOCKET ────────────────────────────────────────────────────
function buildWsUrl() {
  return 'wss://api-feed.dhan.co?version=2'
    + '&token='    + encodeURIComponent(DHAN_ACCESS_TOKEN)
    + '&clientId=' + encodeURIComponent(DHAN_CLIENT_ID)
    + '&authType=2';
}

function subscribe(ws) {
  const instruments = Object.keys(TOKENS).map(secId => ({
    ExchangeSegment: 'MCX_COMM',
    SecurityId: secId,
  }));

  [15, 17, 21].forEach((code, idx) => {
    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      ws.send(JSON.stringify({
        RequestCode: code,
        InstrumentCount: instruments.length,
        InstrumentList: instruments,
      }));
      console.log('[WS] Sent RequestCode', code);
    }, idx * 400);
  });
}

function connectDhan() {
  if (!DHAN_CLIENT_ID || !DHAN_ACCESS_TOKEN) {
    console.warn('[WS] Missing credentials'); return;
  }
  if (WS.status === 'connecting' || WS.status === 'connected') return;

  WS.status = 'connecting';
  WS.lastConnectAt = new Date().toISOString();
  WS.lastDisconnectCode = null;
  WS.packetsReceived = 0;
  console.log('[WS] Connecting...');

  const ws = new WebSocket(buildWsUrl(), { handshakeTimeout: 15000 });
  WS.ws = ws;

  ws.on('open', () => {
    WS.status = 'connected';
    WS.reconnectCount = 0;
    console.log('[WS] Connected — subscribing');
    subscribe(ws);
  });

  ws.on('message', (data) => {
    if (typeof data === 'string') {
      WS.lastTextMsg = data.slice(0, 500);
      console.log('[WS] Text:', WS.lastTextMsg);
      return;
    }

    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    WS.packetsReceived++;

    // Log first 5 raw packets for debug
    if (WS.packetsReceived <= 5) {
      const hex = buf.slice(0, Math.min(buf.length, 32)).toString('hex');
      WS.lastRawBufHex = hex;
      console.log('[WS] Packet #%d feedCode=%d len=%d hex=%s',
        WS.packetsReceived, buf.readUInt8(0), buf.length, hex);
    }

    const tick = parseBuf(buf);
    if (!tick) return;

    const token = TOKENS[tick.secId];
    if (!token) {
      console.log('[WS] Unknown secId=%s ltp=%d', tick.secId, tick.ltp);
      return;
    }

    const key = token.key;
    WS.lastTickAt = Date.now();

    if (tick.type === 'prevClose') {
      liveTick[key].prevClose = tick.prevClose;
      console.log('[WS] PrevClose %s = %d', key, tick.prevClose);
      return;
    }

    // Merge tick into live state
    liveTick[key] = {
      ...liveTick[key],
      ltp:  tick.ltp  || liveTick[key].ltp,
      bid:  tick.bid  || liveTick[key].bid,
      ask:  tick.ask  || liveTick[key].ask,
      open: tick.open || liveTick[key].open,
      high: tick.high || liveTick[key].high,
      low:  tick.low  || liveTick[key].low,
      ts:   WS.lastTickAt,
    };

    if (key === 'gold' || key === 'silver') {
      updateHL(key, tick.ltp, tick.high, tick.low);
    }
  });

  ws.on('close', (code, reason) => {
    WS.status = 'disconnected';
    WS.lastDisconnectAt = new Date().toISOString();
    console.warn('[WS] Closed code=%s reason=%s', code,
      reason && reason.toString ? reason.toString() : '');
    scheduleReconnect();
  });

  ws.on('error', (err) => {
    console.warn('[WS] Error:', err.message);
  });
}

function scheduleReconnect() {
  if (WS.reconnectTimer) return;
  WS.reconnectCount++;
  const delay = Math.min(2000 * Math.pow(2, Math.min(WS.reconnectCount, 5)), 60000);
  console.log('[WS] Reconnect in', delay / 1000, 's');
  WS.reconnectTimer = setTimeout(() => {
    WS.reconnectTimer = null;
    connectDhan();
  }, delay);
}

// ─── ROUTES ───────────────────────────────────────────────────────
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  const now        = new Date().toISOString();

  if (isDhanLive()) {
    const g  = liveTick.gold;
    const s  = liveTick.silver;
    const gN = liveTick.goldNext;
    const sN = liveTick.silverNext;
    const payload = {
      success: true, source: 'dhan_mcx_live', marketOpen,
      tickAgeMs: Date.now() - WS.lastTickAt,
      goldPer10g: g.ltp, silverPerKg: s.ltp,
      futures: {
        gold:       { ltp: g.ltp,  bid: g.bid,  ask: g.ask,  high: sessionHL.gold.high || g.high,   low: sessionHL.gold.low === Infinity ? g.low : sessionHL.gold.low, open: g.open, prevClose: g.prevClose },
        silver:     { ltp: s.ltp,  bid: s.bid,  ask: s.ask,  high: sessionHL.silver.high || s.high, low: sessionHL.silver.low === Infinity ? s.low : sessionHL.silver.low, open: s.open, prevClose: s.prevClose },
        goldNext:   { ltp: gN.ltp || g.ltp, bid: gN.bid || g.bid, ask: gN.ask || g.ask },
        silverNext: { ltp: sN.ltp || s.ltp, bid: sN.bid || s.bid, ask: sN.ask || s.ask },
      },
      usdInr: forexCache.usdInr, xauUsd: forexCache.xauUsd,
      forexUpdatedAt: forexCache.updatedAt, timestamp: now,
    };
    lastKnownRates = { ...payload };
    return res.json(payload);
  }

  if (lastKnownRates) {
    return res.json({
      ...lastKnownRates,
      source: 'last_known_rates',
      tickAgeSeconds: tickAge() === Infinity ? null : tickAge(),
      timestamp: now,
    });
  }

  const { goldPer10g, silverPerKg } = spotDerived();
  return res.json({
    success: true, source: 'spot_derived', marketOpen,
    note: 'MCX feed unavailable — spot derived',
    goldPer10g, silverPerKg,
    usdInr: forexCache.usdInr, xauUsd: forexCache.xauUsd,
    forexUpdatedAt: forexCache.updatedAt, timestamp: now,
  });
});

app.get('/debug', (req, res) => {
  res.json({
    server: 'RR Jewellers WS v3 (docs-exact)',
    wsStatus: WS.status,
    lastTickAt: WS.lastTickAt ? new Date(WS.lastTickAt).toISOString() : null,
    tickAgeSeconds: tickAge() === Infinity ? null : tickAge(),
    packetsReceived: WS.packetsReceived,
    lastRawBufHex: WS.lastRawBufHex,
    lastTextMsg: WS.lastTextMsg,
    lastDisconnectCode: WS.lastDisconnectCode,
    reconnectCount: WS.reconnectCount,
    lastConnectAt: WS.lastConnectAt,
    lastDisconnectAt: WS.lastDisconnectAt,
    marketOpen: isMCXOpen(),
    liveTick, sessionHL, forexCache,
    credentials: { clientId: !!DHAN_CLIENT_ID, accessToken: !!DHAN_ACCESS_TOKEN },
    env: { SELF_URL: SELF_URL || null },
  });
});

app.get('/ping', (req, res) => res.json({ ok: true, ts: Date.now() }));
app.get('/', (req, res) => res.json({
  status: 'RR Jewellers WS v3',
  endpoints: ['/rates', '/debug', '/ping'],
}));

// ─── STARTUP ──────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', async () => {
  console.log('[STARTUP] RR Jewellers WS v3 on port', PORT);
  try { await refreshForex(); } catch (e) { console.warn('[STARTUP] forex err:', e.message); }
  connectDhan();
  setInterval(refreshForex, 5 * 60 * 1000);
  setInterval(() => {
    const url = SELF_URL || `http://localhost:${PORT}`;
    axios.get(url + '/ping').catch(() => {});
  }, 4 * 60 * 1000);
  setInterval(() => {
    if (WS.status === 'disconnected' && !WS.reconnectTimer) connectDhan();
  }, 60 * 1000);
});
