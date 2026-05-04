'use strict';

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

// ─── ENV VARS ──────────────────────────────────────────────────────
const PORT              = process.env.PORT              || 3000;
const SELF_URL          = process.env.SELF_URL          || '';
const SHEET_ID          = process.env.SHEET_ID          || '';
const DHAN_CLIENT_ID    = process.env.DHAN_CLIENT_ID    || '';
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN || '';

const DHAN_API_BASE = 'https://api.dhan.co/v2';

const FEED_CODE = { TICKER: 2, QUOTE: 4, FULL: 8, DISCONNECT: 50 };
const EXCH_SEG  = { MCX_COMM: 'MCX_COMM' };

// ─── STATE ─────────────────────────────────────────────────────────
let currentAccessToken = DHAN_ACCESS_TOKEN;
let tokenRenewedAt     = null;

const liveTick = {
  gold:       { ltp: 0, bid: 0, ask: 0, high: 0, low: 0, open: 0, ts: 0 },
  silver:     { ltp: 0, bid: 0, ask: 0, high: 0, low: 0, open: 0, ts: 0 },
  goldNext:   { ltp: 0, bid: 0, ask: 0, high: 0, low: 0, open: 0, ts: 0 },
  silverNext: { ltp: 0, bid: 0, ask: 0, high: 0, low: 0, open: 0, ts: 0 },
};

const sessionHL = {
  gold:   { high: 0, low: Infinity },
  silver: { high: 0, low: Infinity },
};

const WS = {
  ws: null,
  wsStatus: 'disconnected',
  reconnectCount: 0,
  reconnectTimer: null,
  lastConnectAt: null,
  lastDisconnectAt: null,
  lastTickAt: null,
  lastRawBufHex: '',
  lastTextMsg: '',
  lastDisconnectCode: null,
};

// Tokens for your four MCX contracts
const TOKENS = {
  goldCurrent:   { secId: '436177', symbol: 'GOLD-JUN2026-MCX-FUT' },
  goldNext:      { secId: '436178', symbol: 'GOLD-AUG2026-MCX-FUT' },
  silverCurrent: { secId: '436197', symbol: 'SILVER-JUL2026-MCX-FUT' },
  silverNext:    { secId: '436198', symbol: 'SILVER-SEP2026-MCX-FUT' },
};

let lastKnownRates = null;

const forexCache = {
  usdInr: 94.5,
  xauUsd: 3310,
  xagUsd: 32.8,
  updatedAt: null,
  src: 'init',
};

// ─── TIME HELPERS ──────────────────────────────────────────────────
function getIST() {
  const d = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  return {
    year: d.getUTCFullYear(),
    month: d.getUTCMonth(),
    day: d.getUTCDate(),
    hour: d.getUTCHours(),
    min: d.getUTCMinutes(),
    dow: d.getUTCDay(),
  };
}

function istDayKey() {
  const i = getIST();
  return `${i.year}-${i.month + 1}-${i.day}`;
}

let sessionDayKey = '';

function isMCXOpen() {
  const { dow, hour, min } = getIST();
  if (dow === 0) return false; // Sunday
  const t = hour * 60 + min;
  if (dow === 6) {
    // Saturday: 9:00–14:00
    return t >= 540 && t < 840;
  }
  // Mon–Fri: 9:00–23:59
  return t >= 540 && t < 1435;
}

function resetSessionIfNewDay() {
  const k = istDayKey();
  if (sessionDayKey !== k) {
    sessionDayKey = k;
    sessionHL.gold   = { high: 0, low: Infinity };
    sessionHL.silver = { high: 0, low: Infinity };
    console.log('[SESSION] Reset for day', k);
  }
}

function updateSessionHL(sym, ltp, high, low) {
  resetSessionIfNewDay();
  if (ltp && ltp > 0) {
    if (ltp > sessionHL[sym].high) sessionHL[sym].high = ltp;
    if (ltp < sessionHL[sym].low)  sessionHL[sym].low  = ltp;
  }
  if (high && high > 0 && high > sessionHL[sym].high) sessionHL[sym].high = high;
  if (low  && low  > 0 && low  < sessionHL[sym].low)  sessionHL[sym].low  = low;
}

function tickAgeSeconds() {
  if (!WS.lastTickAt) return Infinity;
  return Math.floor((Date.now() - WS.lastTickAt) / 1000);
}

function isDhanLive() {
  return WS.wsStatus === 'connected' && tickAgeSeconds() < 10 && liveTick.gold.ltp > 0;
}

function isDhanStale() {
  const age = tickAgeSeconds();
  return liveTick.gold.ltp > 0 && age >= 10 && age < 300;
}

// ─── FOREX + SPOT ──────────────────────────────────────────────────
async function refreshForexAndSpot() {
  let usdInr = 0;
  let xauUsd = 0;
  let xagUsd = 0;
  let src = '';

  const fxSources = [
    ['frankfurter', async () => {
      const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 5000 });
      return r.data && r.data.rates && r.data.rates.INR;
    }],
    ['open.er-api', async () => {
      const r = await axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 5000 });
      return r.data && r.data.rates && r.data.rates.INR;
    }],
    ['fawazahmed0', async () => {
      const r = await axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json', { timeout: 5000 });
      return r.data && r.data.usd && r.data.usd.inr;
    }],
  ];

  for (const [name, fn] of fxSources) {
    if (usdInr) break;
    try {
      const v = await fn();
      if (v && v > 70 && v < 110) {
        usdInr = v;
        src = name;
      }
    } catch (e) {
      console.warn('[FOREX]', name, 'failed:', e.message);
    }
  }

  if (!usdInr) {
    usdInr = forexCache.usdInr;
    src = forexCache.src || 'cached';
  }

  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout: 6000 });
    if (Array.isArray(r.data)) {
      const g = r.data.find(x => Object.prototype.hasOwnProperty.call(x, 'gold'));
      const s = r.data.find(x => Object.prototype.hasOwnProperty.call(x, 'silver'));
      if (g && g.gold > 3000 && g.gold < 9000) xauUsd = g.gold;
      if (s && s.silver > 20 && s.silver < 300) xagUsd = s.silver;
    }
  } catch (e) {
    console.warn('[SPOT] metals.live failed:', e.message);
  }

  if (!xauUsd || !xagUsd) {
    try {
      const [gr, sr] = await Promise.all([
        axios.get('https://www.gold-api.com/price/XAU', { timeout: 7000 }),
        axios.get('https://www.gold-api.com/price/XAG', { timeout: 7000 }),
      ]);
      const g = gr.data && gr.data.price;
      const s = sr.data && sr.data.price;
      if (g && g > 3000 && g < 9000) xauUsd = g;
      if (s && s > 20 && s < 300) xagUsd = s;
    } catch (e) {
      console.warn('[SPOT] gold-api failed:', e.message);
    }
  }

  if (!xauUsd || !xagUsd) {
    xauUsd = forexCache.xauUsd;
    xagUsd = forexCache.xagUsd;
  }

  forexCache.usdInr = usdInr;
  forexCache.xauUsd = xauUsd;
  forexCache.xagUsd = xagUsd;
  forexCache.updatedAt = new Date().toISOString();
  forexCache.src = src;

  console.log('[FOREX] usdInr=%s xauUsd=%s xagUsd=%s src=%s', usdInr, xauUsd, xagUsd, src);
}

function getSpotDerived() {
  const { usdInr, xauUsd, xagUsd, src } = forexCache;
  const FACTOR = 1.103; // duties/charges factor
  const goldPer10g = Math.round((xauUsd / 31.1035) * 10 * usdInr * FACTOR);
  const silverPerKg = Math.round((xagUsd / 31.1035) * 1000 * usdInr * FACTOR);
  return { goldPer10g, silverPerKg, usdInr, xauUsd, xagUsd, src };
}

// ─── DHAN TOKEN RENEW ─────────────────────────────────────────────
async function renewDhanToken() {
  if (!currentAccessToken || !DHAN_CLIENT_ID) return false;
  try {
    const r = await axios.post(`${DHAN_API_BASE}/RenewToken`, {}, {
      headers: {
        'access-token': currentAccessToken,
        'dhanClientId': DHAN_CLIENT_ID,
        'Content-Type': 'application/json',
      },
      timeout: 12000,
    });
    const t = r.data && (r.data.accessToken || r.data.access_token || (r.data.data && r.data.data.accessToken));
    if (!t) {
      console.warn('[TOKEN] Renew response missing accessToken');
      return false;
    }
    currentAccessToken = t;
    tokenRenewedAt = new Date().toISOString();
    console.log('[TOKEN] Renewed at', tokenRenewedAt);
    if (WS.ws) {
      try { WS.ws.terminate(); } catch (e) { console.warn('[TOKEN] ws.terminate error', e.message); }
    }
    WS.wsStatus = 'disconnected';
    setTimeout(connectDhan, 3000);
    return true;
  } catch (e) {
    console.warn('[TOKEN] Renew failed:', e.response && e.response.data ? JSON.stringify(e.response.data) : e.message);
    return false;
  }
}

// ─── BINARY PACKET PARSER (v2) ────────────────────────────────────
// header = B (feedCode) H (len) B (segment) I (secId)
function parseDhanPacket(buf) {
  try {
    if (!buf || buf.length < 16) return null;

    const feedCode = buf.readUInt8(0);
    const exchSeg  = buf.readUInt8(3);
    const secId    = buf.readInt32LE(4).toString();

    if (feedCode === FEED_CODE.DISCONNECT) {
      const code = buf.length >= 12 ? buf.readInt16LE(8) : null;
      WS.lastDisconnectCode = code;
      console.warn('[WS] Disconnect packet from server, code=', code);
      return null;
    }

    // Ticker: <BHBIfI (16 bytes)
    if (feedCode === FEED_CODE.TICKER && buf.length >= 16) {
      const ltp  = buf.readFloatLE(8);
      const ltt  = buf.readUInt32LE(12);
      if (!Number.isFinite(ltp) || ltp <= 0) return null;
      return {
        type: 'ticker',
        exchSeg,
        secId,
        ltp: Math.round(ltp),
        bid: Math.round(ltp),
        ask: Math.round(ltp),
        high: 0,
        low: 0,
        open: 0,
        ltt,
      };
    }

    // Full: <BHBIfHIfIIIIIIffff100s (>= 80 bytes)
    if (feedCode === FEED_CODE.FULL && buf.length >= 80) {
      const ltp  = buf.readFloatLE(8);
      const ltt  = buf.readUInt32LE(14);
      const atp  = buf.readFloatLE(18);
      const vol  = buf.readUInt32LE(22);
      const dayOpen  = buf.readFloatLE(46);
      const dayClose = buf.readFloatLE(50);
      const dayHigh  = buf.readFloatLE(54);
      const dayLow   = buf.readFloatLE(58);
      const bidPrice = buf.readFloatLE(62 + 12);
      const askPrice = buf.readFloatLE(62 + 16);

      if (!Number.isFinite(ltp) || ltp <= 0) return null;

      return {
        type: 'full',
        exchSeg,
        secId,
        ltp: Math.round(ltp),
        bid: Number.isFinite(bidPrice) && bidPrice > 0 ? Math.round(bidPrice) : Math.round(ltp),
        ask: Number.isFinite(askPrice) && askPrice > 0 ? Math.round(askPrice) : Math.round(ltp),
        high: Number.isFinite(dayHigh) && dayHigh > 0 ? Math.round(dayHigh) : 0,
        low:  Number.isFinite(dayLow)  && dayLow  > 0 ? Math.round(dayLow)  : 0,
        open: Number.isFinite(dayOpen) && dayOpen > 0 ? Math.round(dayOpen) : 0,
        dayClose: Number.isFinite(dayClose) ? Math.round(dayClose) : 0,
        atp,
        vol,
        ltt,
      };
    }

    // Quote: <BHBIfHIfIIIffff (>= 50 bytes)
    if (feedCode === FEED_CODE.QUOTE && buf.length >= 50) {
      const ltp  = buf.readFloatLE(8);
      const ltt  = buf.readUInt32LE(14);
      const atp  = buf.readFloatLE(18);
      const vol  = buf.readUInt32LE(22);
      const dayOpen  = buf.readFloatLE(35);
      const dayClose = buf.readFloatLE(39);
      const dayHigh  = buf.readFloatLE(43);
      const dayLow   = buf.readFloatLE(47);

      if (!Number.isFinite(ltp) || ltp <= 0) return null;

      return {
        type: 'quote',
        exchSeg,
        secId,
        ltp: Math.round(ltp),
        bid: Math.round(ltp),
        ask: Math.round(ltp),
        high: Number.isFinite(dayHigh) && dayHigh > 0 ? Math.round(dayHigh) : 0,
        low:  Number.isFinite(dayLow)  && dayLow  > 0 ? Math.round(dayLow)  : 0,
        open: Number.isFinite(dayOpen) && dayOpen > 0 ? Math.round(dayOpen) : 0,
        dayClose: Number.isFinite(dayClose) ? Math.round(dayClose) : 0,
        atp,
        vol,
        ltt,
      };
    }

    return null;
  } catch (e) {
    console.warn('[PARSE] Failed:', e.message);
    return null;
  }
}

// ─── WEBSOCKET ─────────────────────────────────────────────────────
function getDhanWsUrl() {
  return 'wss://api-feed.dhan.co?version=2'
    + '&token='    + encodeURIComponent(currentAccessToken)
    + '&clientId=' + encodeURIComponent(DHAN_CLIENT_ID)
    + '&authType=2';
}

function connectDhan() {
  if (!DHAN_CLIENT_ID || !currentAccessToken) {
    console.warn('[WS] Missing DHAN_CLIENT_ID or access token');
    return;
  }
  if (WS.wsStatus === 'connecting' || WS.wsStatus === 'connected') return;

  WS.wsStatus = 'connecting';
  WS.lastConnectAt = new Date().toISOString();
  WS.lastDisconnectCode = null;
  console.log('[WS] Connecting to Dhan feed...');

  const ws = new WebSocket(getDhanWsUrl(), { handshakeTimeout: 15000 });
  WS.ws = ws;

  ws.on('open', () => {
    WS.wsStatus = 'connected';
    WS.reconnectCount = 0;
    console.log('[WS] Connected, subscribing instruments');

    const instruments = [
      { ExchangeSegment: EXCH_SEG.MCX_COMM, SecurityId: TOKENS.goldCurrent.secId },
      { ExchangeSegment: EXCH_SEG.MCX_COMM, SecurityId: TOKENS.goldNext.secId },
      { ExchangeSegment: EXCH_SEG.MCX_COMM, SecurityId: TOKENS.silverCurrent.secId },
      { ExchangeSegment: EXCH_SEG.MCX_COMM, SecurityId: TOKENS.silverNext.secId },
    ];

    const payload15 = { RequestCode: 15, InstrumentCount: instruments.length, InstrumentList: instruments };
    ws.send(JSON.stringify(payload15));
    console.log('[WS] Sent RequestCode 15 (Ticker)');

    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      const payload17 = { RequestCode: 17, InstrumentCount: instruments.length, InstrumentList: instruments };
      ws.send(JSON.stringify(payload17));
      console.log('[WS] Sent RequestCode 17 (Quote)');
    }, 300);

    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      const payload21 = { RequestCode: 21, InstrumentCount: instruments.length, InstrumentList: instruments };
      ws.send(JSON.stringify(payload21));
      console.log('[WS] Sent RequestCode 21 (Full)');
    }, 600);
  });

  ws.on('message', (data) => {
    if (typeof data === 'string') {
      WS.lastTextMsg = data.toString().slice(0, 500);
      console.log('[WS] Text message:', WS.lastTextMsg);
      return;
    }

    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (!WS.lastRawBufHex) {
      WS.lastRawBufHex = buf.slice(0, Math.min(buf.length, 32)).toString('hex');
      console.log('[WS] First binary packet hex=%s len=%d feedCode=%d', WS.lastRawBufHex, buf.length, buf.readUInt8(0));
    }

    const tick = parseDhanPacket(buf);
    if (!tick || !tick.ltp) return;

    WS.lastTickAt = Date.now();

    const { secId } = tick;

    if (secId === TOKENS.goldCurrent.secId) {
      liveTick.gold = { ...liveTick.gold, ...tick, ts: WS.lastTickAt };
      updateSessionHL('gold', tick.ltp, tick.high, tick.low);
    } else if (secId === TOKENS.goldNext.secId) {
      liveTick.goldNext = { ...liveTick.goldNext, ...tick, ts: WS.lastTickAt };
    } else if (secId === TOKENS.silverCurrent.secId) {
      liveTick.silver = { ...liveTick.silver, ...tick, ts: WS.lastTickAt };
      updateSessionHL('silver', tick.ltp, tick.high, tick.low);
    } else if (secId === TOKENS.silverNext.secId) {
      liveTick.silverNext = { ...liveTick.silverNext, ...tick, ts: WS.lastTickAt };
    } else {
      console.log('[WS] Tick for unknown secId=%s ltp=%d', secId, tick.ltp);
    }
  });

  ws.on('close', (code, reason) => {
    WS.wsStatus = 'disconnected';
    WS.lastDisconnectAt = new Date().toISOString();
    console.warn('[WS] Closed code=%s reason=%s', code, reason && reason.toString ? reason.toString() : '');

    if (code === 807 || code === 808 || code === 809) {
      renewDhanToken().then(() => scheduleReconnect());
    } else {
      scheduleReconnect();
    }
  });

  ws.on('error', (err) => {
    console.warn('[WS] Error:', err.message);
  });
}

function scheduleReconnect() {
  if (WS.reconnectTimer) return;
  WS.reconnectCount += 1;
  const delay = Math.min(2000 * Math.pow(2, Math.min(WS.reconnectCount, 5)), 60000);
  console.log('[WS] Reconnect scheduled in', delay / 1000, 'seconds');
  WS.reconnectTimer = setTimeout(() => {
    WS.reconnectTimer = null;
    connectDhan();
  }, delay);
}

// ─── ROUTES ────────────────────────────────────────────────────────
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  const nowIso = new Date().toISOString();
  const { usdInr, xauUsd, xagUsd } = forexCache;

  if (isDhanLive()) {
    const g  = liveTick.gold;
    const s  = liveTick.silver;
    const gN = liveTick.goldNext;
    const sN = liveTick.silverNext;

    const gHigh = sessionHL.gold.high || g.high;
    const gLow  = sessionHL.gold.low === Infinity ? (g.low || g.ltp) : sessionHL.gold.low;
    const sHigh = sessionHL.silver.high || s.high;
    const sLow  = sessionHL.silver.low === Infinity ? (s.low || s.ltp) : sessionHL.silver.low;

    const payload = {
      success: true,
      source: 'dhan_mcx_live',
      marketOpen,
      tickAgeMs: Date.now() - WS.lastTickAt,
      tickAgeSeconds: tickAgeSeconds(),
      goldPer10g: g.ltp,
      silverPerKg: s.ltp,
      futures: {
        gold:       { ltp: g.ltp, bid: g.bid, ask: g.ask, high: gHigh, low: gLow, open: g.open },
        silver:     { ltp: s.ltp, bid: s.bid, ask: s.ask, high: sHigh, low: sLow, open: s.open },
        goldNext:   { ltp: gN.ltp || g.ltp, bid: gN.bid || g.bid, ask: gN.ask || g.ask },
        silverNext: { ltp: sN.ltp || s.ltp, bid: sN.bid || s.bid, ask: sN.ask || s.ask },
      },
      usdInr,
      xauUsd,
      xagUsd,
      forexUpdatedAt: forexCache.updatedAt,
      timestamp: nowIso,
    };

    lastKnownRates = { ...payload };
    return res.json(payload);
  }

  if (isDhanStale() || lastKnownRates) {
    return res.json({
      ...(lastKnownRates || {}),
      success: true,
      source: 'last_known_rates',
      marketOpen,
      tickAgeSeconds: tickAgeSeconds() === Infinity ? null : tickAgeSeconds(),
      priceAsOf: WS.lastTickAt ? new Date(WS.lastTickAt).toISOString() : null,
      usdInr,
      xauUsd,
      xagUsd,
      forexUpdatedAt: forexCache.updatedAt,
      timestamp: nowIso,
    });
  }

  const spot = getSpotDerived();
  return res.json({
    success: true,
    source: 'spot_derived',
    marketOpen,
    note: 'Live MCX feed unavailable, using spot-derived estimate',
    spotSource: spot.src,
    usdInr: spot.usdInr,
    xauUsd: spot.xauUsd,
    xagUsd: spot.xagUsd,
    forexUpdatedAt: forexCache.updatedAt,
    goldPer10g: spot.goldPer10g,
    silverPerKg: spot.silverPerKg,
    futures: {
      gold:       { ltp: spot.goldPer10g, bid: spot.goldPer10g, ask: spot.goldPer10g },
      silver:     { ltp: spot.silverPerKg, bid: spot.silverPerKg, ask: spot.silverPerKg },
      goldNext:   { ltp: null, bid: null, ask: null },
      silverNext: { ltp: null, bid: null, ask: null },
    },
    timestamp: nowIso,
  });
});

app.get('/debug', (req, res) => {
  res.json({
    server: 'RR Jewellers full v2',
    wsStatus: WS.wsStatus,
    lastTickAt: WS.lastTickAt ? new Date(WS.lastTickAt).toISOString() : null,
    tickAgeSeconds: tickAgeSeconds() === Infinity ? null : tickAgeSeconds(),
    reconnectCount: WS.reconnectCount,
    lastConnectAt: WS.lastConnectAt,
    lastDisconnectAt: WS.lastDisconnectAt,
    lastDisconnectCode: WS.lastDisconnectCode,
    currentSource: isDhanLive()
      ? 'dhan_mcx_live'
      : (isDhanStale() || lastKnownRates ? 'last_known_rates' : 'spot_derived'),
    marketOpen: isMCXOpen(),
    tokenRenewedAt,
    sessionHL,
    liveTick,
    tokens: TOKENS,
    forexCache,
    lastRawBufHex: WS.lastRawBufHex,
    lastTextMsg: WS.lastTextMsg,
    lastKnownRatesAt: lastKnownRates && lastKnownRates.timestamp ? lastKnownRates.timestamp : null,
    credentials: { clientId: !!DHAN_CLIENT_ID, accessToken: !!currentAccessToken },
    env: { SHEET_ID: !!SHEET_ID, SELF_URL: SELF_URL || null },
  });
});

app.get('/ping', (req, res) => {
  res.json({ ok: true, ts: Date.now() });
});

app.get('/', (req, res) => {
  res.json({
    status: 'RR Jewellers full v2',
    wsStatus: WS.wsStatus,
    endpoints: ['/rates', '/debug', '/ping'],
  });
});

// ─── STARTUP ───────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', async () => {
  console.log('[STARTUP] RR Jewellers full v2 on port', PORT);
  try {
    await refreshForexAndSpot();
  } catch (e) {
    console.warn('[STARTUP] refreshForexAndSpot failed:', e.message);
  }
  connectDhan();

  setInterval(refreshForexAndSpot, 5 * 60 * 1000);
  setInterval(resetSessionIfNewDay, 60 * 1000);
  setInterval(() => {
    const url = SELF_URL || `http://localhost:${PORT}`;
    axios.get(url + '/ping').catch(() => {});
  }, 4 * 60 * 1000);
  setInterval(() => {
    if (WS.wsStatus === 'disconnected' && !WS.reconnectTimer) connectDhan();
  }, 2 * 60 * 1000);
});
