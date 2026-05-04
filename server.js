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
};

// ← IMPORTANT: yahi 4 tokens tumne pehle bhi use kiye the
const TOKENS = {
  goldCurrent:   { secId: '436177', symbol: 'GOLD-JUN2026-MCX-FUT' },
  goldNext:      { secId: '436178', symbol: 'GOLD-AUG2026-MCX-FUT' },
  silverCurrent: { secId: '436197', symbol: 'SILVER-JUL2026-MCX-FUT' },
  silverNext:    { secId: '436198', symbol: 'SILVER-SEP2026-MCX-FUT' },
};

let lastKnownRates = null;
const forexCache = { usdInr: 94.5, xauUsd: 3310, xagUsd: 32.8, updatedAt: null, src: 'init' };

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
  if (dow === 0) return false;
  const t = hour * 60 + min;
  return dow === 6 ? (t >= 540 && t < 840) : (t >= 540 && t < 1435);
}

function resetSessionIfNewDay() {
  const k = istDayKey();
  if (sessionDayKey !== k) {
    sessionDayKey = k;
    sessionHL.gold   = { high: 0, low: Infinity };
    sessionHL.silver = { high: 0, low: Infinity };
    console.log('[SESSION] reset', k);
  }
}

function updateSessionHL(sym, ltp, high, low) {
  resetSessionIfNewDay();
  if (ltp > 0) {
    if (ltp > sessionHL[sym].high) sessionHL[sym].high = ltp;
    if (ltp < sessionHL[sym].low)  sessionHL[sym].low  = ltp;
  }
  if (high > 0 && high > sessionHL[sym].high) sessionHL[sym].high = high;
  if (low  > 0 && low  < sessionHL[sym].low)  sessionHL[sym].low  = low;
}

function tickAgeSeconds() {
  if (!WS.lastTickAt) return Infinity;
  return Math.floor((Date.now() - WS.lastTickAt) / 1000);
}

function isDhanLive() {
  return WS.wsStatus === 'connected' && tickAgeSeconds() < 10 && liveTick.gold.ltp > 0;
}

function isDhanStale() {
  const a = tickAgeSeconds();
  return liveTick.gold.ltp > 0 && a >= 10 && a < 300;
}

// ─── FOREX + SPOT ──────────────────────────────────────────────────
async function refreshForexAndSpot() {
  let usdInr = 0, xauUsd = 0, xagUsd = 0, src = '';

  const fxTry = [
    ['frankfurter', () =>
      axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 5000 })
           .then(r => r.data?.rates?.INR)],
    ['open.er-api', () =>
      axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 5000 })
           .then(r => r.data?.rates?.INR)],
    ['fawazahmed0', () =>
      axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json', { timeout: 5000 })
           .then(r => r.data?.usd?.inr)],
  ];

  for (const [name, fn] of fxTry) {
    if (usdInr) break;
    try {
      const v = await fn();
      if (v > 70 && v < 110) {
        usdInr = v;
        src = name;
      }
    } catch {}
  }

  if (!usdInr) {
    usdInr = forexCache.usdInr;
    src = 'cached';
  }

  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout: 6000 });
    if (Array.isArray(r.data)) {
      const g = r.data.find(x => x.gold)?.gold;
      const s = r.data.find(x => x.silver)?.silver;
      if (g > 3000 && g < 9000 && s > 20 && s < 300) {
        xauUsd = g;
        xagUsd = s;
      }
    }
  } catch {}

  if (!xauUsd) {
    try {
      const [gr, sr] = await Promise.all([
        axios.get('https://www.gold-api.com/price/XAU', { timeout: 7000 }),
        axios.get('https://www.gold-api.com/price/XAG', { timeout: 7000 }),
      ]);
      const g = gr.data?.price;
      const s = sr.data?.price;
      if (g > 3000 && g < 9000 && s > 20 && s < 300) {
        xauUsd = g;
        xagUsd = s;
      }
    } catch {}
  }

  if (!xauUsd) {
    xauUsd = forexCache.xauUsd;
    xagUsd = forexCache.xagUsd;
  }

  forexCache.usdInr = usdInr;
  forexCache.xauUsd = xauUsd;
  forexCache.xagUsd = xagUsd;
  forexCache.updatedAt = new Date().toISOString();
  forexCache.src = src;

  console.log('[FOREX]', usdInr, xauUsd, xagUsd, src);
}

function getSpotDerived() {
  const { usdInr, xauUsd, xagUsd, src } = forexCache;
  const F = 1.103;
  return {
    gLtp: Math.round(xauUsd / 31.1035 * 10   * usdInr * F),
    sLtp: Math.round(xagUsd / 31.1035 * 1000 * usdInr * F),
    usdInr, xauUsd, xagUsd, src,
  };
}

// ─── DHAN TOKEN RENEW (simple) ─────────────────────────────────────
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
    const t = r.data?.accessToken || r.data?.access_token || r.data?.data?.accessToken;
    if (!t) return false;
    currentAccessToken = t;
    tokenRenewedAt = new Date().toISOString();
    if (WS.ws) try { WS.ws.terminate(); } catch {}
    WS.wsStatus = 'disconnected';
    setTimeout(connectDhan, 3000);
    console.log('[TOKEN] renewed');
    return true;
  } catch (e) {
    console.warn('[TOKEN] renew failed', e.message);
    return false;
  }
}

// ─── BINARY PACKET PARSER (v2) ─────────────────────────────────────
// Header: feedCode(0) | msgLen(1-2) | exchSeg(3) | secId(4-7)[web:4]
function parseDhanPacket(buf) {
  try {
    if (!buf || buf.length < 12) return null;
    const feedCode = buf.readUInt8(0);
    const secId    = buf.readInt32LE(4).toString();

    if (feedCode === FEED_CODE.TICKER && buf.length >= 16) {
      const ltp = buf.readFloatLE(8) / 100;
      if (ltp < 100 || ltp > 9999999) return null;
      return {
        secId,
        ltp: Math.round(ltp),
        bid: Math.round(ltp),
        ask: Math.round(ltp),
        high: 0, low: 0, open: 0,
      };
    }

    if (feedCode === FEED_CODE.FULL && buf.length >= 80) {
      const ltp  = buf.readFloatLE(8)  / 100;
      if (ltp < 100 || ltp > 9999999) return null;
      const open = buf.readFloatLE(46) / 100;
      const high = buf.readFloatLE(54) / 100;
      const low  = buf.readFloatLE(58) / 100;
      let bid = Math.round(ltp), ask = Math.round(ltp);
      if (buf.length >= 82) {
        const b = buf.readFloatLE(62 + 12) / 100;
        const a = buf.readFloatLE(62 + 16) / 100;
        if (b > 100) bid = Math.round(b);
        if (a > 100) ask = Math.round(a);
      }
      return {
        secId,
        ltp: Math.round(ltp),
        bid, ask,
        high: Math.round(high) || 0,
        low:  Math.round(low)  || 0,
        open: Math.round(open) || 0,
      };
    }

    if (feedCode === FEED_CODE.DISCONNECT && buf.length >= 10) {
      console.warn('[WS] server disconnect code', buf.readInt16LE(8));
      return null;
    }

    return null;
  } catch (e) {
    console.warn('[PARSE]', e.message);
    return null;
  }
}

// ─── WEBSOCKET ─────────────────────────────────────────────────────
function getDhanWsUrl() {
  return 'wss://api-feed.dhan.co?version=2'
    + '&token='   + encodeURIComponent(currentAccessToken)
    + '&clientId=' + encodeURIComponent(DHAN_CLIENT_ID)
    + '&authType=2';
}

function connectDhan() {
  if (!DHAN_CLIENT_ID || !currentAccessToken) {
    console.warn('[WS] missing creds');
    return;
  }
  if (WS.wsStatus === 'connecting' || WS.wsStatus === 'connected') return;

  WS.wsStatus = 'connecting';
  WS.lastConnectAt = new Date().toISOString();
  console.log('[WS] connecting...');

  const ws = new WebSocket(getDhanWsUrl(), { handshakeTimeout: 15000 });
  WS.ws = ws;

  ws.on('open', () => {
    WS.wsStatus = 'connected';
    WS.reconnectCount = 0;
    console.log('[WS] connected, subscribing...');

    const instruments = [
      { ExchangeSegment: EXCH_SEG.MCX_COMM, SecurityId: TOKENS.goldCurrent.secId },
      { ExchangeSegment: EXCH_SEG.MCX_COMM, SecurityId: TOKENS.goldNext.secId },
      { ExchangeSegment: EXCH_SEG.MCX_COMM, SecurityId: TOKENS.silverCurrent.secId },
      { ExchangeSegment: EXCH_SEG.MCX_COMM, SecurityId: TOKENS.silverNext.secId },
    ];

    ws.send(JSON.stringify({
      RequestCode: 21,           // FULL feed as per v2 docs[web:4]
      InstrumentCount: instruments.length,
      InstrumentList: instruments,
    }));
    console.log('[WS] sent RequestCode 21 for', instruments.length, 'instruments');
  });

  ws.on('message', (data) => {
    if (typeof data === 'string') {
      WS.lastTextMsg = data.toString().slice(0, 500);
      console.log('[WS] text from Dhan:', WS.lastTextMsg);
      return;
    }

    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (!WS.lastRawBufHex) {
      WS.lastRawBufHex = buf.slice(0, Math.min(buf.length, 32)).toString('hex');
      console.log('[WS] first binary hex:', WS.lastRawBufHex, 'len:', buf.length, 'feedCode:', buf[0]);
    }

    const tick = parseDhanPacket(buf);
    if (!tick || tick.ltp <= 0) return;

    WS.lastTickAt = Date.now();
    const sid = tick.secId;

    if (sid === TOKENS.goldCurrent.secId) {
      liveTick.gold = { ...tick, ts: WS.lastTickAt };
      updateSessionHL('gold', tick.ltp, tick.high, tick.low);
    } else if (sid === TOKENS.goldNext.secId) {
      liveTick.goldNext = { ...tick, ts: WS.lastTickAt };
    } else if (sid === TOKENS.silverCurrent.secId) {
      liveTick.silver = { ...tick, ts: WS.lastTickAt };
      updateSessionHL('silver', tick.ltp, tick.high, tick.low);
    } else if (sid === TOKENS.silverNext.secId) {
      liveTick.silverNext = { ...tick, ts: WS.lastTickAt };
    } else {
      console.log('[WS] tick unknown secId', sid, 'ltp', tick.ltp);
    }
  });

  ws.on('close', (code, reason) => {
    WS.wsStatus = 'disconnected';
    WS.lastDisconnectAt = new Date().toISOString();
    console.log('[WS] closed code:', code, reason?.toString()?.slice(0, 100) || '');
    if (code === 807 || code === 808 || code === 809) {
      renewDhanToken().then(() => scheduleReconnect());
    } else {
      scheduleReconnect();
    }
  });

  ws.on('error', (e) => console.warn('[WS] error:', e.message));
}

function scheduleReconnect() {
  if (WS.reconnectTimer) return;
  WS.reconnectCount++;
  const d = Math.min(2000 * Math.pow(2, Math.min(WS.reconnectCount, 5)), 60000);
  console.log('[WS] reconnect in', d / 1000, 's');
  WS.reconnectTimer = setTimeout(() => {
    WS.reconnectTimer = null;
    connectDhan();
  }, d);
}

// ─── ROUTES ────────────────────────────────────────────────────────
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  const now = new Date().toISOString();
  const { usdInr, xauUsd, xagUsd } = forexCache;

  if (isDhanLive()) {
    const g  = liveTick.gold;
    const s  = liveTick.silver;
    const gN = liveTick.goldNext;
    const sN = liveTick.silverNext;

    const gH = sessionHL.gold.high   || g.high;
    const gL = sessionHL.gold.low === Infinity ? g.low : sessionHL.gold.low;
    const sH = sessionHL.silver.high || s.high;
    const sL = sessionHL.silver.low === Infinity ? s.low : sessionHL.silver.low;

    const payload = {
      success: true,
      source: 'dhan_mcx_live',
      marketOpen,
      tickAgeMs: Date.now() - WS.lastTickAt,
      tickAgeSeconds: tickAgeSeconds(),
      goldPer10g: g.ltp,
      silverPerKg: s.ltp,
      futures: {
        gold:       { ltp: g.ltp, bid: g.bid, ask: g.ask, high: gH, low: gL, open: g.open },
        silver:     { ltp: s.ltp, bid: s.bid, ask: s.ask, high: sH, low: sL, open: s.open },
        goldNext:   { ltp: gN.ltp || g.ltp, bid: gN.bid || g.bid, ask: gN.ask || g.ask },
        silverNext: { ltp: sN.ltp || s.ltp, bid: sN.bid || s.bid, ask: sN.ask || s.ask },
      },
      usdInr, xauUsd, xagUsd,
      forexUpdatedAt: forexCache.updatedAt,
      timestamp: now,
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
      usdInr, xauUsd, xagUsd,
      forexUpdatedAt: forexCache.updatedAt,
      timestamp: now,
    });
  }

  const d = getSpotDerived();
  return res.json({
    success: true,
    source: 'spot_derived',
    marketOpen,
    note: 'Live MCX unavailable',
    spotSource: d.src,
    usdInr: d.usdInr,
    xauUsd: d.xauUsd,
    xagUsd: d.xagUsd,
    forexUpdatedAt: forexCache.updatedAt,
    goldPer10g: d.gLtp,
    silverPerKg: d.sLtp,
    futures: {
      gold:       { ltp: d.gLtp, bid: d.gLtp, ask: d.gLtp },
      silver:     { ltp: d.sLtp, bid: d.sLtp, ask: d.sLtp },
      goldNext:   { ltp: null, bid: null, ask: null },
      silverNext: { ltp: null, bid: null, ask: null },
    },
    timestamp: now,
  });
});

app.get('/debug', (req, res) => {
  res.json({
    server: 'RR Jewellers minimal v1',
    wsStatus: WS.wsStatus,
    lastTickAt: WS.lastTickAt ? new Date(WS.lastTickAt).toISOString() : null,
    tickAgeSeconds: tickAgeSeconds() === Infinity ? null : tickAgeSeconds(),
    reconnectCount: WS.reconnectCount,
    lastConnectAt: WS.lastConnectAt,
    lastDisconnectAt: WS.lastDisconnectAt,
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
    lastKnownRatesAt: lastKnownRates?.timestamp || null,
    credentials: { clientId: !!DHAN_CLIENT_ID, accessToken: !!currentAccessToken },
  });
});

app.get('/ping', (req, res) => res.json({ ok: true, ts: Date.now() }));

app.get('/', (req, res) => {
  res.json({
    status: 'RR Jewellers minimal v1',
    wsStatus: WS.wsStatus,
    endpoints: ['/rates', '/debug', '/ping'],
  });
});

// ─── STARTUP ───────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', async () => {
  console.log('[STARTUP] RR Jewellers minimal server, port', PORT);
  await refreshForexAndSpot();
  connectDhan();
  setInterval(refreshForexAndSpot, 5 * 60 * 1000);
  setInterval(resetSessionIfNewDay, 60 * 1000);
  setInterval(() => {
    axios.get((SELF_URL || `http://localhost:${PORT}`) + '/ping').catch(() => {});
  }, 4 * 60 * 1000);
  setInterval(() => {
    if (WS.wsStatus === 'disconnected' && !WS.reconnectTimer) connectDhan();
  }, 2 * 60 * 1000);
});
