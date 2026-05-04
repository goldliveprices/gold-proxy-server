# Build the complete hybrid server.js:
# - V2 WebSocket (primary, real-time ticks in memory)  
# - Dhan OHLC REST poller (backup, every 1s when market open)
# - In-memory rate cache (responds to /rates in <1ms)
# - Auto-rollover contracts
# - Spot derived fallback (metals.live / gold-api)
# - Self-ping, forex refresh
# Write file, then we share it

server_code = r"""'use strict';

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

// ─── CONFIG (change these env vars in Render) ────────────────────
const PORT              = process.env.PORT              || 3000;
const SELF_URL          = process.env.SELF_URL          || '';
const DHAN_CLIENT_ID    = process.env.DHAN_CLIENT_ID    || '';
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN || '';

const DHAN_BASE = 'https://api.dhan.co/v2';

// ─── MCX CONTRACTS (auto-rollover by expiry date) ────────────────
// Confirmed SecurityIds from Dhan scrip master (May 2026)
const GOLD_CONTRACTS = [
  { secId: '459277', display: 'GOLD JUN FUT',  expiry: '2026-06-05' },
  { secId: '466583', display: 'GOLD AUG FUT',  expiry: '2026-08-05' },
  { secId: '483079', display: 'GOLD OCT FUT',  expiry: '2026-10-05' },
  { secId: '495213', display: 'GOLD DEC FUT',  expiry: '2026-12-04' },
  { secId: '559933', display: 'GOLD FEB FUT',  expiry: '2027-02-05' },
];
const SILVER_CONTRACTS = [
  { secId: '457532', display: 'SILVER MAY FUT', expiry: '2026-05-05' },
  { secId: '464150', display: 'SILVER JUL FUT', expiry: '2026-07-03' },
  { secId: '471725', display: 'SILVER SEP FUT', expiry: '2026-09-04' },
  { secId: '495214', display: 'SILVER DEC FUT', expiry: '2026-12-04' },
  { secId: '564619', display: 'SILVER MAR FUT', expiry: '2027-03-05' },
];

function pickCurrentAndNext(contracts) {
  const now = new Date();
  const sorted = contracts
    .map(c => ({ ...c, expiryDate: new Date(c.expiry) }))
    .filter(c => !isNaN(c.expiryDate))
    .sort((a, b) => a.expiryDate - b.expiryDate);
  const upcoming = sorted.filter(c => c.expiryDate >= now);
  if (upcoming.length >= 2) return { current: upcoming[0], next: upcoming[1] };
  if (upcoming.length === 1) return { current: sorted[sorted.length - 2] || upcoming[0], next: upcoming[0] };
  const last = sorted.slice(-2);
  return { current: last[0] || sorted[0], next: last[1] || sorted[0] };
}

function getActiveContracts() {
  return {
    gold:   pickCurrentAndNext(GOLD_CONTRACTS),
    silver: pickCurrentAndNext(SILVER_CONTRACTS),
  };
}

// ─── IN-MEMORY RATE CACHE ────────────────────────────────────────
// Sheet /rates pe aayi to ye cache serve hoga — network latency nahi
const rateCache = {
  goldLtp:      0,
  silverLtp:    0,
  goldHigh:     0, goldLow:    0, goldOpen:    0, goldPrevClose:  0,
  silverHigh:   0, silverLow:  0, silverOpen:  0, silverPrevClose:0,
  goldNextLtp:  0, silverNextLtp: 0,
  source:       'init',
  updatedAt:    null,
  tickAgeMs:    null,
};

function updateCacheFromTick(key, tick) {
  if (!tick || !tick.ltp || tick.ltp <= 0) return;
  if (key === 'gold') {
    rateCache.goldLtp      = tick.ltp;
    if (tick.high > 0)      rateCache.goldHigh      = tick.high;
    if (tick.low  > 0)      rateCache.goldLow       = tick.low;
    if (tick.open > 0)      rateCache.goldOpen      = tick.open;
    if (tick.prevClose > 0) rateCache.goldPrevClose = tick.prevClose;
  } else if (key === 'goldNext') {
    rateCache.goldNextLtp  = tick.ltp;
  } else if (key === 'silver') {
    rateCache.silverLtp      = tick.ltp;
    if (tick.high > 0)        rateCache.silverHigh      = tick.high;
    if (tick.low  > 0)        rateCache.silverLow       = tick.low;
    if (tick.open > 0)        rateCache.silverOpen      = tick.open;
    if (tick.prevClose > 0)   rateCache.silverPrevClose = tick.prevClose;
  } else if (key === 'silverNext') {
    rateCache.silverNextLtp = tick.ltp;
  }
  rateCache.updatedAt  = new Date().toISOString();
  rateCache.tickAgeMs  = 0;
}

// ─── FOREX + SPOT FALLBACK ────────────────────────────────────────
const forexCache = { usdInr: 94.5, xauUsd: 3310, xagUsd: 32.8, updatedAt: null, src: 'init' };

async function refreshForex() {
  let usdInr = 0, src = '';
  const fxSrc = [
    ['frankfurter', async () => (await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 5000 })).data.rates.INR],
    ['open.er-api', async () => (await axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 5000 })).data.rates.INR],
  ];
  for (const [name, fn] of fxSrc) {
    if (usdInr) break;
    try { const v = await fn(); if (v > 70 && v < 110) { usdInr = v; src = name; } } catch (e) { console.warn('[FOREX]', name, e.message); }
  }
  let xauUsd = 0, xagUsd = 0;
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout: 6000 });
    if (Array.isArray(r.data)) {
      const g = r.data.find(x => x.gold), s = r.data.find(x => x.silver);
      if (g && g.gold > 3000) xauUsd = g.gold;
      if (s && s.silver > 20) xagUsd = s.silver;
    }
  } catch (e) { console.warn('[SPOT] metals.live:', e.message); }
  if (!xauUsd) {
    try {
      const [g, s] = await Promise.all([
        axios.get('https://www.gold-api.com/price/XAU', { timeout: 7000 }),
        axios.get('https://www.gold-api.com/price/XAG', { timeout: 7000 }),
      ]);
      if (g.data.price > 3000) xauUsd = g.data.price;
      if (s.data.price > 20)   xagUsd = s.data.price;
    } catch (e) { console.warn('[SPOT] gold-api:', e.message); }
  }
  forexCache.usdInr    = usdInr    || forexCache.usdInr;
  forexCache.xauUsd    = xauUsd    || forexCache.xauUsd;
  forexCache.xagUsd    = xagUsd    || forexCache.xagUsd;
  forexCache.updatedAt = new Date().toISOString();
  forexCache.src       = src || forexCache.src;
  console.log('[FOREX] usdInr=%s xauUsd=%s src=%s', forexCache.usdInr, forexCache.xauUsd, forexCache.src);
}

function getSpotDerived() {
  const { usdInr, xauUsd, xagUsd } = forexCache, F = 1.103;
  return {
    goldPer10g:  Math.round((xauUsd / 31.1035) * 10   * usdInr * F),
    silverPerKg: Math.round((xagUsd / 31.1035) * 1000 * usdInr * F),
  };
}

// ─── MCX MARKET HOURS ────────────────────────────────────────────
function isMCXOpen() {
  const d = new Date(Date.now() + 5.5 * 3600000);
  const dow = d.getUTCDay(), t = d.getUTCHours() * 60 + d.getUTCMinutes();
  if (dow === 0) return false;
  return dow === 6 ? (t >= 540 && t < 840) : (t >= 540 && t < 1435);
}

// ─── WEBSOCKET (PRIMARY — real-time ticks) ────────────────────────
// V2 URL with query-param auth (docs: version=2, authType=2)
const WS_URL = () =>
  `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(DHAN_ACCESS_TOKEN)}&clientId=${encodeURIComponent(DHAN_CLIENT_ID)}&authType=2`;

const WS = {
  ws: null, status: 'disconnected',
  reconnectTimer: null, reconnectCount: 0,
  lastConnectAt: null, lastDisconnectAt: null,
  lastTickAt: null, lastDisconnectCode: null,
  packetsReceived: 0, lastRawHex: '', lastTextMsg: '',
};

// SecurityId → rateCache key map (rebuilt from active contracts)
let TOKEN_MAP = {};

function buildTokenMap() {
  const ac = getActiveContracts();
  TOKEN_MAP = {
    [ac.gold.current.secId]:   'gold',
    [ac.gold.next.secId]:      'goldNext',
    [ac.silver.current.secId]: 'silver',
    [ac.silver.next.secId]:    'silverNext',
  };
}

function subscribeWS(ws) {
  const ac = getActiveContracts();
  const instruments = [
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.gold.current.secId },
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.gold.next.secId },
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.silver.current.secId },
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.silver.next.secId },
  ];
  // Also test with NSE as fallback test instrument
  instruments.push({ ExchangeSegment: 'NSE_EQ', SecurityId: '1333' });

  [[15, 0], [17, 600], [21, 1200]].forEach(([code, delay]) => {
    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      const payload = { RequestCode: code, InstrumentCount: instruments.length, InstrumentList: instruments };
      ws.send(JSON.stringify(payload));
      console.log('[WS] Subscribe RequestCode=%d sent', code);
    }, delay);
  });
}

function parseBuf(buf) {
  try {
    if (!buf || buf.length < 8) return null;
    const fc    = buf.readUInt8(0);
    const secId = buf.readInt32LE(4).toString();

    if (fc === 50) {
      WS.lastDisconnectCode = buf.length >= 10 ? buf.readInt16LE(8) : 0;
      console.warn('[WS] Server disconnect code=', WS.lastDisconnectCode);
      return null;
    }
    // Previous close packet (fc=6)
    if (fc === 6 && buf.length >= 16) {
      return { type: 'prevClose', secId, prevClose: Math.round(buf.readFloatLE(8)) };
    }
    // Ticker (fc=2)
    if (fc === 2 && buf.length >= 16) {
      const ltp = buf.readFloatLE(8);
      if (!Number.isFinite(ltp) || ltp <= 0) return null;
      return { type: 'ticker', secId, ltp: Math.round(ltp) };
    }
    // Quote (fc=4): has ohlc fields
    if (fc === 4 && buf.length >= 50) {
      const ltp = buf.readFloatLE(8);
      if (!Number.isFinite(ltp) || ltp <= 0) return null;
      return {
        type:  'quote', secId,
        ltp:   Math.round(ltp),
        open:  Math.round(buf.readFloatLE(34)),
        high:  Math.round(buf.readFloatLE(42)),
        low:   Math.round(buf.readFloatLE(46)),
      };
    }
    if (WS.packetsReceived <= 20)
      console.log('[WS] Unknown fc=%d len=%d hex=%s', fc, buf.length, buf.slice(0, 16).toString('hex'));
    return null;
  } catch (e) { console.warn('[PARSE]', e.message); return null; }
}

function connectDhan() {
  if (!DHAN_CLIENT_ID || !DHAN_ACCESS_TOKEN) { console.warn('[WS] No credentials'); return; }
  if (WS.status === 'connecting' || WS.status === 'connected') return;

  WS.status = 'connecting';
  WS.lastConnectAt = new Date().toISOString();
  WS.packetsReceived = 0; WS.lastRawHex = '';
  buildTokenMap();

  const ws = new WebSocket(WS_URL(), { handshakeTimeout: 15000 });
  WS.ws = ws;

  ws.on('open', () => {
    WS.status = 'connected'; WS.reconnectCount = 0;
    console.log('[WS] Connected token_len=%d', DHAN_ACCESS_TOKEN.length);
    subscribeWS(ws);
  });

  ws.on('message', (data) => {
    if (typeof data === 'string') {
      WS.lastTextMsg = data.slice(0, 500);
      console.log('[WS] Text:', WS.lastTextMsg);
      return;
    }
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    WS.packetsReceived++;
    if (WS.packetsReceived <= 20) {
      WS.lastRawHex = buf.slice(0, 32).toString('hex');
      console.log('[WS] Pkt#%d fc=%d len=%d', WS.packetsReceived, buf.readUInt8(0), buf.length);
    }

    const tick = parseBuf(buf);
    if (!tick) return;

    WS.lastTickAt = Date.now();
    const key = TOKEN_MAP[tick.secId];

    if (!key) {
      if (tick.secId === '1333') console.log('[WS] NSE test tick received ltp=%d', tick.ltp);
      return;
    }

    if (tick.type === 'prevClose') {
      if (key === 'gold')   rateCache.goldPrevClose   = tick.prevClose;
      if (key === 'silver') rateCache.silverPrevClose = tick.prevClose;
      return;
    }

    updateCacheFromTick(key, tick);
    rateCache.source = 'dhan_ws_live';
    console.log('[WS] Tick %s ltp=%d', key, tick.ltp);
  });

  ws.on('close', (code, reason) => {
    WS.status = 'disconnected';
    WS.lastDisconnectAt = new Date().toISOString();
    console.warn('[WS] Closed code=%s reason=%s packets=%d',
      code, reason && reason.toString ? reason.toString() : '', WS.packetsReceived);
    scheduleReconnect();
  });

  ws.on('error', (err) => { console.warn('[WS] Error:', err.message); });
}

function scheduleReconnect() {
  if (WS.reconnectTimer) return;
  WS.reconnectCount++;
  const delay = Math.min(3000 * Math.pow(2, Math.min(WS.reconnectCount, 4)), 30000);
  console.log('[WS] Reconnect in %ds attempt=%d', delay / 1000, WS.reconnectCount);
  WS.reconnectTimer = setTimeout(() => { WS.reconnectTimer = null; connectDhan(); }, delay);
}

// ─── OHLC REST POLLER (SECONDARY — backup + extra freshness) ─────
// Runs every 1s when market open. Feeds rateCache even if WS is down.
let ohlcPollTimer    = null;
let lastOhlcError    = null;
let ohlcCallCount    = 0;

async function pollOhlcOnce() {
  if (!DHAN_CLIENT_ID || !DHAN_ACCESS_TOKEN) return;

  const ac = getActiveContracts();
  const secIds = [
    parseInt(ac.gold.current.secId, 10),
    parseInt(ac.gold.next.secId, 10),
    parseInt(ac.silver.current.secId, 10),
    parseInt(ac.silver.next.secId, 10),
  ];
  const body = { MCX_COMM: secIds };

  try {
    const resp = await axios.post(`${DHAN_BASE}/marketfeed/ohlc`, body, {
      headers: {
        'Accept':       'application/json',
        'Content-Type': 'application/json',
        'access-token': DHAN_ACCESS_TOKEN,
        'client-id':    DHAN_CLIENT_ID,
      },
      timeout: 5000,
    });

    const seg = resp.data && resp.data.data && resp.data.data['MCX_COMM'];
    if (!seg) { lastOhlcError = 'No MCX_COMM in response'; return; }

    ohlcCallCount++;
    lastOhlcError = null;

    function applyOhlc(secId, key) {
      const row = seg[String(secId)];
      if (!row) return;
      const ltp  = row.last_price || 0;
      const ohlc = row.ohlc || {};
      // Only update if WS is NOT live (don't override faster WS ticks)
      if (WS.status === 'connected' && WS.lastTickAt && Date.now() - WS.lastTickAt < 5000) return;
      if (ltp > 0) {
        updateCacheFromTick(key, {
          ltp:   Math.round(ltp),
          open:  ohlc.open  ? Math.round(ohlc.open)  : 0,
          high:  ohlc.high  ? Math.round(ohlc.high)  : 0,
          low:   ohlc.low   ? Math.round(ohlc.low)   : 0,
        });
        rateCache.source = WS.status === 'connected' ? 'dhan_ws_live' : 'dhan_ohlc_rest';
        if (ohlcCallCount <= 3) console.log('[OHLC] %s ltp=%d', key, Math.round(ltp));
      }
    }

    applyOhlc(ac.gold.current.secId,   'gold');
    applyOhlc(ac.gold.next.secId,      'goldNext');
    applyOhlc(ac.silver.current.secId, 'silver');
    applyOhlc(ac.silver.next.secId,    'silverNext');

  } catch (e) {
    const msg = e.response && e.response.data
      ? JSON.stringify(e.response.data)
      : e.message;
    lastOhlcError = msg;
    if (ohlcCallCount < 3) console.warn('[OHLC] Error:', msg);
  }
}

function startOhlcPoller() {
  if (ohlcPollTimer) return;
  ohlcPollTimer = setInterval(() => {
    if (isMCXOpen()) pollOhlcOnce().catch(() => {});
  }, 1000);
  console.log('[OHLC] Poller started (1s interval)');
}

// ─── /rates ──────────────────────────────────────────────────────
// Sheet yahin hit kare — memory se respond karta hai, <1ms
app.get('/rates', (req, res) => {
  const now     = Date.now();
  const market  = isMCXOpen();
  const spot    = getSpotDerived();
  const ac      = getActiveContracts();
  const tickAge = WS.lastTickAt ? now - WS.lastTickAt : null;

  const goldLtp   = rateCache.goldLtp   || spot.goldPer10g;
  const silverLtp = rateCache.silverLtp || spot.silverPerKg;
  const src       = rateCache.goldLtp > 0 ? rateCache.source : 'spot_derived';

  return res.json({
    success: true,
    source: src,
    marketOpen: market,
    goldPer10g:  goldLtp,
    silverPerKg: silverLtp,
    futures: {
      gold: {
        ltp:       rateCache.goldLtp,
        open:      rateCache.goldOpen,
        high:      rateCache.goldHigh,
        low:       rateCache.goldLow,
        prevClose: rateCache.goldPrevClose,
        contract:  ac.gold.current.display,
        expiry:    ac.gold.current.expiry,
      },
      goldNext: {
        ltp:      rateCache.goldNextLtp,
        contract: ac.gold.next.display,
        expiry:   ac.gold.next.expiry,
      },
      silver: {
        ltp:       rateCache.silverLtp,
        open:      rateCache.silverOpen,
        high:      rateCache.silverHigh,
        low:       rateCache.silverLow,
        prevClose: rateCache.silverPrevClose,
        contract:  ac.silver.current.display,
        expiry:    ac.silver.current.expiry,
      },
      silverNext: {
        ltp:      rateCache.silverNextLtp,
        contract: ac.silver.next.display,
        expiry:   ac.silver.next.expiry,
      },
    },
    spotDerived: spot,
    wsStatus: WS.status,
    wsTickAgeMs: tickAge,
    forexUpdatedAt: forexCache.updatedAt,
    usdInr: forexCache.usdInr,
    xauUsd: forexCache.xauUsd,
    xagUsd: forexCache.xagUsd,
    timestamp: new Date().toISOString(),
  });
});

// ─── /debug ──────────────────────────────────────────────────────
app.get('/debug', (req, res) => {
  const ac = getActiveContracts();
  res.json({
    server:  'RR Jewellers Hybrid v7',
    mode:    'WS primary + OHLC REST secondary + spot fallback',
    wsStatus: WS.status,
    wsPacketsReceived: WS.packetsReceived,
    lastRawHex: WS.lastRawHex,
    lastTextMsg: WS.lastTextMsg,
    lastDisconnectCode: WS.lastDisconnectCode,
    reconnectCount: WS.reconnectCount,
    lastConnectAt: WS.lastConnectAt,
    lastDisconnectAt: WS.lastDisconnectAt,
    lastTickAt: WS.lastTickAt ? new Date(WS.lastTickAt).toISOString() : null,
    wsTickAgeMs: WS.lastTickAt ? Date.now() - WS.lastTickAt : null,
    ohlcCallCount, lastOhlcError,
    activeContracts: ac,
    tokenMap: TOKEN_MAP,
    rateCache, forexCache,
    marketOpen: isMCXOpen(),
    env: {
      DHAN_CLIENT_ID: !!DHAN_CLIENT_ID, clientIdLen: DHAN_CLIENT_ID.length,
      DHAN_ACCESS_TOKEN: !!DHAN_ACCESS_TOKEN, tokenLen: DHAN_ACCESS_TOKEN.length,
    },
  });
});

app.get('/ping', (req, res) => res.json({ ok: true, ts: Date.now() }));
app.get('/', (req, res) => res.json({
  status: 'RR Jewellers Hybrid v7',
  endpoints: ['/rates', '/debug', '/ping'],
}));

// ─── STARTUP ─────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', async () => {
  console.log('[STARTUP] RR Jewellers Hybrid v7 port=%s tokenLen=%d', PORT, DHAN_ACCESS_TOKEN.length);

  await refreshForex().catch(e => console.warn('[STARTUP] forex:', e.message));

  // Start WebSocket
  connectDhan();

  // Start OHLC REST poller (1s interval, market hours only)
  startOhlcPoller();

  // Forex refresh every 5 min
  setInterval(refreshForex, 5 * 60 * 1000);

  // WS watchdog: reconnect if dead
  setInterval(() => {
    if (WS.status === 'disconnected' && !WS.reconnectTimer) connectDhan();
  }, 30 * 1000);

  // Self-ping to prevent Render idle
  setInterval(() => {
    axios.get((SELF_URL || `http://localhost:${PORT}`) + '/ping').catch(() => {});
  }, 4 * 60 * 1000);

  // Rebuild token map daily (handles monthly contract rollover)
  setInterval(() => {
    buildTokenMap();
    console.log('[ROLLOVER] Token map rebuilt:', TOKEN_MAP);
  }, 24 * 60 * 60 * 1000);
});
"""

with open('/root/server.js', 'w') as f:
    f.write(server_code)

# Quick syntax check
import subprocess, sys
result = subprocess.run(['node', '--check', '/root/server.js'], capture_output=True, text=True)
print('STDOUT:', result.stdout)
print('STDERR:', result.stderr)
print('Return code:', result.returncode)
