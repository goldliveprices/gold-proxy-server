'use strict';

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

const PORT              = process.env.PORT              || 3000;
const SELF_URL          = process.env.SELF_URL          || '';
const DHAN_CLIENT_ID    = process.env.DHAN_CLIENT_ID    || '';
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN || '';

// ─── V1 BINARY AUTH PROTOCOL ─────────────────────────────────────
// V1: wss://api-feed.dhan.co (no query params)
// Auth = binary packet: 83-byte header + 500-byte token + 2-byte auth type
// Docs: https://dhanhq.co/docs/v1/live-market-feed/

function buildAuthPacket() {
  const buf = Buffer.alloc(585, 0);

  // Byte 0: Feed Request Code = 11 (connect)
  buf.writeUInt8(11, 0);

  // Bytes 1-2: Message Length
  buf.writeInt16LE(585, 1);

  // Bytes 3-32: Client ID (30 bytes, padded with 0)
  const clientIdBuf = Buffer.from(DHAN_CLIENT_ID, 'utf8');
  clientIdBuf.copy(buf, 3, 0, Math.min(clientIdBuf.length, 30));

  // Bytes 33-82: Dhan Auth (50 bytes, all zero — as per docs)
  // Already zero from Buffer.alloc

  // Bytes 83-582: API Access Token (500 bytes)
  const tokenBuf = Buffer.from(DHAN_ACCESS_TOKEN, 'utf8');
  tokenBuf.copy(buf, 83, 0, Math.min(tokenBuf.length, 500));

  // Bytes 583-584: Auth Type = "2P"
  buf.write('2P', 583, 'utf8');

  return buf;
}

// V1 subscribe: binary packet
// ExchangeSegment enums from V1 docs annexure
const EXCH_ENUM = {
  MCX_COMM: 7, // MCX Commodity
  NSE_EQ:   1,
  NSE_FNO:  2,
  BSE_EQ:   3,
};

const FEED_REQ_CODES = {
  TICKER: 11, // already used for auth; subscribe uses different codes
  // For V1: RequestCode in header byte[0] for subscribe:
  // 21=Ticker, 22=Quote, 23=Market Depth, 24=Full
  SUBSCRIBE_TICKER: 21,
  SUBSCRIBE_QUOTE: 22,
  SUBSCRIBE_FULL: 24,
};

// V1 Instrument subscribe binary format:
// Header (83 bytes) + 4 bytes instrument count + (N * 21 bytes)
// Each instrument: 1 byte exchangeSegment + 20 bytes securityId (string, padded)
function buildSubscribePacket(requestCode, instruments) {
  const headerSize = 83;
  const countSize  = 4;
  const instrSize  = 21;
  const total = headerSize + countSize + instruments.length * instrSize;

  const buf = Buffer.alloc(total, 0);

  // Header
  buf.writeUInt8(requestCode, 0);
  buf.writeInt16LE(total, 1);
  const clientIdBuf = Buffer.from(DHAN_CLIENT_ID, 'utf8');
  clientIdBuf.copy(buf, 3, 0, Math.min(clientIdBuf.length, 30));

  // Count
  buf.writeInt32LE(instruments.length, 83);

  // Each instrument
  instruments.forEach((instr, i) => {
    const base = headerSize + countSize + i * instrSize;
    buf.writeUInt8(EXCH_ENUM[instr.segment] || 7, base);
    const secBuf = Buffer.from(String(instr.secId), 'utf8');
    secBuf.copy(buf, base + 1, 0, Math.min(secBuf.length, 20));
  });

  return buf;
}

// ─── TOKENS ───────────────────────────────────────────────────────
const INSTRUMENTS = [
  { secId: '436177', segment: 'MCX_COMM', key: 'gold',       name: 'GOLD-JUN2026' },
  { secId: '436178', segment: 'MCX_COMM', key: 'goldNext',   name: 'GOLD-AUG2026' },
  { secId: '436197', segment: 'MCX_COMM', key: 'silver',     name: 'SILVER-JUL2026' },
  { secId: '436198', segment: 'MCX_COMM', key: 'silverNext', name: 'SILVER-SEP2026' },
];

const TOKEN_MAP = {};
INSTRUMENTS.forEach(i => { TOKEN_MAP[i.secId] = i; });

// ─── LIVE STATE ───────────────────────────────────────────────────
function emptyTick() {
  return { ltp: 0, bid: 0, ask: 0, high: 0, low: 0, open: 0, prevClose: 0, ts: 0 };
}
const liveTick = {
  gold: emptyTick(), goldNext: emptyTick(),
  silver: emptyTick(), silverNext: emptyTick(),
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
  packetsReceived: 0, authed: false,
};

let lastKnownRates = null;

// ─── FOREX ────────────────────────────────────────────────────────
const forexCache = { usdInr: 94.5, xauUsd: 3310, xagUsd: 32.8, updatedAt: null, src: 'init' };

async function refreshForex() {
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
  let usdInr = 0, src = '';
  for (const [name, fn] of fxSrc) {
    if (usdInr) break;
    try { const v = await fn(); if (v > 70 && v < 110) { usdInr = v; src = name; } }
    catch (e) { console.warn('[FOREX]', name, e.message); }
  }
  let xauUsd = 0, xagUsd = 0;
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout: 6000 });
    if (Array.isArray(r.data)) {
      const g = r.data.find(x => x.gold), s = r.data.find(x => x.silver);
      if (g && g.gold > 3000) xauUsd = g.gold;
      if (s && s.silver > 20) xagUsd = s.silver;
    }
  } catch (e) { console.warn('[SPOT]', e.message); }
  if (!xauUsd) {
    try {
      const [g, s] = await Promise.all([
        axios.get('https://www.gold-api.com/price/XAU', { timeout: 7000 }),
        axios.get('https://www.gold-api.com/price/XAG', { timeout: 7000 }),
      ]);
      if (g.data.price > 3000) xauUsd = g.data.price;
      if (s.data.price > 20)   xagUsd = s.data.price;
    } catch (e) { console.warn('[SPOT2]', e.message); }
  }
  forexCache.usdInr    = usdInr    || forexCache.usdInr;
  forexCache.xauUsd    = xauUsd    || forexCache.xauUsd;
  forexCache.xagUsd    = xagUsd    || forexCache.xagUsd;
  forexCache.updatedAt = new Date().toISOString();
  forexCache.src       = src;
  console.log('[FOREX] usdInr=%s xauUsd=%s src=%s', forexCache.usdInr, forexCache.xauUsd, src);
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
  const d = new Date(Date.now() + 5.5 * 3600000);
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
  return WS.status === 'connected' && tickAge() < 15 && liveTick.gold.ltp > 0;
}
function updateHL(sym, ltp, high, low) {
  if (ltp > 0) {
    if (ltp > sessionHL[sym].high) sessionHL[sym].high = ltp;
    if (ltp < sessionHL[sym].low)  sessionHL[sym].low  = ltp;
  }
  if (high > 0 && high > sessionHL[sym].high) sessionHL[sym].high = high;
  if (low  > 0 && low  < sessionHL[sym].low)  sessionHL[sym].low  = low;
}

// ─── BINARY PARSER (V1 byte layout, 1-indexed in docs = 0-indexed in code) ──
// Ticker:    bytes 9-12 LTP → offset 8
// PrevClose: bytes 9-12 int32 → offset 8
// Quote:     bytes 9-12 LTP → offset 8, bytes 35-38 open → offset 34
// Full:      bytes 9-12 LTP → offset 8
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

    if (fc === 6 && buf.length >= 16) {
      // PrevClose — docs say int32, but community reports it should be float32
      const prevClose = buf.readFloatLE(8);
      return { type: 'prevClose', secId, prevClose: Math.round(prevClose) };
    }

    if (fc === 2 && buf.length >= 16) {
      const ltp = buf.readFloatLE(8);
      const ltt = buf.readUInt32LE(12);
      if (!Number.isFinite(ltp) || ltp <= 0) return null;
      return { type: 'ticker', secId, ltp: Math.round(ltp), ltt };
    }

    if (fc === 4 && buf.length >= 50) {
      const ltp  = buf.readFloatLE(8);
      const ltt  = buf.readUInt32LE(14);
      const open = buf.readFloatLE(34);
      const high = buf.readFloatLE(42);
      const low  = buf.readFloatLE(46);
      if (!Number.isFinite(ltp) || ltp <= 0) return null;
      return {
        type: 'quote', secId,
        ltp: Math.round(ltp), bid: Math.round(ltp), ask: Math.round(ltp),
        open: Math.round(open), high: Math.round(high), low: Math.round(low), ltt,
      };
    }

    if (fc === 8 && buf.length >= 112) {
      const ltp  = buf.readFloatLE(8);
      const ltt  = buf.readUInt32LE(14);
      const open = buf.readFloatLE(34);
      const high = buf.readFloatLE(42);
      const low  = buf.readFloatLE(46);
      let bestBid = 0, bestAsk = 0;
      // Market depth starts at byte 13 (after LTP 9-12) for V1 market depth packet
      // Full packet: LTP at 9-12, depth at 13-112
      if (buf.length >= 112) {
        bestBid = buf.readFloatLE(12 + 4);  // depth[0] bid price
        bestAsk = buf.readFloatLE(12 + 8);  // depth[0] ask price
      }
      if (!Number.isFinite(ltp) || ltp <= 0) return null;
      return {
        type: 'full', secId,
        ltp: Math.round(ltp),
        bid: bestBid > 0 ? Math.round(bestBid) : Math.round(ltp),
        ask: bestAsk > 0 ? Math.round(bestAsk) : Math.round(ltp),
        open: Math.round(open), high: Math.round(high), low: Math.round(low), ltt,
      };
    }

    // Unknown but log it
    if (WS.packetsReceived <= 10) {
      console.log('[PARSE] fc=%d len=%d hex=%s', fc, buf.length,
        buf.slice(0, Math.min(buf.length, 16)).toString('hex'));
    }
    return null;
  } catch (e) {
    console.warn('[PARSE]', e.message);
    return null;
  }
}

// ─── WEBSOCKET (V1 Protocol) ──────────────────────────────────────
function connectDhan() {
  if (!DHAN_CLIENT_ID || !DHAN_ACCESS_TOKEN) {
    console.warn('[WS] Missing credentials'); return;
  }
  if (WS.status === 'connecting' || WS.status === 'connected') return;

  WS.status = 'connecting';
  WS.lastConnectAt = new Date().toISOString();
  WS.lastDisconnectCode = null;
  WS.packetsReceived = 0;
  WS.authed = false;
  WS.lastRawBufHex = '';
  console.log('[WS] Connecting V1...');

  // V1 endpoint — NO query params
  const ws = new WebSocket('wss://api-feed.dhan.co', { handshakeTimeout: 15000 });
  WS.ws = ws;

  ws.on('open', () => {
    WS.status = 'connected';
    WS.reconnectCount = 0;
    console.log('[WS] Open — sending binary auth packet');

    const authPkt = buildAuthPacket();
    ws.send(authPkt);
    console.log('[WS] Auth packet sent (%d bytes)', authPkt.length);

    // After auth, subscribe
    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;

      const tickerPkt = buildSubscribePacket(FEED_REQ_CODES.SUBSCRIBE_TICKER, INSTRUMENTS);
      ws.send(tickerPkt);
      console.log('[WS] Subscribe TICKER sent');

      setTimeout(() => {
        if (ws.readyState !== WebSocket.OPEN) return;
        const quotePkt = buildSubscribePacket(FEED_REQ_CODES.SUBSCRIBE_QUOTE, INSTRUMENTS);
        ws.send(quotePkt);
        console.log('[WS] Subscribe QUOTE sent');
      }, 500);

    }, 1000);
  });

  ws.on('message', (data) => {
    if (typeof data === 'string') {
      WS.lastTextMsg = data.slice(0, 500);
      console.log('[WS] Text:', WS.lastTextMsg);
      return;
    }

    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    WS.packetsReceived++;

    if (WS.packetsReceived <= 10) {
      const hex = buf.slice(0, Math.min(buf.length, 32)).toString('hex');
      WS.lastRawBufHex = hex;
      console.log('[WS] Pkt#%d fc=%d len=%d hex=%s',
        WS.packetsReceived, buf.readUInt8(0), buf.length, hex);
    }

    const tick = parseBuf(buf);
    if (!tick) return;

    const token = TOKEN_MAP[tick.secId];
    if (!token) {
      console.log('[WS] Unknown secId=%s', tick.secId); return;
    }

    const key = token.key;
    WS.lastTickAt = Date.now();

    if (tick.type === 'prevClose') {
      liveTick[key].prevClose = tick.prevClose;
      console.log('[WS] PrevClose %s=%d', key, tick.prevClose);
      return;
    }

    liveTick[key] = {
      ...liveTick[key],
      ltp:  tick.ltp  || liveTick[key].ltp,
      bid:  tick.bid  || liveTick[key].bid,
      ask:  tick.ask  || liveTick[key].ask,
      open: tick.open || liveTick[key].open,
      high: tick.high || liveTick[key].high,
      low:  (tick.low && tick.low > 0) ? tick.low : liveTick[key].low,
      ts:   WS.lastTickAt,
    };

    if (key === 'gold' || key === 'silver') {
      updateHL(key, tick.ltp, tick.high, tick.low);
    }
  });

  ws.on('close', (code, reason) => {
    WS.status = 'disconnected';
    WS.lastDisconnectAt = new Date().toISOString();
    console.warn('[WS] Closed code=%s', code);
    scheduleReconnect();
  });

  ws.on('error', (err) => { console.warn('[WS] Error:', err.message); });
}

function scheduleReconnect() {
  if (WS.reconnectTimer) return;
  WS.reconnectCount++;
  const delay = Math.min(2000 * Math.pow(2, Math.min(WS.reconnectCount, 5)), 60000);
  console.log('[WS] Reconnect in', delay / 1000, 's');
  WS.reconnectTimer = setTimeout(() => { WS.reconnectTimer = null; connectDhan(); }, delay);
}

// ─── ROUTES ───────────────────────────────────────────────────────
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  const now = new Date().toISOString();

  if (isDhanLive()) {
    const g = liveTick.gold, s = liveTick.silver;
    const gN = liveTick.goldNext, sN = liveTick.silverNext;
    const payload = {
      success: true, source: 'dhan_mcx_live', marketOpen,
      tickAgeMs: Date.now() - WS.lastTickAt,
      goldPer10g: g.ltp, silverPerKg: s.ltp,
      futures: {
        gold:       { ltp: g.ltp, bid: g.bid, ask: g.ask, high: sessionHL.gold.high || g.high, low: sessionHL.gold.low === Infinity ? g.low : sessionHL.gold.low, open: g.open, prevClose: g.prevClose },
        silver:     { ltp: s.ltp, bid: s.bid, ask: s.ask, high: sessionHL.silver.high || s.high, low: sessionHL.silver.low === Infinity ? s.low : sessionHL.silver.low, open: s.open, prevClose: s.prevClose },
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
    goldPer10g, silverPerKg,
    usdInr: forexCache.usdInr, xauUsd: forexCache.xauUsd,
    forexUpdatedAt: forexCache.updatedAt, timestamp: now,
  });
});

app.get('/debug', (req, res) => {
  res.json({
    server: 'RR Jewellers V1-Auth v4',
    protocol: 'DhanHQ V1 binary auth',
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
    credentials: {
      clientId: !!DHAN_CLIENT_ID,
      clientIdLen: DHAN_CLIENT_ID.length,
      accessToken: !!DHAN_ACCESS_TOKEN,
      accessTokenLen: DHAN_ACCESS_TOKEN.length,
    },
    env: { SELF_URL: SELF_URL || null },
  });
});

app.get('/ping', (req, res) => res.json({ ok: true, ts: Date.now() }));
app.get('/', (req, res) => res.json({ status: 'RR Jewellers V1-Auth v4', endpoints: ['/rates', '/debug', '/ping'] }));

// ─── STARTUP ──────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', async () => {
  console.log('[STARTUP] RR Jewellers V1-Auth v4 on port', PORT);
  try { await refreshForex(); } catch (e) { console.warn('[STARTUP]', e.message); }
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
