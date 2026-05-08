'use strict';
// ╔══════════════════════════════════════════════════════════════════╗
// ║  RR Jewellers — Production Server v14                           ║
// ║  Ultra-low-latency hybrid: Dhan WS (primary) + FCS WS (backup) ║
// ║  Zero npm deps beyond express/axios/ws                          ║
// ╚══════════════════════════════════════════════════════════════════╝

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const axios     = require('axios');
const crypto    = require('crypto');
const cors      = require('cors');

const app    = express();
const server = http.createServer(app);
app.use(cors());
app.use(express.json());

// ── ENV ──────────────────────────────────────────────────────────────
const ENV = {
  PORT:              process.env.PORT              || 3000,
  SELF_URL:          process.env.SELF_URL          || '',
  SHEET_ID:          process.env.SHEET_ID          || '',
  DHAN_CLIENT_ID:    process.env.DHAN_CLIENT_ID    || '',
  DHAN_ACCESS_TOKEN: process.env.DHAN_ACCESS_TOKEN || '',
  DHAN_PIN:          process.env.DHAN_PIN          || '',
  DHAN_TOTP_SECRET:  process.env.DHAN_TOTP_SECRET  || '',
  TWELVE_DATA_KEY:   process.env.TWELVE_DATA_KEY   || '',
  FCS_API_KEY:       process.env.FCS_API_KEY       || '',
  METALPRICEAPI_KEY: process.env.METALPRICEAPI_KEY || '',
  get GOLD_MARGIN()  { return parseFloat(process.env.GOLD_MARGIN_PCT  || '0'); },
  get SILVER_MARGIN(){ return parseFloat(process.env.SILVER_MARGIN_PCT|| '0'); },
};

// ── MCX CONTRACTS ─────────────────────────────────────────────────────
const GOLD_CONTRACTS = [
  { secId:'459277', display:'GOLD JUN26',  expiry:'2026-06-05' },
  { secId:'466583', display:'GOLD AUG26',  expiry:'2026-08-05' },
  { secId:'483079', display:'GOLD OCT26',  expiry:'2026-10-05' },
  { secId:'495213', display:'GOLD DEC26',  expiry:'2026-12-04' },
  { secId:'559933', display:'GOLD FEB27',  expiry:'2027-02-05' },
];
const SILVER_CONTRACTS = [
  { secId:'464150', display:'SILVER JUL26', expiry:'2026-07-03' },
  { secId:'471725', display:'SILVER SEP26', expiry:'2026-09-04' },
  { secId:'495214', display:'SILVER DEC26', expiry:'2026-12-04' },
  { secId:'564619', display:'SILVER MAR27', expiry:'2027-03-05' },
];

function pickPair(contracts) {
  const now = new Date();
  const sorted = contracts
    .map(c => ({ ...c, ed: new Date(c.expiry) }))
    .filter(c => !isNaN(c.ed))
    .sort((a,b) => a.ed - b.ed);
  const up = sorted.filter(c => c.ed >= now);
  if (up.length >= 2) return { current: up[0], next: up[1] };
  if (up.length === 1) return { current: sorted[sorted.length-2]||up[0], next: up[0] };
  const last = sorted.slice(-2);
  return { current: last[0]||sorted[0], next: last[1]||sorted[0] };
}

let _acCache = null, _acExpiry = 0;
function getAC() {
  const now = Date.now();
  if (_acCache && now < _acExpiry) return _acCache;
  _acCache = { gold: pickPair(GOLD_CONTRACTS), silver: pickPair(SILVER_CONTRACTS) };
  _acExpiry = now + 60000;
  return _acCache;
}

// ── RATE CACHE ────────────────────────────────────────────────────────
const RC = {
  goldLtp:0, goldOpen:0, goldHigh:0, goldLow:0, goldPrevClose:0, goldBid:0, goldAsk:0,
  goldNextLtp:0, goldNextBid:0, goldNextAsk:0, goldNextHigh:0, goldNextLow:0,
  silverLtp:0, silverOpen:0, silverHigh:0, silverLow:0, silverPrevClose:0, silverBid:0, silverAsk:0,
  silverNextLtp:0, silverNextBid:0, silverNextAsk:0, silverNextHigh:0, silverNextLow:0,
  source:'init', updatedAt:null, updatedMs:0,
};

function applyTick(key, tick) {
  if (!tick || tick.ltp <= 0) return false;
  const l = tick.ltp;
  let changed = false;
  if (key === 'gold') {
    if (RC.goldLtp !== l) { RC.goldLtp = l; changed = true; }
    if (tick.high > 0 && RC.goldHigh !== tick.high) { RC.goldHigh = tick.high; changed = true; }
    if (tick.low > 0 && RC.goldLow !== tick.low) { RC.goldLow = tick.low; changed = true; }
    if (tick.open > 0) RC.goldOpen = tick.open;
    if (tick.prevClose > 0) RC.goldPrevClose = tick.prevClose;
    RC.goldBid = tick.bid > 0 ? tick.bid : l - 30;
    RC.goldAsk = tick.ask > 0 ? tick.ask : l + 30;
  } else if (key === 'goldNext') {
    if (RC.goldNextLtp !== l) { RC.goldNextLtp = l; changed = true; }
    RC.goldNextBid = tick.bid > 0 ? tick.bid : l - 50;
    RC.goldNextAsk = tick.ask > 0 ? tick.ask : l + 50;
    if (tick.high > 0) RC.goldNextHigh = tick.high;
    if (tick.low > 0) RC.goldNextLow = tick.low;
  } else if (key === 'silver') {
    if (RC.silverLtp !== l) { RC.silverLtp = l; changed = true; }
    if (tick.high > 0 && RC.silverHigh !== tick.high) { RC.silverHigh = tick.high; changed = true; }
    if (tick.low > 0 && RC.silverLow !== tick.low) { RC.silverLow = tick.low; changed = true; }
    if (tick.open > 0) RC.silverOpen = tick.open;
    if (tick.prevClose > 0) RC.silverPrevClose = tick.prevClose;
    RC.silverBid = tick.bid > 0 ? tick.bid : l - 100;
    RC.silverAsk = tick.ask > 0 ? tick.ask : l + 100;
  } else if (key === 'silverNext') {
    if (RC.silverNextLtp !== l) { RC.silverNextLtp = l; changed = true; }
    RC.silverNextBid = tick.bid > 0 ? tick.bid : l - 200;
    RC.silverNextAsk = tick.ask > 0 ? tick.ask : l + 200;
    if (tick.high > 0) RC.silverNextHigh = tick.high;
    if (tick.low > 0) RC.silverNextLow = tick.low;
  }
  if (changed) { RC.updatedAt = new Date().toISOString(); RC.updatedMs = Date.now(); }
  return changed;
}

// ── FOREX CACHE ───────────────────────────────────────────────────────
const FX = {
  usdInr:84.5, usdInrHigh:0, usdInrLow:Infinity, usdInrBid:0, usdInrAsk:0,
  xauUsd:0, xauBid:0, xauAsk:0, xauHigh:0, xauLow:0,
  xagUsd:0, xagBid:0, xagAsk:0, xagHigh:0, xagLow:0,
  updatedAt:null, src:'init', xauUpdatedAt:null, xagUpdatedAt:null,
};

// ── MARKET STATUS ─────────────────────────────────────────────────────
function isMCXOpen() {
  const d = new Date(Date.now() + 5.5 * 3600000);
  const dow = d.getUTCDay(), t = d.getUTCHours() * 60 + d.getUTCMinutes();
  if (dow === 0) return false;
  return dow === 6 ? (t >= 540 && t < 840) : (t >= 540 && t < 1435);
}
function msUntilIST(hh, mm) {
  const nowMs = Date.now();
  const ist = new Date(nowMs + 5.5 * 3600000);
  const [y, mo, d] = [ist.getUTCFullYear(), ist.getUTCMonth(), ist.getUTCDate()];
  let utcMin = hh * 60 + mm - 330;
  let utcH = Math.floor(utcMin / 60), utcM = utcMin % 60;
  if (utcH < 0) utcH += 24;
  const target = new Date(Date.UTC(y, mo, d, utcH, utcM, 0, 0));
  if (target.getTime() <= nowMs) target.setUTCDate(target.getUTCDate() + 1);
  return target.getTime() - nowMs;
}

// ── PAYLOAD BUILDER ───────────────────────────────────────────────────
function buildPayload() {
  const ac = getAC();
  const gSell = RC.goldLtp > 0   ? Math.round(RC.goldLtp   * (1 + ENV.GOLD_MARGIN   / 100)) : null;
  const sSell = RC.silverLtp > 0 ? Math.round(RC.silverLtp * (1 + ENV.SILVER_MARGIN / 100)) : null;
  return {
    ts: Date.now(), src: RC.source, mktOpen: isMCXOpen(),
    goldSell: gSell, silverSell: sSell,
    f: {
      g:  { ltp:RC.goldLtp||null,      bid:RC.goldBid||null,      ask:RC.goldAsk||null,
            high:RC.goldHigh||null,     low:RC.goldLow||null,      open:RC.goldOpen||null,
            pc:RC.goldPrevClose||null,  con:ac.gold.current.display,    exp:ac.gold.current.expiry },
      gN: { ltp:RC.goldNextLtp||null,  bid:RC.goldNextBid||null,  ask:RC.goldNextAsk||null,
            high:RC.goldNextHigh||null, low:RC.goldNextLow||null,
            con:ac.gold.next.display,       exp:ac.gold.next.expiry },
      s:  { ltp:RC.silverLtp||null,    bid:RC.silverBid||null,    ask:RC.silverAsk||null,
            high:RC.silverHigh||null,   low:RC.silverLow||null,    open:RC.silverOpen||null,
            pc:RC.silverPrevClose||null, con:ac.silver.current.display, exp:ac.silver.current.expiry },
      sN: { ltp:RC.silverNextLtp||null,bid:RC.silverNextBid||null,ask:RC.silverNextAsk||null,
            high:RC.silverNextHigh||null,low:RC.silverNextLow||null,
            con:ac.silver.next.display,     exp:ac.silver.next.expiry },
    },
    sp: {
      xauUsd:FX.xauUsd||null, xauBid:FX.xauBid||null, xauAsk:FX.xauAsk||null,
      xauHigh:FX.xauHigh||null, xauLow:FX.xauLow||null,
      xagUsd:FX.xagUsd||null, xagBid:FX.xagBid||null, xagAsk:FX.xagAsk||null,
      xagHigh:FX.xagHigh||null, xagLow:FX.xagLow||null,
      usdInr:FX.usdInr||null, usdInrBid:FX.usdInrBid||null, usdInrAsk:FX.usdInrAsk||null,
      usdInrHigh:FX.usdInrHigh||null,
      usdInrLow: FX.usdInrLow === Infinity ? null : FX.usdInrLow,
    },
    margin: { g: ENV.GOLD_MARGIN, s: ENV.SILVER_MARGIN },
  };
}

// ── HTML /feed WebSocket PUSH SERVER ──────────────────────────────────
// Every data source calls broadcast() independently at its own speed.
// Dhan tick   → broadcast() immediately  (exchange speed ~20-100ms)
// FCS tick    → broadcast() immediately  (~200ms)
// TD/FX REST  → broadcast() on update    (~3-5min)

const feedWSS = new WebSocket.Server({ server, path: '/feed' });
const feedClients = new Set();
let _lastBroadcastMsg = '';

feedWSS.on('connection', ws => {
  feedClients.add(ws);
  ws.isAlive = true;
  const snap = JSON.stringify(buildPayload());
  ws.send(snap); // instant snapshot on connect
  ws.on('pong',  () => { ws.isAlive = true; });
  ws.on('close', () => feedClients.delete(ws));
  ws.on('error', () => feedClients.delete(ws));
  METRICS.htmlConnects++;
  METRICS.htmlClients = feedClients.size;
});

// Heartbeat — kill dead connections, keep Render WS alive
setInterval(() => {
  feedClients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); feedClients.delete(ws); return; }
    ws.isAlive = false;
    ws.ping();
  });
  METRICS.htmlClients = feedClients.size;
}, 15000);

function broadcast() {
  if (feedClients.size === 0) return;
  const msg = JSON.stringify(buildPayload());
  if (msg === _lastBroadcastMsg) return; // deduplicate identical payloads
  _lastBroadcastMsg = msg;
  const buf = Buffer.from(msg);
  feedClients.forEach(ws => {
    if (ws.readyState === 1) ws.send(buf);
  });
  METRICS.broadcasts++;
}

// ── METRICS & HEALTH ─────────────────────────────────────────────────
const METRICS = {
  broadcasts: 0, htmlClients: 0, htmlConnects: 0,
  dhanPackets: 0, dhanReconnects: 0, dhanLastTickMs: 0,
  fcsPackets: 0,  fcsReconnects: 0,
  ohlcCalls: 0,   ohlcErrors: 0,
  startedAt: new Date().toISOString(),
};

// ── PURE NODE.JS TOTP (RFC 6238) ──────────────────────────────────────
function base32Decode(s) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  s = s.toUpperCase().replace(/=+$/, '');
  let bits = 0, val = 0, out = [];
  for (let i = 0; i < s.length; i++) {
    const idx = alpha.indexOf(s[i]);
    if (idx === -1) continue;
    val = (val << 5) | idx; bits += 5;
    if (bits >= 8) { bits -= 8; out.push((val >> bits) & 0xFF); }
  }
  return Buffer.from(out);
}
function getTOTP(secret) {
  if (!secret) return null;
  try {
    const key = base32Decode(secret.replace(/\s/g, ''));
    const t = Math.floor(Date.now() / 1000 / 30);
    const tb = Buffer.alloc(8);
    tb.writeUInt32BE(Math.floor(t / 0x100000000), 0);
    tb.writeUInt32BE(t >>> 0, 4);
    const hmac = crypto.createHmac('sha1', key).update(tb).digest();
    const offset = hmac[19] & 0xF;
    const code = ((hmac[offset] & 0x7F) << 24 | (hmac[offset+1] & 0xFF) << 16 |
                  (hmac[offset+2] & 0xFF) << 8  | (hmac[offset+3] & 0xFF)) % 1000000;
    return String(code).padStart(6, '0');
  } catch(e) { return null; }
}

// ── TOKEN MANAGER ─────────────────────────────────────────────────────
let currentToken = ENV.DHAN_ACCESS_TOKEN;
let tokenRenewedAt = null, renewAttempts = 0, renewRetryTimer = null;

function applyNewToken(t, src) {
  currentToken = t;
  tokenRenewedAt = new Date().toISOString();
  renewAttempts = 0;
  if (renewRetryTimer) { clearTimeout(renewRetryTimer); renewRetryTimer = null; }
  console.log(`[TOKEN] ✅ ${src} len=${t.length}`);
  if (DHAN.ws) { try { DHAN.ws.terminate(); } catch(e) {} }
  DHAN.status = 'disconnected';
  setTimeout(connectDhan, 2000);
}

async function renewToken() {
  if (!ENV.DHAN_CLIENT_ID) return false;

  // 0. Check if env var updated on Render (manual update)
  const envTok = process.env.DHAN_ACCESS_TOKEN || '';
  if (envTok && envTok !== currentToken && envTok.length > 100) {
    applyNewToken(envTok, 'env-update'); return true;
  }

  // 1. TOTP generateAccessToken (fully automatic, no browser)
  const pin = ENV.DHAN_PIN, secret = ENV.DHAN_TOTP_SECRET;
  if (pin && secret) {
    const totp = getTOTP(secret);
    if (totp) {
      try {
        const r = await axios.post('https://auth.dhan.co/app/generateAccessToken', {}, {
          params: { dhanClientId: ENV.DHAN_CLIENT_ID, pin, totp },
          timeout: 15000,
        });
        const t = r.data?.accessToken || r.data?.access_token;
        if (t && t.length > 100) { applyNewToken(t, 'TOTP'); return true; }
        console.warn('[TOKEN] TOTP: no token in response');
      } catch(e) {
        console.warn('[TOKEN] TOTP fail:', e.response?.status, e.message.slice(0, 60));
      }
    }
  }

  // 2. RenewToken (only if current token still active)
  if (currentToken) {
    try {
      const r = await axios.post('https://api.dhan.co/v2/RenewToken', {}, {
        headers: { 'access-token': currentToken, 'dhanClientId': ENV.DHAN_CLIENT_ID },
        timeout: 12000,
      });
      const t = r.data?.accessToken || r.data?.access_token;
      if (t && t.length > 100) { applyNewToken(t, 'RenewToken'); return true; }
    } catch(e) {
      if (e.response?.status === 400 || e.response?.status === 401)
        console.warn('[TOKEN] Expired — update DHAN_ACCESS_TOKEN on Render');
      else
        console.warn('[TOKEN] RenewToken fail:', e.message.slice(0, 60));
    }
  }
  return false;
}

async function renewWithRetry() {
  const ok = await renewToken();
  if (!ok) {
    renewAttempts++;
    const d = Math.min(renewAttempts * 5 * 60000, 30 * 60000);
    console.warn(`[TOKEN] Retry #${renewAttempts} in ${(d/60000).toFixed(0)}m`);
    renewRetryTimer = setTimeout(renewWithRetry, d);
  }
}

function scheduleDailyRenew() {
  const ms = msUntilIST(8, 30);
  console.log(`[TOKEN] Daily renew in ${(ms/60000).toFixed(0)}m`);
  setTimeout(() => { renewWithRetry(); scheduleDailyRenew(); }, ms);
}
function scheduleSpotHLReset() {
  setTimeout(() => {
    FX.xauHigh=0; FX.xauLow=0; FX.xagHigh=0; FX.xagLow=0;
    FX.usdInrHigh=0; FX.usdInrLow=Infinity;
    console.log('[HL-RESET] 9AM IST spot H/L reset');
    scheduleSpotHLReset();
  }, msUntilIST(9, 0));
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  DHAN WEBSOCKET — PRIMARY ULTRA-FAST MCX TICKS                  ║
// ║  RC15 (Ticker) = smallest packet, every exchange tick           ║
// ║  RC17 (Quote)  = +OHLC, sent 200ms after                        ║
// ╚══════════════════════════════════════════════════════════════════╝
const DHAN = {
  ws: null, status: 'disconnected',
  reconnectTimer: null, reconnectCount: 0,
  lastConnectAt: null, lastTickMs: 0,
  pingTimer: null, reconnectLockUntil: 0,
};
let TOKEN_MAP = {};

function buildTokenMap() {
  const ac = getAC(); TOKEN_MAP = {};
  TOKEN_MAP[ac.gold.current.secId]   = 'gold';
  TOKEN_MAP[ac.gold.next.secId]      = 'goldNext';
  TOKEN_MAP[ac.silver.current.secId] = 'silver';
  TOKEN_MAP[ac.silver.next.secId]    = 'silverNext';
}

function subscribeWS(ws) {
  const ac = getAC();
  const mcx = [
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.gold.current.secId },
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.gold.next.secId },
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.silver.current.secId },
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.silver.next.secId },
  ];
  const send = (obj, delay) => setTimeout(() => {
    if (ws.readyState === 1) ws.send(JSON.stringify(obj));
  }, delay);
  // RC15 = Ticker (LTP only, ~16 bytes) — immediate, fastest
  send({ RequestCode: 15, InstrumentCount: mcx.length, InstrumentList: mcx }, 0);
  // RC17 = Quote (OHLC + more) — 200ms after
  send({ RequestCode: 17, InstrumentCount: mcx.length, InstrumentList: mcx }, 200);
  console.log(`[DHAN] Subscribed RC15+RC17 to ${mcx.length} instruments`);
}

function parseDhanBuf(buf) {
  try {
    if (!buf || buf.length < 8) return null;
    const fc = buf.readUInt8(0);
    const secId = buf.readInt32LE(4).toString();
    if (fc === 50) return null; // heartbeat
    if (fc === 6 && buf.length >= 16) {
      const pc = buf.readFloatLE(8);
      return isFinite(pc) && pc > 0 ? { type:'prevClose', secId, prevClose: Math.round(pc) } : null;
    }
    if (fc === 2 && buf.length >= 16) {
      const l = buf.readFloatLE(8);
      return !isFinite(l) || l <= 10 ? null : { type:'ticker', secId, ltp: Math.round(l * 100) / 100 };
    }
    if (fc === 4 && buf.length >= 50) {
      const l = buf.readFloatLE(8);
      if (!isFinite(l) || l <= 10) return null;
      return { type:'quote', secId,
        ltp:  Math.round(l * 100) / 100,
        open: Math.round(buf.readFloatLE(34) * 100) / 100 || 0,
        high: Math.round(buf.readFloatLE(42) * 100) / 100 || 0,
        low:  Math.round(buf.readFloatLE(46) * 100) / 100 || 0,
      };
    }
    if (fc === 8 && buf.length >= 62) {
      const l = buf.readFloatLE(8);
      if (!isFinite(l) || l <= 10) return null;
      const o = buf.length > 49 ? Math.round(buf.readFloatLE(46) * 100) / 100 : 0;
      const h = buf.length > 57 ? Math.round(buf.readFloatLE(54) * 100) / 100 : 0;
      const lw = buf.length > 61 ? Math.round(buf.readFloatLE(58) * 100) / 100 : 0;
      let b = Math.round(l * 100) / 100, a = b;
      if (buf.length >= 82) {
        const bf = buf.readFloatLE(74), af = buf.readFloatLE(78);
        if (isFinite(bf) && bf > 10) b = Math.round(bf * 100) / 100;
        if (isFinite(af) && af > 10) a = Math.round(af * 100) / 100;
      }
      return { type:'full', secId, ltp:Math.round(l*100)/100, bid:b, ask:a, open:o, high:h, low:lw };
    }
    return null;
  } catch(e) { return null; }
}

function scheduleDhanReconnect() {
  if (DHAN.reconnectTimer) return;
  const now = Date.now();
  if (now < DHAN.reconnectLockUntil) {
    const wait = DHAN.reconnectLockUntil - now;
    console.log(`[DHAN] Reconnect locked for ${(wait/1000).toFixed(0)}s (cooldown)`);
    DHAN.reconnectTimer = setTimeout(() => { DHAN.reconnectTimer = null; connectDhan(); }, wait);
    return;
  }
  DHAN.reconnectCount++;
  if (DHAN.reconnectCount >= 5 && DHAN.reconnectCount % 5 === 0) {
    console.warn(`[DHAN] ${DHAN.reconnectCount} reconnects — triggering token renew`);
    renewWithRetry();
  }
  // Exponential backoff with jitter: 2s, 4s, 8s, 16s, max 30s
  const base = Math.min(2000 * Math.pow(2, Math.min(DHAN.reconnectCount - 1, 4)), 30000);
  const jitter = Math.random() * 1000;
  const delay = base + jitter;
  console.log(`[DHAN] Reconnect #${DHAN.reconnectCount} in ${(delay/1000).toFixed(1)}s`);
  DHAN.reconnectTimer = setTimeout(() => { DHAN.reconnectTimer = null; connectDhan(); }, delay);
}

function connectDhan() {
  if (!ENV.DHAN_CLIENT_ID || !currentToken) {
    console.warn('[DHAN] No credentials'); return;
  }
  if (DHAN.status === 'connecting' || DHAN.status === 'connected') return;
  DHAN.status = 'connecting';
  DHAN.lastConnectAt = new Date().toISOString();
  buildTokenMap();

  const url = `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(currentToken)}&clientId=${encodeURIComponent(ENV.DHAN_CLIENT_ID)}&authType=2`;
  const ws = new WebSocket(url, { handshakeTimeout: 15000 });
  DHAN.ws = ws;

  ws.on('open', () => {
    DHAN.status = 'connected';
    DHAN.reconnectCount = 0;
    console.log('[DHAN] ✅ Connected');
    subscribeWS(ws);
    // Keepalive ping every 20s — prevent idle disconnect
    if (DHAN.pingTimer) clearInterval(DHAN.pingTimer);
    DHAN.pingTimer = setInterval(() => {
      if (ws.readyState === 1) ws.ping();
    }, 20000);
  });

  // ── HOT PATH: every binary tick ──────────────────────────────────
  ws.on('message', data => {
    if (typeof data === 'string') return;
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    METRICS.dhanPackets++;
    const tick = parseDhanBuf(buf);
    if (!tick) return;

    DHAN.lastTickMs = Date.now();
    METRICS.dhanLastTickMs = DHAN.lastTickMs;

    const key = TOKEN_MAP[tick.secId];
    if (!key) return;

    if (tick.type === 'prevClose') {
      if (key === 'gold')   RC.goldPrevClose = tick.prevClose;
      if (key === 'silver') RC.silverPrevClose = tick.prevClose;
      return;
    }

    const changed = applyTick(key, tick);
    if (changed) {
      RC.source = 'dhan_ws_live';
      broadcast(); // ← push at Dhan tick speed
    }
  });

  ws.on('pong', () => { DHAN.lastTickMs = Date.now(); });

  ws.on('close', code => {
    DHAN.status = 'disconnected';
    if (DHAN.pingTimer) { clearInterval(DHAN.pingTimer); DHAN.pingTimer = null; }
    console.warn(`[DHAN] Closed code=${code} packets=${METRICS.dhanPackets}`);
    METRICS.dhanReconnects++;
    scheduleDhanReconnect();
  });

  ws.on('error', e => {
    console.warn('[DHAN] WS error:', e.message);
    // On 401/token issues, lock reconnect for 60s then renew
    if (e.message.includes('401') || e.message.includes('403')) {
      DHAN.reconnectLockUntil = Date.now() + 60000;
      renewWithRetry();
    }
  });
}

// Watchdog: if connected but no tick for 30s, reconnect
setInterval(() => {
  if (DHAN.status === 'connected' && DHAN.lastTickMs > 0) {
    const age = Date.now() - DHAN.lastTickMs;
    if (age > 30000 && isMCXOpen()) {
      console.warn(`[DHAN] Stale — no tick for ${(age/1000).toFixed(0)}s — reconnecting`);
      try { DHAN.ws?.terminate(); } catch(e) {}
      DHAN.status = 'disconnected';
      scheduleDhanReconnect();
    }
  }
  if (DHAN.status === 'disconnected' && !DHAN.reconnectTimer) {
    connectDhan();
  }
}, 30000);

// ╔══════════════════════════════════════════════════════════════════╗
// ║  FCS WEBSOCKET — XAU/USD, XAG/USD, USD/INR live ticks           ║
// ║  Independent source — broadcasts at its own speed (~200ms)       ║
// ╚══════════════════════════════════════════════════════════════════╝
const FCS = {
  ws: null, status: 'disconnected',
  reconnectTimer: null, reconnects: 0,
  lastTickMs: 0, pingTimer: null,
  reconnectLockUntil: 0,
};

function connectFCS() {
  if (!ENV.FCS_API_KEY) { console.warn('[FCS] No FCS_API_KEY'); return; }
  if (FCS.status === 'connecting' || FCS.status === 'connected') return;
  FCS.status = 'connecting';
  const url = `wss://ws-v4.fcsapi.com/ws?access_key=${ENV.FCS_API_KEY}`;
  const ws = new WebSocket(url, { handshakeTimeout: 15000 });
  FCS.ws = ws;

  ws.on('open', () => {
    FCS.status = 'connected'; FCS.reconnects = 0;
    console.log('[FCS] ✅ Connected');
    // Subscribe: XAU/USD, XAG/USD, USD/INR — timeframe "0" = every tick
    ['FX:XAUUSD','FX:XAGUSD','FX:USDINR'].forEach(sym => {
      ws.send(JSON.stringify({ type: 'join_symbol', symbol: sym, timeframe: '0' }));
    });
    if (FCS.pingTimer) clearInterval(FCS.pingTimer);
    FCS.pingTimer = setInterval(() => {
      if (ws.readyState === 1) ws.ping();
    }, 25000);
  });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }
    if (!msg || msg.type !== 'price') return;
    const p = msg.prices;
    if (!p) return;
    METRICS.fcsPackets++;
    FCS.lastTickMs = Date.now();

    const sym = msg.symbol || '';
    const price = p.c || p.close || 0;
    const bid   = p.b || p.bid   || 0;
    const ask   = p.a || p.ask   || 0;
    const now   = new Date().toISOString();
    let changed = false;

    if (sym === 'FX:XAUUSD' && price > 3000 && price < 9000) {
      FX.xauUsd = Math.round(price * 100) / 100;
      if (bid > 3000) FX.xauBid = Math.round(bid * 100) / 100;
      if (ask > 3000) FX.xauAsk = Math.round(ask * 100) / 100;
      if (!FX.xauHigh || price > FX.xauHigh) FX.xauHigh = Math.round(price * 100) / 100;
      if (!FX.xauLow  || price < FX.xauLow)  FX.xauLow  = Math.round(price * 100) / 100;
      FX.xauUpdatedAt = now; changed = true;
    } else if (sym === 'FX:XAGUSD' && price > 20 && price < 300) {
      FX.xagUsd = Math.round(price * 1000) / 1000;
      if (bid > 20) FX.xagBid = Math.round(bid * 1000) / 1000;
      if (ask > 20) FX.xagAsk = Math.round(ask * 1000) / 1000;
      if (!FX.xagHigh || price > FX.xagHigh) FX.xagHigh = Math.round(price * 1000) / 1000;
      if (!FX.xagLow  || price < FX.xagLow)  FX.xagLow  = Math.round(price * 1000) / 1000;
      FX.xagUpdatedAt = now; changed = true;
    } else if (sym === 'FX:USDINR' && price > 70 && price < 115) {
      FX.usdInr = Math.round(price * 100) / 100;
      if (bid > 70) FX.usdInrBid = Math.round(bid * 100) / 100;
      if (ask > 70) FX.usdInrAsk = Math.round(ask * 100) / 100;
      if (!FX.usdInrHigh || price > FX.usdInrHigh) FX.usdInrHigh = Math.round(price * 100) / 100;
      if (FX.usdInrLow === Infinity || price < FX.usdInrLow) FX.usdInrLow = Math.round(price * 100) / 100;
      FX.updatedAt = now; FX.src = 'fcs_ws'; changed = true;
    }

    if (changed) broadcast(); // push at FCS tick speed, independent of Dhan
  });

  ws.on('pong', () => { FCS.status = 'connected'; });

  ws.on('close', code => {
    FCS.status = 'disconnected';
    if (FCS.pingTimer) { clearInterval(FCS.pingTimer); FCS.pingTimer = null; }
    FCS.reconnects++;
    METRICS.fcsReconnects++;
    // Anti-429: if too many reconnects, add 60s cooldown
    if (FCS.reconnects > 3) FCS.reconnectLockUntil = Date.now() + 60000;
    const now = Date.now();
    const wait = Math.max(FCS.reconnectLockUntil - now, 0);
    const backoff = wait + Math.min(2000 * Math.pow(2, Math.min(FCS.reconnects - 1, 4)), 30000);
    console.warn(`[FCS] Closed code=${code} reconnect in ${(backoff/1000).toFixed(1)}s`);
    FCS.reconnectTimer = setTimeout(() => { FCS.reconnectTimer = null; connectFCS(); }, backoff);
  });

  ws.on('error', e => { console.warn('[FCS] err:', e.message); });
}

// FCS watchdog
setInterval(() => {
  if (ENV.FCS_API_KEY && FCS.status === 'disconnected' && !FCS.reconnectTimer) connectFCS();
}, 30000);

// ╔══════════════════════════════════════════════════════════════════╗
// ║  TWELVE DATA WEBSOCKET — XAU/USD live ticks                      ║
// ╚══════════════════════════════════════════════════════════════════╝
const TDws = { ws:null, status:'disconnected', reconnects:0, packets:0, pingTimer:null };

function connectTwelveData() {
  if (!ENV.TWELVE_DATA_KEY) return;
  if (TDws.status === 'connecting' || TDws.status === 'connected') return;
  TDws.status = 'connecting';
  const ws = new WebSocket(`wss://ws.twelvedata.com/v1/quotes/price?apikey=${ENV.TWELVE_DATA_KEY}`, { handshakeTimeout: 15000 });
  TDws.ws = ws;
  ws.on('open', () => {
    TDws.status = 'connected'; TDws.reconnects = 0;
    ws.send(JSON.stringify({ action: 'subscribe', params: { symbols: 'XAU/USD' } }));
    if (TDws.pingTimer) clearInterval(TDws.pingTimer);
    TDws.pingTimer = setInterval(() => { if (ws.readyState === 1) ws.ping(); }, 20000);
  });
  ws.on('message', data => {
    try {
      const msg = JSON.parse(data);
      if (msg.event === 'heartbeat') return;
      if (msg.event === 'price' && msg.symbol === 'XAU/USD' && msg.price) {
        const p = parseFloat(msg.price);
        if (p > 3000 && p < 9000) {
          // Only update if FCS is not connected (FCS is faster and has real bid/ask)
          if (FCS.status !== 'connected') {
            FX.xauUsd = Math.round(p * 100) / 100;
            FX.xauBid = Math.round((parseFloat(msg.bid||p)) * 100) / 100;
            FX.xauAsk = Math.round((parseFloat(msg.ask||p)) * 100) / 100;
            if (!FX.xauHigh || p > FX.xauHigh) FX.xauHigh = Math.round(p * 100) / 100;
            if (!FX.xauLow  || p < FX.xauLow)  FX.xauLow  = Math.round(p * 100) / 100;
            FX.xauUpdatedAt = new Date().toISOString();
            broadcast();
          } else {
            // FCS handles XAU — only update if stale > 5s
            if (Date.now() - (FCS.lastTickMs || 0) > 5000) {
              FX.xauUsd = Math.round(p * 100) / 100;
              FX.xauUpdatedAt = new Date().toISOString();
              broadcast();
            }
          }
          TDws.packets++;
        }
      }
    } catch(e) {}
  });
  ws.on('close', () => {
    TDws.status = 'disconnected';
    if (TDws.pingTimer) { clearInterval(TDws.pingTimer); TDws.pingTimer = null; }
    TDws.reconnects++;
    const d = Math.min(3000 * Math.pow(2, Math.min(TDws.reconnects - 1, 4)), 30000);
    setTimeout(() => connectTwelveData(), d);
  });
  ws.on('error', e => { console.warn('[TD] err:', e.message); });
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  REST DATA SOURCES — slower, periodic refresh                    ║
// ╚══════════════════════════════════════════════════════════════════╝

// USD/INR — every 5 min (fallback when FCS not connected)
async function refreshUsdInr() {
  if (FCS.status === 'connected') return; // FCS handles it faster
  const apis = [
    ['frankfurter', () => axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', {timeout:5000}).then(r => r.data.rates.INR)],
    ['open.er-api',  () => axios.get('https://open.er-api.com/v6/latest/USD',              {timeout:5000}).then(r => r.data.rates.INR)],
    ['fawazahmed0',  () => axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json', {timeout:5000}).then(r => r.data.usd.inr)],
  ];
  let v = 0, src = '';
  for (const [name, fn] of apis) {
    try { const r = await fn(); if (r > 70 && r < 115) { v = r; src = name; break; } } catch(e) {}
  }
  if (!v) { v = FX.usdInr; src = 'cached'; }
  FX.usdInr = Math.round(v * 100) / 100;
  if (FX.usdInr > 0) {
    FX.usdInrBid = Math.round((FX.usdInr - 0.03) * 100) / 100;
    FX.usdInrAsk = Math.round((FX.usdInr + 0.03) * 100) / 100;
    if (!FX.usdInrHigh || FX.usdInr > FX.usdInrHigh) FX.usdInrHigh = FX.usdInr;
    if (FX.usdInrLow === Infinity || FX.usdInr < FX.usdInrLow) FX.usdInrLow = FX.usdInr;
  }
  FX.updatedAt = new Date().toISOString(); FX.src = src;
  broadcast();
}

// XAG/USD — 6-source waterfall, every 3 min
async function pollXagUsd() {
  if (FCS.status === 'connected') return; // FCS handles XAG/USD live

  const setXag = (p, bid, ask, src) => {
    FX.xagUsd = Math.round(p * 1000) / 1000;
    FX.xagBid = bid > 0 ? Math.round(bid * 1000) / 1000 : 0;
    FX.xagAsk = ask > 0 ? Math.round(ask * 1000) / 1000 : 0;
    if (!FX.xagHigh || p > FX.xagHigh) FX.xagHigh = Math.round(p * 1000) / 1000;
    if (!FX.xagLow  || p < FX.xagLow)  FX.xagLow  = Math.round(p * 1000) / 1000;
    FX.xagUpdatedAt = new Date().toISOString();
    console.log(`[XAG] ${src} price=${FX.xagUsd}`);
    broadcast();
  };

  // 1. Twelve Data
  if (ENV.TWELVE_DATA_KEY) {
    try {
      const r = await axios.get('https://api.twelvedata.com/price', { params: { symbol:'XAG/USD', apikey:ENV.TWELVE_DATA_KEY }, timeout:8000 });
      const p = parseFloat(r.data?.price);
      if (p > 20 && p < 300) { setXag(p, 0, 0, 'TD'); return; }
    } catch(e) { console.warn('[XAG] TD fail:', e.message.slice(0, 40)); }
  }
  // 2. Frankfurter
  try {
    const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=XAG', { timeout:6000 });
    const rate = r.data?.rates?.XAG;
    if (rate > 0) { const p = Math.round((1/rate)*1000)/1000; if (p>20&&p<300) { setXag(p,0,0,'Frankfurter'); return; } }
  } catch(e) {}
  // 3. open.er-api
  try {
    const r = await axios.get('https://open.er-api.com/v6/latest/XAG', { timeout:6000 });
    const p = r.data?.rates?.USD;
    if (p > 20 && p < 300) { setXag(p, 0, 0, 'open.er-api'); return; }
  } catch(e) {}
  // 4. fawazahmed0 (unlimited, never fails)
  try {
    const r = await axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/xag.json', { timeout:7000 });
    const p = r.data?.xag?.usd;
    if (p > 20 && p < 300) { setXag(p, 0, 0, 'fawazahmed0'); return; }
  } catch(e) {}
}

// Twelve Data daily H/L batch — XAU + XAG + USD/INR every 10 min
async function pollSpotQuoteTD() {
  if (!ENV.TWELVE_DATA_KEY) return;
  try {
    const r = await axios.get('https://api.twelvedata.com/quote', {
      params: { symbol: 'XAU/USD,XAG/USD,USD/INR', apikey: ENV.TWELVE_DATA_KEY },
      timeout: 12000,
    });
    const data = r.data;
    const xau = data['XAU/USD'] || data;
    const xag = data['XAG/USD'];
    const inr = data['USD/INR'];
    if (xau?.high && xau?.low) {
      const h = parseFloat(xau.high), l = parseFloat(xau.low);
      if (h > 3000 && h < 9000) FX.xauHigh = Math.round(h * 100) / 100;
      if (l > 3000 && l < 9000) FX.xauLow  = Math.round(l * 100) / 100;
      if (!FX.xauUsd && xau.close) {
        const p = parseFloat(xau.close);
        if (p > 3000) FX.xauUsd = Math.round(p * 100) / 100;
      }
    }
    if (xag?.high && xag?.low) {
      const ah = parseFloat(xag.high), al = parseFloat(xag.low);
      if (ah > 20 && ah < 300) FX.xagHigh = Math.round(ah * 1000) / 1000;
      if (al > 20 && al < 300) FX.xagLow  = Math.round(al * 1000) / 1000;
      if (!FX.xagUsd && xag.close) {
        const p = parseFloat(xag.close);
        if (p > 20) { FX.xagUsd = Math.round(p*1000)/1000; FX.xagUpdatedAt = new Date().toISOString(); }
      }
    }
    if (inr?.high && inr?.low) {
      const ih = parseFloat(inr.high), il = parseFloat(inr.low);
      if (ih > 70 && ih < 115) FX.usdInrHigh = Math.round(ih * 100) / 100;
      if (il > 70 && il < 115) FX.usdInrLow  = Math.round(il * 100) / 100;
      if (inr.bid) FX.usdInrBid = Math.round(parseFloat(inr.bid) * 100) / 100;
      if (inr.ask) FX.usdInrAsk = Math.round(parseFloat(inr.ask) * 100) / 100;
    }
    broadcast();
  } catch(e) { console.warn('[TD-QUOTE] fail:', e.message.slice(0, 60)); }
}

// OHLC REST backup (5s, only when Dhan WS not ticking)
let ohlcBackoffUntil = 0;
async function pollOhlc() {
  if (!ENV.DHAN_CLIENT_ID || !currentToken) return;
  if (Date.now() < ohlcBackoffUntil) return;
  if (DHAN.status === 'connected' && DHAN.lastTickMs && Date.now() - DHAN.lastTickMs < 3000) return;
  const ac = getAC();
  const ids = [ac.gold.current.secId, ac.gold.next.secId, ac.silver.current.secId, ac.silver.next.secId].map(Number);
  try {
    const resp = await axios.post('https://api.dhan.co/v2/marketfeed/ohlc', { MCX_COMM: ids }, {
      headers: { 'Accept':'application/json', 'Content-Type':'application/json', 'access-token':currentToken, 'client-id':ENV.DHAN_CLIENT_ID },
      timeout: 5000,
    });
    const seg = resp.data?.data?.MCX_COMM;
    if (!seg) return;
    METRICS.ohlcCalls++;
    const applyRow = (secId, key) => {
      const row = seg[String(secId)];
      if (!row) return;
      const ltp = row.last_price || 0, ohlc = row.ohlc || {};
      if (ltp > 0) {
        applyTick(key, { ltp:Math.round(ltp), open:Math.round(ohlc.open||0), high:Math.round(ohlc.high||0), low:Math.round(ohlc.low||0) });
        RC.source = 'dhan_ohlc_rest';
      }
    };
    applyRow(ac.gold.current.secId,   'gold');
    applyRow(ac.gold.next.secId,      'goldNext');
    applyRow(ac.silver.current.secId, 'silver');
    applyRow(ac.silver.next.secId,    'silverNext');
    broadcast();
  } catch(e) {
    METRICS.ohlcErrors++;
    if (e.response?.status === 429) {
      ohlcBackoffUntil = Date.now() + 60000;
      console.warn('[OHLC] 429 — backoff 60s');
    }
  }
}

// ── ROUTES ────────────────────────────────────────────────────────────
app.get('/rates', (req, res) => {
  const p = buildPayload();
  // Legacy compatibility fields
  p.success = true;
  p.source = p.src;
  p.wsTickAgeMs = DHAN.lastTickMs ? Date.now() - DHAN.lastTickMs : null;
  p.goldPer10g = p.goldSell || 0;
  p.silverPerKg = p.silverSell || 0;
  const sp = p.sp || {};
  p.xauUsd = sp.xauUsd || 0; p.xagUsd = sp.xagUsd || 0; p.usdInr = sp.usdInr || 0;
  p.spot = sp;
  p.futures = {
    gold:       p.f?.g  ? { ltp:p.f.g.ltp,  bid:p.f.g.bid,  ask:p.f.g.ask,  high:p.f.g.high,  low:p.f.g.low  } : {},
    silver:     p.f?.s  ? { ltp:p.f.s.ltp,  bid:p.f.s.bid,  ask:p.f.s.ask,  high:p.f.s.high,  low:p.f.s.low  } : {},
    goldNext:   p.f?.gN ? { ltp:p.f.gN.ltp, bid:p.f.gN.bid, ask:p.f.gN.ask, high:p.f.gN.high, low:p.f.gN.low } : {},
    silverNext: p.f?.sN ? { ltp:p.f.sN.ltp, bid:p.f.sN.bid, ask:p.f.sN.ask, high:p.f.sN.high, low:p.f.sN.low } : {},
  };
  p.priceAsOf = p.ts ? new Date(p.ts).toISOString() : null;
  res.json(p);
});

app.get('/debug', (req, res) => res.json({
  server: 'RR Jewellers v14',
  htmlClients: feedClients.size,
  dhan: {
    wsStatus: DHAN.status, packets: METRICS.dhanPackets,
    tickAgeMs: DHAN.lastTickMs ? Date.now() - DHAN.lastTickMs : null,
    reconnects: METRICS.dhanReconnects, lastConnect: DHAN.lastConnectAt,
  },
  fcs: {
    wsStatus: FCS.status, packets: METRICS.fcsPackets,
    reconnects: METRICS.fcsReconnects, hasKey: !!ENV.FCS_API_KEY,
    tickAgeMs: FCS.lastTickMs ? Date.now() - FCS.lastTickMs : null,
  },
  twelveData: { wsStatus: TDws.status, packets: TDws.packets, hasKey: !!ENV.TWELVE_DATA_KEY },
  ohlc: { calls: METRICS.ohlcCalls, errors: METRICS.ohlcErrors, backoffUntil: ohlcBackoffUntil > Date.now() ? new Date(ohlcBackoffUntil).toISOString() : null },
  rateCache: RC, forexCache: FX,
  activeContracts: getAC(), tokenMap: TOKEN_MAP,
  marketOpen: isMCXOpen(), tokenRenewedAt, renewAttempts,
  env: { DHAN_CLIENT_ID: !!ENV.DHAN_CLIENT_ID, tokenLen: currentToken.length, TWELVE_DATA_KEY: !!ENV.TWELVE_DATA_KEY, FCS_API_KEY: !!ENV.FCS_API_KEY },
  metrics: METRICS,
}));

app.get('/health', (req, res) => {
  const ok = DHAN.status === 'connected' || FCS.status === 'connected';
  res.status(ok ? 200 : 503).json({
    status: ok ? 'ok' : 'degraded',
    dhan: DHAN.status, fcs: FCS.status,
    tickAgeMs: DHAN.lastTickMs ? Date.now() - DHAN.lastTickMs : null,
    uptime: process.uptime(),
  });
});

app.get('/token-renew', async (req, res) => {
  const ok = await renewToken();
  res.json({ success: ok, tokenRenewedAt, wsStatus: DHAN.status, tokenLen: currentToken.length });
});

app.get('/ping', (req, res) => res.json({
  ok: true, ts: Date.now(), dhan: DHAN.status, fcs: FCS.status,
  htmlClients: feedClients.size, tokenRenewedAt,
}));

app.get('/updates', async (req, res) => {
  try {
    if (!ENV.SHEET_ID) throw new Error('no SHEET_ID');
    const url = `https://docs.google.com/spreadsheets/d/${ENV.SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r = await axios.get(url, { timeout: 8000 });
    const json = r.data.replace(/.*?({.*}).*/s, '$1');
    const data = JSON.parse(json);
    const rows = data.table.rows.map(row => ({
      date: row.c[0]?.v || '', title: row.c[1]?.v || '', content: row.c[2]?.v || '', image: row.c[3]?.v || '',
    }));
    res.json({ success: true, updates: rows.filter(r => r.title) });
  } catch(e) {
    res.json({ success: true, updates: [{ date:'Today', title:'Welcome to R.R. Jewellers', content:'Live gold & silver rates.', image:'' }] });
  }
});

app.get('/', (req, res) => res.json({
  server: 'RR Jewellers v14', dhanWS: DHAN.status, fcsWS: FCS.status, tdWS: TDws.status,
  htmlClients: feedClients.size, tokenRenewedAt,
  endpoints: ['/rates', '/debug', '/health', '/ping', '/token-renew', '/updates', '/feed (WS)'],
}));

// ── STARTUP ───────────────────────────────────────────────────────────
server.listen(ENV.PORT, '0.0.0.0', async () => {
  console.log(`[STARTUP] RR Jewellers v14 port=${ENV.PORT}`);

  // 1. Token renew (TOTP auto)
  await renewWithRetry();

  // 2. Baseline forex
  await refreshUsdInr();

  // 3. Connect all WebSockets
  connectDhan();
  connectTwelveData();
  connectFCS();

  // 4. REST polls
  pollXagUsd();
  setInterval(pollXagUsd, 3 * 60 * 1000);

  pollSpotQuoteTD();
  setInterval(pollSpotQuoteTD, 10 * 60 * 1000);

  setInterval(() => { if (isMCXOpen()) pollOhlc(); }, 5000);
  setInterval(refreshUsdInr, 5 * 60 * 1000);

  // 5. Self-ping (Render free keepalive)
  setInterval(() => {
    axios.get((ENV.SELF_URL || `http://localhost:${ENV.PORT}`) + '/ping').catch(() => {});
  }, 4 * 60 * 1000);

  // 6. Token map rebuild daily
  setInterval(buildTokenMap, 24 * 60 * 60 * 1000);

  // 7. Daily schedules
  scheduleDailyRenew();
  scheduleSpotHLReset();

  console.log('[STARTUP] v14 ready — Dhan WS + FCS WS + TD WS active');
});
