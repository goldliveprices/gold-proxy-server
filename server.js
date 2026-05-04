'use strict';
// ═══════════════════════════════════════════════════════════
// RR JEWELLERS GOLD SERVER — Dhan Live Feed Edition
// Deploy on Render.com — NO static IP needed for market data
// Token auto-refreshes daily via cron using API key + secret
// ═══════════════════════════════════════════════════════════

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const crypto    = require('crypto');
const WebSocket = require('ws');

const app  = express();
app.use(cors());
app.use(express.json());

// ═══════════════════════════════════════
// ENV VARS — set these in Render Dashboard
// ═══════════════════════════════════════
const PORT             = process.env.PORT             || 3000;
const SELF_URL         = process.env.SELF_URL         || '';
const SHEET_ID         = process.env.SHEET_ID         || '';
const DHAN_CLIENT_ID   = process.env.DHAN_CLIENT_ID   || '';
const DHAN_ACCESS_TOKEN= process.env.DHAN_ACCESS_TOKEN|| '';  // refreshed daily
const DHAN_API_KEY     = process.env.DHAN_API_KEY     || '';  // for auto-refresh
const DHAN_API_SECRET  = process.env.DHAN_API_SECRET  || '';  // for auto-refresh
const DHAN_TOTP_SECRET = process.env.DHAN_TOTP_SECRET || '';  // for auto-refresh

// ═══════════════════════════════════════
// DHAN WEBSOCKET CONFIG
// ═══════════════════════════════════════
const DHAN_FEED_URL  = 'wss://api-feed.dhan.co';
const DHAN_API_BASE  = 'https://api.dhan.co/v2';

// ═══════════════════════════════════════
// MCX CONTRACT CYCLES
// Gold:   JAN MAR MAY JUL SEP NOV  (0,2,4,6,8,10 = month index)
// Silver: FEB APR JUN AUG NOV      (1,3,5,7,10)
// ═══════════════════════════════════════
const MONTHS    = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
const GOLD_M    = [0,2,4,6,8,10];
const SILVER_M  = [1,3,5,7,10];

// ═══════════════════════════════════════
// RUNTIME STATE
// ═══════════════════════════════════════
let currentAccessToken = DHAN_ACCESS_TOKEN;

// Live tick cache
const liveTick = {
  gold:       { ltp:0, bid:0, ask:0, high:0, low:0, open:0, ts:0 },
  silver:     { ltp:0, bid:0, ask:0, high:0, low:0, open:0, ts:0 },
  goldNext:   { ltp:0, bid:0, ask:0, high:0, low:0, open:0, ts:0 },
  silverNext: { ltp:0, bid:0, ask:0, high:0, low:0, open:0, ts:0 },
};

// Session high/low (reset daily at 9AM IST)
let sessionDayKey = '';
const sessionHL = {
  gold:   { high:0, low:Infinity },
  silver: { high:0, low:Infinity },
};

// WebSocket connection state
const WS = {
  ws:               null,
  wsStatus:         'disconnected',
  reconnectCount:   0,
  reconnectTimer:   null,
  lastConnectAt:    null,
  lastDisconnectAt: null,
  lastTickAt:       null,
};

// Instrument tokens (fetched from Dhan API on startup)
let TOKENS = {
  goldCurrent:    { secId:'', symbol:'' },
  goldNext:       { secId:'', symbol:'' },
  silverCurrent:  { secId:'', symbol:'' },
  silverNext:     { secId:'', symbol:'' },
};

let lastKnownRates = null;

// ═══════════════════════════════════════
// IST HELPERS
// ═══════════════════════════════════════
function getIST() {
  const d = new Date(Date.now() + 5.5*60*60*1000);
  return {
    year:  d.getUTCFullYear(),
    month: d.getUTCMonth(),
    day:   d.getUTCDate(),
    hour:  d.getUTCHours(),
    min:   d.getUTCMinutes(),
    dow:   d.getUTCDay(),   // 0=Sun
  };
}

function istDayKey() {
  const i = getIST();
  return `${i.year}-${i.month+1}-${i.day}`;
}

function isMCXOpen() {
  const { dow, hour, min } = getIST();
  if (dow === 0) return false;
  const t = hour*60 + min;
  if (dow === 6) return t >= 540 && t < 840;  // Sat 9AM-2PM
  return t >= 540 && t < 1435;                 // Mon-Fri 9AM-11:55PM
}

function resetSessionIfNewDay() {
  const k = istDayKey();
  if (sessionDayKey !== k) {
    sessionDayKey = k;
    sessionHL.gold   = { high:0, low:Infinity };
    sessionHL.silver = { high:0, low:Infinity };
    console.log('[SESSION] Reset for new day:', k);
  }
}

function updateSessionHL(sym, ltp, high, low) {
  resetSessionIfNewDay();
  if (ltp > 0) {
    if (ltp > sessionHL[sym].high) sessionHL[sym].high = ltp;
    if (ltp < sessionHL[sym].low)  sessionHL[sym].low  = ltp;
  }
  // Use vendor-provided high/low if better
  if (high > 0 && high > sessionHL[sym].high) sessionHL[sym].high = high;
  if (low  > 0 && low  < sessionHL[sym].low)  sessionHL[sym].low  = low;
}

function tickAgeSeconds() {
  if (!WS.lastTickAt) return Infinity;
  return Math.floor((Date.now() - WS.lastTickAt) / 1000);
}

function isDhanLive()  { return WS.wsStatus === 'connected' && tickAgeSeconds() < 10  && liveTick.gold.ltp > 0; }
function isDhanStale() { const a=tickAgeSeconds(); return liveTick.gold.ltp > 0 && a >= 10 && a < 300; }

// ═══════════════════════════════════════
// CONTRACT MONTH LOGIC
// ═══════════════════════════════════════
function getContracts(validM) {
  const ist = getIST();
  let m = ist.month, y = ist.year;
  // Find next 2 valid contract months
  const out = [];
  for (let i = 0; i < 24 && out.length < 2; i++) {
    if (validM.includes(m)) out.push({ month: MONTHS[m], year: y.toString().slice(-2), m, y });
    if (++m > 11) { m=0; y++; }
  }
  return out;
}

// ═══════════════════════════════════════
// DHAN INSTRUMENT LOOKUP
// Fetch correct Security IDs from Dhan API
// ═══════════════════════════════════════
async function fetchDhanInstruments() {
  try {
    console.log('[INSTRUMENTS] Fetching MCX instruments from Dhan...');
    const r = await axios.get(`${DHAN_API_BASE}/instrument/MCX_FO`, {
      headers: { 'access-token': currentAccessToken, 'client-id': DHAN_CLIENT_ID },
      timeout: 15000,
      responseType: 'text',
    });

    const lines = r.data.split('\n');
    const header = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
    const colMap = {};
    header.forEach((h,i) => colMap[h] = i);

    const secIdCol    = colMap['SEM_SMST_SECURITY_ID'] ?? colMap['SecurityId'] ?? colMap['securityId'] ?? 0;
    const symCol      = colMap['SEM_TRADING_SYMBOL']   ?? colMap['tradingsymbol'] ?? colMap['symbol'] ?? 1;
    const nameCol     = colMap['SEM_INSTRUMENT_NAME']  ?? colMap['instrumentName'] ?? 2;
    const expiryCol   = colMap['SM_EXPIRY_DATE']       ?? colMap['expiryDate'] ?? 5;

    const gold   = []; const silver = [];

    for (let i=1; i<lines.length; i++) {
      const cols = lines[i].split(',').map(c => c.trim().replace(/"/g,''));
      if (!cols[secIdCol]) continue;
      const name = (cols[nameCol]||'').toUpperCase();
      const sym  = (cols[symCol] ||'').toUpperCase();
      if (name === 'GOLD'   || sym.startsWith('GOLD-'))   gold.push({ secId:cols[secIdCol], symbol:sym, expiry:cols[expiryCol]||'' });
      if (name === 'SILVER' || sym.startsWith('SILVER-')) silver.push({ secId:cols[secIdCol], symbol:sym, expiry:cols[expiryCol]||'' });
    }

    // Sort by expiry ascending → first 2 are current + next
    const sortByExpiry = arr => arr.sort((a,b) => new Date(a.expiry) - new Date(b.expiry));
    const gSorted = sortByExpiry(gold.filter(g => new Date(g.expiry) > new Date()));
    const sSorted = sortByExpiry(silver.filter(s => new Date(s.expiry) > new Date()));

    if (gSorted.length >= 2 && sSorted.length >= 2) {
      TOKENS.goldCurrent   = gSorted[0];
      TOKENS.goldNext      = gSorted[1];
      TOKENS.silverCurrent = sSorted[0];
      TOKENS.silverNext    = sSorted[1];
      console.log('[INSTRUMENTS] Gold:', TOKENS.goldCurrent.symbol, TOKENS.goldNext.symbol);
      console.log('[INSTRUMENTS] Silver:', TOKENS.silverCurrent.symbol, TOKENS.silverNext.symbol);
      return true;
    }
    throw new Error('Not enough contracts found');
  } catch(e) {
    console.warn('[INSTRUMENTS] Failed:', e.message);
    // Hardcoded fallback tokens (update if needed)
    TOKENS = {
      goldCurrent:   { secId:'436177', symbol:'GOLD-JUN2026-MCX-FUT' },
      goldNext:      { secId:'436178', symbol:'GOLD-AUG2026-MCX-FUT' },
      silverCurrent: { secId:'436197', symbol:'SILVER-JUL2026-MCX-FUT' },
      silverNext:    { secId:'436198', symbol:'SILVER-SEP2026-MCX-FUT' },
    };
    console.log('[INSTRUMENTS] Using hardcoded fallback tokens');
    return false;
  }
}

// ═══════════════════════════════════════
// DHAN BINARY PACKET PARSER
// Based on official Dhan v2 spec (Little Endian)
// Full packet = Quote mode (type 21)
// ═══════════════════════════════════════
function parseDhanPacket(buf) {
  try {
    if (!buf || buf.length < 20) return null;

    const msgType = buf.readUInt8(0);
    const exchSeg = buf.readUInt8(1);
    const secId   = buf.readUInt32LE(2).toString();

    // LTP packet (type 1 / 11) — minimal data
    if (msgType === 1 || msgType === 11) {
      const ltp = buf.readFloatLE(6) / 100;
      if (ltp <= 0 || ltp > 1000000) return null;
      return { secId, ltp:Math.round(ltp), bid:Math.round(ltp), ask:Math.round(ltp), high:0, low:0, open:0, mode:'ltp' };
    }

    // Quote packet (type 2 / 21) — full OHLC + depth
    if (msgType === 2 || msgType === 21) {
      if (buf.length < 50) return null;
      const ltp  = Math.round(buf.readFloatLE(6)  / 100);
      // Quote packet offsets (Dhan v2 binary spec)
      let open=0, high=0, low=0, close=0, bid=ltp, ask=ltp;
      if (buf.length >= 58) {
        open  = Math.round(buf.readDoubleLE(10) / 100) || 0;
        high  = Math.round(buf.readDoubleLE(18) / 100) || 0;
        low   = Math.round(buf.readDoubleLE(26) / 100) || 0;
        close = Math.round(buf.readDoubleLE(34) / 100) || 0;
      }
      if (buf.length >= 74) {
        bid = Math.round(buf.readDoubleLE(42) / 100) || ltp;
        ask = Math.round(buf.readDoubleLE(50) / 100) || ltp;
      }
      if (ltp <= 0 || ltp > 1000000) return null;
      return { secId, ltp, bid, ask, high, low, open, close, mode:'quote' };
    }

    return null;
  } catch { return null; }
}

// ═══════════════════════════════════════
// WEBSOCKET — Connect, Auth, Subscribe
// ═══════════════════════════════════════
function connectDhan() {
  if (!DHAN_CLIENT_ID || !currentAccessToken) {
    console.warn('[WS] Missing DHAN_CLIENT_ID or access token — skipping WS connect');
    return;
  }
  if (WS.wsStatus === 'connecting' || WS.wsStatus === 'connected') return;

  WS.wsStatus     = 'connecting';
  WS.lastConnectAt = new Date().toISOString();
  console.log('[WS] Connecting to Dhan feed...');

  const ws = new WebSocket(DHAN_FEED_URL, { handshakeTimeout: 10000 });
  WS.ws = ws;

  ws.on('open', () => {
    WS.wsStatus      = 'connected';
    WS.reconnectCount = 0;
    console.log('[WS] Connected. Authenticating...');

    // Step 1: Login
    ws.send(JSON.stringify({
      LoginReq: {
        MsgCode:  11,
        ClientId: DHAN_CLIENT_ID,
        Token:    currentAccessToken,
      }
    }));

    // Step 2: Subscribe after 800ms (give auth time)
    setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      const instruments = [
        TOKENS.goldCurrent.secId,
        TOKENS.goldNext.secId,
        TOKENS.silverCurrent.secId,
        TOKENS.silverNext.secId,
      ].filter(Boolean).map(id => ({
        ExchangeSegment: 'MCX_FO',
        SecurityId:       id,
      }));

      ws.send(JSON.stringify({
        RequestCode:     21,   // Quote mode = full OHLC + depth
        InstrumentCount: instruments.length,
        InstrumentList:  instruments,
      }));
      console.log('[WS] Subscribed to', instruments.length, 'instruments');
    }, 800);
  });

  ws.on('message', (data) => {
    // Auth/text messages
    if (typeof data === 'string') {
      try {
        const msg = JSON.parse(data);
        if (msg?.LoginResp?.Response === 'Success') {
          console.log('[WS] Auth confirmed');
        } else if (msg?.LoginResp?.Response) {
          console.warn('[WS] Auth response:', msg.LoginResp.Response);
        }
      } catch {}
      return;
    }

    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (buf.length < 10) return;

    const tick = parseDhanPacket(buf);
    if (!tick || tick.ltp <= 0) return;

    WS.lastTickAt = Date.now();

    if (tick.secId === TOKENS.goldCurrent.secId) {
      liveTick.gold = { ...tick, ts: WS.lastTickAt };
      updateSessionHL('gold', tick.ltp, tick.high, tick.low);

    } else if (tick.secId === TOKENS.goldNext.secId) {
      liveTick.goldNext = { ...tick, ts: WS.lastTickAt };

    } else if (tick.secId === TOKENS.silverCurrent.secId) {
      liveTick.silver = { ...tick, ts: WS.lastTickAt };
      updateSessionHL('silver', tick.ltp, tick.high, tick.low);

    } else if (tick.secId === TOKENS.silverNext.secId) {
      liveTick.silverNext = { ...tick, ts: WS.lastTickAt };
    }
  });

  ws.on('close', (code, reason) => {
    WS.wsStatus          = 'disconnected';
    WS.lastDisconnectAt  = new Date().toISOString();
    console.log('[WS] Disconnected:', code, reason?.toString()?.slice(0,50)||'');
    scheduleReconnect();
  });

  ws.on('error', (err) => {
    console.warn('[WS] Error:', err.message.slice(0,80));
  });
}

function scheduleReconnect() {
  if (WS.reconnectTimer) return;
  WS.reconnectCount++;
  const delay = Math.min(2000 * Math.pow(2, Math.min(WS.reconnectCount,5)), 60000);
  console.log(`[WS] Reconnecting in ${delay/1000}s (attempt ${WS.reconnectCount})`);
  WS.reconnectTimer = setTimeout(() => {
    WS.reconnectTimer = null;
    connectDhan();
  }, delay);
}

// ═══════════════════════════════════════
// TOKEN AUTO-REFRESH (TOTP-based, daily)
// ═══════════════════════════════════════
function generateTOTP(secret) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const v = alpha.indexOf(c);
    if (v >= 0) bits += v.toString(2).padStart(5,'0');
  }
  const bytes = [];
  for (let i=0; i+8<=bits.length; i+=8) bytes.push(parseInt(bits.slice(i,i+8),2));
  const key = Buffer.from(bytes);
  const t   = Math.floor(Date.now()/1000/30);
  const tb  = Buffer.alloc(8);
  tb.writeUInt32BE(Math.floor(t/0x100000000),0);
  tb.writeUInt32BE(t>>>0,4);
  const hmac = crypto.createHmac('sha1',key).update(tb).digest();
  const off  = hmac[hmac.length-1]&0xf;
  const code = ((hmac[off]&0x7f)<<24|(hmac[off+1]&0xff)<<16|(hmac[off+2]&0xff)<<8|hmac[off+3]&0xff)%1000000;
  return code.toString().padStart(6,'0');
}

async function refreshDhanToken() {
  if (!DHAN_API_KEY || !DHAN_API_SECRET) {
    console.log('[TOKEN] No API key/secret — using manual token');
    return false;
  }
  try {
    const totp = DHAN_TOTP_SECRET ? generateTOTP(DHAN_TOTP_SECRET) : undefined;
    const body  = { clientId: DHAN_CLIENT_ID, apiKey: DHAN_API_KEY, apiSecret: DHAN_API_SECRET };
    if (totp) body.totp = totp;

    const r = await axios.post(`${DHAN_API_BASE}/token`, body, { timeout:10000 });
    const newToken = r.data?.access_token || r.data?.accessToken || r.data?.data?.access_token;
    if (newToken) {
      currentAccessToken = newToken;
      console.log('[TOKEN] Refreshed successfully');
      // Reconnect WS with new token
      if (WS.ws) WS.ws.terminate();
      setTimeout(connectDhan, 2000);
      return true;
    }
    throw new Error('No token in response: ' + JSON.stringify(r.data).slice(0,100));
  } catch(e) {
    console.warn('[TOKEN] Refresh failed:', e.message.slice(0,80));
    return false;
  }
}

// ═══════════════════════════════════════
// SPOT FALLBACK (used when WS has no data)
// ═══════════════════════════════════════
async function getSpotRates() {
  // Source 1: metals.live
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout:6000 });
    if (Array.isArray(r.data)) {
      const gold   = r.data.find(x=>x.gold)?.gold;
      const silver = r.data.find(x=>x.silver)?.silver;
      if (gold>3000 && gold<8000 && silver>20 && silver<200)
        return { xauUsd:gold, xagUsd:silver, src:'metals.live' };
    }
  } catch {}
  // Source 2: gold-api.com
  try {
    const [g,s] = await Promise.all([
      axios.get('https://www.gold-api.com/price/XAU',{timeout:7000}),
      axios.get('https://www.gold-api.com/price/XAG',{timeout:7000}),
    ]);
    const xau=g.data?.price, xag=s.data?.price;
    if (xau>3000&&xau<8000&&xag>20&&xag<200)
      return { xauUsd:xau, xagUsd:xag, src:'gold-api.com' };
  } catch {}
  // Fixed fallback (May 2026 approximate)
  return { xauUsd:3310, xagUsd:32.8, src:'fixed_fallback' };
}

async function getForex() {
  try {
    const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000});
    const v = r.data?.rates?.INR;
    if (v>82&&v<110) return v;
  } catch {}
  try {
    const r = await axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000});
    const v = r.data?.rates?.INR;
    if (v>82&&v<110) return v;
  } catch {}
  return 94.5;
}

async function getSpotDerived() {
  const [spot, usdInr] = await Promise.all([getSpotRates(), getForex()]);
  // MCX import duty + GST + premium factor
  const F = 1.103;
  const gLtp  = Math.round(spot.xauUsd/31.1035*10*usdInr*F);
  const sLtp  = Math.round(spot.xagUsd/31.1035*1000*usdInr*F);
  return { gLtp, sLtp, usdInr, xauUsd:spot.xauUsd, xagUsd:spot.xagUsd, src:spot.src };
}

// ═══════════════════════════════════════
// HELPER: build contracts labels
// ═══════════════════════════════════════
function buildContracts() {
  const gC = getContracts(GOLD_M);
  const sC = getContracts(SILVER_M);
  return {
    gold:   { current: gC[0]?.month+(gC[0]?.year||'') || TOKENS.goldCurrent.symbol,
              next:    gC[1]?.month+(gC[1]?.year||'') || TOKENS.goldNext.symbol },
    silver: { current: sC[0]?.month+(sC[0]?.year||'') || TOKENS.silverCurrent.symbol,
              next:    sC[1]?.month+(sC[1]?.year||'') || TOKENS.silverNext.symbol },
  };
}

// ═══════════════════════════════════════
// /rates — Main endpoint (RAM only)
// ═══════════════════════════════════════
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  const contracts  = buildContracts();
  const now        = new Date().toISOString();

  // ── Source 1: LIVE Dhan feed ──────────────────
  if (isDhanLive()) {
    const g  = liveTick.gold,       s  = liveTick.silver;
    const gN = liveTick.goldNext,   sN = liveTick.silverNext;

    const gHigh = sessionHL.gold.high   || g.high;
    const gLow  = sessionHL.gold.low === Infinity ? g.low : sessionHL.gold.low;
    const sHigh = sessionHL.silver.high || s.high;
    const sLow  = sessionHL.silver.low === Infinity ? s.low : sessionHL.silver.low;

    const payload = {
      success: true, source: 'dhan_mcx_live', marketOpen,
      tickAgeMs:   Date.now()-WS.lastTickAt,
      tickAgeSeconds: tickAgeSeconds(),
      contracts,
      goldPer10g:   g.ltp,
      silverPerKg:  s.ltp,
      futures: {
        gold:      { ltp:g.ltp,   bid:g.bid,   ask:g.ask,   high:gHigh, low:gLow, open:g.open   },
        silver:    { ltp:s.ltp,   bid:s.bid,   ask:s.ask,   high:sHigh, low:sLow, open:s.open   },
        goldNext:  { ltp:gN.ltp||g.ltp, bid:gN.bid||g.bid, ask:gN.ask||g.ask, high:gN.high||gHigh, low:gN.low||gLow, open:gN.open||g.open },
        silverNext:{ ltp:sN.ltp||s.ltp, bid:sN.bid||s.bid, ask:sN.ask||s.ask, high:sN.high||sHigh, low:sN.low||sLow, open:sN.open||s.open },
      },
      timestamp: now,
    };
    lastKnownRates = { ...payload };
    return res.json(payload);
  }

  // ── Source 2: Last known rates (stale feed) ──
  if (isDhanStale() || lastKnownRates) {
    return res.json({
      ...(lastKnownRates||{}),
      success:        true,
      source:         'last_known_rates',
      marketOpen,
      tickAgeSeconds: tickAgeSeconds() === Infinity ? null : tickAgeSeconds(),
      priceAsOf:      WS.lastTickAt ? new Date(WS.lastTickAt).toISOString() : null,
      timestamp:      now,
    });
  }

  // ── Source 3: Spot derived (pure fallback, NO mock) ──
  const d = await getSpotDerived();
  return res.json({
    success: true, source: 'spot_derived', marketOpen,
    note: 'No live feed — showing international spot converted to INR',
    spotSource: d.src,
    usdInr:  d.usdInr,
    xauUsd:  d.xauUsd,
    xagUsd:  d.xagUsd,
    contracts,
    goldPer10g:  d.gLtp,
    silverPerKg: d.sLtp,
    futures: {
      // Only LTP available — bid/ask set to LTP, high/low null (not fabricated)
      gold:      { ltp:d.gLtp,     bid:d.gLtp,   ask:d.gLtp,   high:null, low:null, open:null },
      silver:    { ltp:d.sLtp,     bid:d.sLtp,   ask:d.sLtp,   high:null, low:null, open:null },
      goldNext:  { ltp:null, bid:null, ask:null, high:null, low:null, open:null },
      silverNext:{ ltp:null, bid:null, ask:null, high:null, low:null, open:null },
    },
    timestamp: now,
  });
});

// ═══════════════════════════════════════
// /debug
// ═══════════════════════════════════════
app.get('/debug', (req, res) => {
  res.json({
    server:          'RR Jewellers Gold Server — Dhan Edition',
    wsStatus:         WS.wsStatus,
    lastTickAt:       WS.lastTickAt ? new Date(WS.lastTickAt).toISOString() : null,
    tickAgeSeconds:   tickAgeSeconds() === Infinity ? null : tickAgeSeconds(),
    reconnectCount:   WS.reconnectCount,
    lastConnectAt:    WS.lastConnectAt,
    lastDisconnectAt: WS.lastDisconnectAt,
    currentSource:    isDhanLive() ? 'dhan_mcx_live' : (isDhanStale()||lastKnownRates ? 'last_known_rates' : 'spot_derived'),
    marketOpen:       isMCXOpen(),
    sessionHL,
    liveTick,
    tokens:           TOKENS,
    lastKnownRatesAt: lastKnownRates?.timestamp || null,
    credentials: {
      clientId:    !!DHAN_CLIENT_ID,
      accessToken: !!currentAccessToken,
      apiKey:      !!DHAN_API_KEY,
      apiSecret:   !!DHAN_API_SECRET,
      totpSecret:  !!DHAN_TOTP_SECRET,
    },
  });
});

// ═══════════════════════════════════════
// /spot-test, /forex-test, /cache-status, /ping
// ═══════════════════════════════════════
app.get('/spot-test',  async (req,res) => { try { res.json(await getSpotDerived()); } catch(e){ res.status(500).json({error:e.message}); } });
app.get('/forex-test', async (req,res) => { res.json({ usdInr: await getForex() }); });
app.get('/cache-status',(req,res) => {
  res.json({
    goldContracts:   getContracts(GOLD_M).map(c=>c.month+c.year),
    silverContracts: getContracts(SILVER_M).map(c=>c.month+c.year),
    tokens:          TOKENS,
    wsStatus:        WS.wsStatus,
    tickAgeSeconds:  tickAgeSeconds()===Infinity?null:tickAgeSeconds(),
    lastKnownRatesAt:lastKnownRates?.timestamp||null,
  });
});
app.get('/ping', (req,res) => res.json({ ok:true, ts:Date.now() }));

// ═══════════════════════════════════════
// /updates — Google Sheets
// ═══════════════════════════════════════
app.get('/updates', async (req,res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID not set');
    const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r   = await axios.get(url, { timeout:8000 });
    const json = r.data.replace(/.*?({.*}).*/s,'$1');
    const data = JSON.parse(json);
    const rows = data.table.rows.map(row=>({
      date:    row.c[0]?.v||'',
      title:   row.c[1]?.v||'',
      content: row.c[2]?.v||'',
      image:   row.c[3]?.v||'',
    }));
    res.json({ success:true, updates:rows.filter(r=>r.title) });
  } catch {
    res.json({ success:true, updates:[{
      date:'Today', title:'Welcome to R.R. Jewellers',
      content:'Live gold & silver rates. Contact us for best prices!', image:''
    }]});
  }
});

// ═══════════════════════════════════════
// Root
// ═══════════════════════════════════════
app.get('/', (req,res) => res.json({
  status: 'RR Jewellers Gold Server — Dhan Edition',
  wsStatus: WS.wsStatus,
  endpoints:['/rates','/debug','/spot-test','/forex-test','/cache-status','/ping','/updates'],
}));

// ═══════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════
app.listen(PORT, '0.0.0.0', async () => {
  console.log(`[STARTUP] RR Jewellers Gold Server — port ${PORT}`);

  // 1. Fetch instrument tokens
  await fetchDhanInstruments();

  // 2. Connect WebSocket
  connectDhan();

  // 3. Session reset every minute
  setInterval(resetSessionIfNewDay, 60*1000);

  // 4. Self-ping every 4 min (Render free tier keep-alive)
  setInterval(() => {
    axios.get((SELF_URL||`http://localhost:${PORT}`)+'/ping').catch(()=>{});
  }, 4*60*1000);

  // 5. Token refresh daily at 7:55 AM IST (before market open at 9 AM)
  setInterval(async () => {
    const { hour, min } = getIST();
    if (hour===7 && min>=55 && min<=58) {
      console.log('[TOKEN] Daily refresh triggered at 7:55 AM IST');
      await refreshDhanToken();
    }
  }, 60*1000);   // checks every minute

  // 6. Reconnect WS if dead (every 2 min)
  setInterval(() => {
    if (WS.wsStatus==='disconnected' && !WS.reconnectTimer) {
      console.log('[HEALTH] WS dead — forcing reconnect');
      connectDhan();
    }
  }, 2*60*1000);
});
