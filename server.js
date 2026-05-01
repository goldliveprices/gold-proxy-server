'use strict';

/**
 * R.R Jewellers — Gold & Silver Rates Server v24
 * 
 * ARCHITECTURE:
 * - Angel One SmartAPI WebSocket → per-second MCX ticks (SNAP_QUOTE mode)
 * - In-memory tick cache → /rates returns instantly (no wait)
 * - SSE /stream endpoint → index.html gets pushed on every tick
 * - Spot rates polled every 30s from gold-api / goldprice.org
 */

const express  = require('express');
const cors     = require('cors');
const axios    = require('axios');
const crypto   = require('crypto');
const https    = require('https');
const WebSocket = require('ws');
const app      = express();

app.use(cors());
app.use(express.json());

// ═══════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════
const CLIENT_ID      = process.env.CLIENT_ID      || 'AAAA238852';
const API_KEY        = process.env.API_KEY;
const TOTP_SECRET    = process.env.TOTP_SECRET    || 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN      = process.env.ANGEL_PIN      || '1857';
const SHEET_ID       = process.env.SHEET_ID       || '';
const SELF_URL       = process.env.SELF_URL       || 'https://gold-proxy-server.onrender.com/ping';
const PROXY_URL      = process.env.QUOTAGUARDSTATIC_URL || null;
const PREMIUM_FACTOR = parseFloat(process.env.PREMIUM_FACTOR || '1.103');
const PORT           = process.env.PORT || 3000;

const MONTHS = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];

// ═══════════════════════════════════════
// IN-MEMORY TICK STORE
// ═══════════════════════════════════════
let tickStore = {};        // { [symbolToken]: { ltp, bid, ask, high, low, open, close, ts } }
let spotStore = {          // latest spot rates
  xauUsd: 0, xagUsd: 0, usdInr: 0,
  xauInrPerOz: 0, xagInrPerOz: 0,
  xauUsdHigh: null, xauUsdLow: null,
  xagUsdHigh: null, xagUsdLow: null,
  usdInrHigh: null, usdInrLow: null,
  src: 'init', ts: 0
};
let tokenMap = {};          // { [symbolToken]: 'GOLD_CUR' | 'GOLD_NXT' | 'SILVER_CUR' | 'SILVER_NXT' }
let activeTokens = [];      // current MCX tokens to subscribe
let wsConn = null;          // active WebSocket connection
let feedToken = null;       // Angel feed token from login
let JWT = null, JWT_EXP = 0;
let wsReconnectTimer = null;
let wsConnected = false;
let lastWsTick = 0;

// SSE clients
let sseClients = new Set();

// ═══════════════════════════════════════
// PROXY
// ═══════════════════════════════════════
function makeAxios(useProxy = false) {
  if (useProxy && PROXY_URL) {
    try {
      const u = new URL(PROXY_URL);
      return axios.create({
        proxy: { protocol: u.protocol.replace(':', ''), host: u.hostname,
          port: parseInt(u.port) || 9293,
          auth: u.username ? { username: decodeURIComponent(u.username), password: decodeURIComponent(u.password) } : undefined }
      });
    } catch (e) {}
  }
  return axios;
}

// ═══════════════════════════════════════
// IST HELPERS
// ═══════════════════════════════════════
function getIST() {
  const ist = new Date(Date.now() + (5 * 60 + 30) * 60 * 1000);
  return { year: ist.getUTCFullYear(), month: ist.getUTCMonth(), day: ist.getUTCDate(),
           hour: ist.getUTCHours(), min: ist.getUTCMinutes(), dow: ist.getUTCDay() };
}
function isWeekday() { return getIST().dow !== 0; }
const MCX_HOLIDAYS = new Set(['2026-01-26','2026-03-25','2026-04-02','2026-04-14',
  '2026-04-30','2026-05-01','2026-08-15','2026-10-02','2026-10-20','2026-11-04','2026-12-25']);
function isMCXHoliday() {
  const { year, month, day } = getIST();
  return MCX_HOLIDAYS.has(`${year}-${String(month+1).padStart(2,'0')}-${String(day).padStart(2,'0')}`);
}

// ═══════════════════════════════════════
// SCRIP MASTER CACHE
// ═══════════════════════════════════════
let tokenCache = { GOLD: {}, SILVER: {} };
let cacheBuiltAt = 0;
const CACHE_TTL = 22 * 60 * 60 * 1000;

function getExpiryDate(year, month) {
  const fifth = new Date(Date.UTC(year, month, 5));
  const dow = fifth.getUTCDay();
  if (dow === 0) return new Date(Date.UTC(year, month, 3));
  if (dow === 6) return new Date(Date.UTC(year, month, 4));
  return fifth;
}

async function buildScripCache() {
  try {
    console.log('📋 Fetching ScripMaster...');
    const r = await axios.get(
      'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json',
      { timeout: 30000 }
    );
    if (!Array.isArray(r.data) || r.data.length === 0) return false;
    const nc = { GOLD: {}, SILVER: {} };
    for (const inst of r.data) {
      if (inst.exch_seg !== 'MCX' || inst.instrumenttype !== 'FUTCOM') continue;
      const sym = (inst.symbol || '').toUpperCase().trim();
      const tok = inst.token;
      const exp = (inst.expiry || '').toUpperCase().trim();
      if (!sym || !tok) continue;
      let label = null;
      const em = exp.match(/([A-Z]{3})20(\d{2})/);
      if (em) label = em[1] + em[2];
      else { const sm = sym.match(/([A-Z]{3})(\d{2})/); if (sm) label = sm[1] + sm[2]; }
      if (!label || !MONTHS.includes(label.slice(0,3))) continue;
      if (sym.startsWith('GOLD') && !sym.includes('GOLDM') && !sym.includes('GOLDPETAL') && !sym.includes('GOLDGUINEA') && !sym.includes('TEN'))
        nc.GOLD[label] = { symboltoken: String(tok), tradingsymbol: sym };
      if (sym.startsWith('SILVER') && !sym.includes('SILVERM') && !sym.includes('SILVERMIC')) {
        const ex = nc.SILVER[label];
        if (!ex || sym.startsWith('SILVER30')) nc.SILVER[label] = { symboltoken: String(tok), tradingsymbol: sym };
      }
    }
    tokenCache = nc; cacheBuiltAt = Date.now();
    console.log(`✅ Cache GOLD:${Object.keys(nc.GOLD).length} SILVER:${Object.keys(nc.SILVER).length}`);
    return true;
  } catch (e) { console.error('❌ buildScripCache:', e.message); return false; }
}

async function ensureCache() {
  if (!cacheBuiltAt || Date.now() - cacheBuiltAt > CACHE_TTL) await buildScripCache();
}

function getDynamicContracts(base) {
  const cache = tokenCache[base] || {};
  const now = getIST();
  const curYr2 = parseInt(now.year.toString().slice(-2));
  const valid = Object.keys(cache).filter(k => {
    const mon = MONTHS.indexOf(k.slice(0,3)), yr2 = parseInt(k.slice(3,5));
    if (yr2 < curYr2) return false;
    if (yr2 === curYr2 && mon < now.month) return false;
    if (yr2 === curYr2 && mon === now.month) {
      const exp = getExpiryDate(now.year, mon);
      if (new Date(Date.UTC(now.year, mon, now.day)) > exp) return false;
    }
    return true;
  });
  valid.sort((a,b) => {
    const ya=parseInt(a.slice(3,5)),ma=MONTHS.indexOf(a.slice(0,3));
    const yb=parseInt(b.slice(3,5)),mb=MONTHS.indexOf(b.slice(0,3));
    return ya!==yb ? ya-yb : ma-mb;
  });
  return valid.slice(0,2);
}

// ═══════════════════════════════════════
// TOTP + AUTH
// ═══════════════════════════════════════
function generateTOTP(secret, offset = 0) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) { const v = alpha.indexOf(c); if (v >= 0) bits += v.toString(2).padStart(5,'0'); }
  const bytes = [];
  for (let i = 0; i+8 <= bits.length; i+=8) bytes.push(parseInt(bits.slice(i,i+8),2));
  const key = Buffer.from(bytes);
  const t = Math.floor(Date.now()/30000) + offset;
  const tb = Buffer.alloc(8);
  tb.writeUInt32BE(Math.floor(t/0x100000000),0); tb.writeUInt32BE(t>>>0,4);
  const hmac = crypto.createHmac('sha1',key).update(tb).digest();
  const off = hmac[hmac.length-1] & 0x0f;
  const code = (((hmac[off]&0x7f)<<24)|((hmac[off+1]&0xff)<<16)|((hmac[off+2]&0xff)<<8)|(hmac[off+3]&0xff)) % 1000000;
  return code.toString().padStart(6,'0');
}

const buildHeaders = (jwt) => ({
  'Content-Type': 'application/json', 'Accept': 'application/json',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
  'X-UserType': 'USER', 'X-SourceID': 'WEB',
  'X-ClientLocalIP': '192.168.1.5', 'X-ClientPublicIP': '74.220.52.100',
  'X-MACAddress': '00:1A:2B:3C:4D:5E', 'X-PrivateKey': API_KEY,
  ...(jwt ? { Authorization: 'Bearer ' + jwt } : {}),
});

async function login() {
  if (JWT && Date.now() < JWT_EXP) return { jwt: JWT, feedToken };
  JWT = null; JWT_EXP = 0; feedToken = null;
  for (const offset of [-2,-1,0,1,2]) {
    const totp = generateTOTP(TOTP_SECRET, offset);
    try {
      const r = await makeAxios(true).post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        { clientcode: CLIENT_ID, password: ANGEL_PIN, totp },
        { headers: buildHeaders(), timeout: 10000 }
      );
      if (r.data?.status === true && r.data?.data?.jwtToken) {
        JWT       = r.data.data.jwtToken;
        feedToken = r.data.data.feedToken || null;
        JWT_EXP   = Date.now() + 6 * 60 * 60 * 1000;
        console.log(`✅ Angel login OK (offset ${offset}), feedToken: ${feedToken ? 'yes' : 'no'}`);
        return { jwt: JWT, feedToken };
      }
    } catch (e) { console.warn(`⚠️ login offset ${offset}:`, e.message); }
  }
  throw new Error('Angel login failed all TOTP offsets');
}

// ═══════════════════════════════════════
// ANGEL SMARTAPI WEBSOCKET (per-second ticks)
// ═══════════════════════════════════════
function parseTick(buf) {
  // Angel binary tick format (SNAP_QUOTE mode = 3)
  // https://smartapi.angelbroking.com/docs (WebSocket feed)
  try {
    if (!Buffer.isBuffer(buf) && !(buf instanceof ArrayBuffer)) return null;
    const data = Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
    if (data.length < 50) return null;
    // Subscription mode 3 (SNAP_QUOTE): 184 bytes per token
    const token     = data.readInt32BE(19).toString();  // symbol token
    const seqNo     = data.readBigInt64BE(1);
    const ltp       = data.readInt32BE(43) / 100;
    const open      = data.readInt32BE(55) / 100;
    const high      = data.readInt32BE(59) / 100;
    const low       = data.readInt32BE(63) / 100;
    const close     = data.readInt32BE(67) / 100;
    const bestBidP  = data.readInt32BE(71) / 100;
    const bestAskP  = data.readInt32BE(99) / 100;
    return { token, ltp, open, high, low, close, bid: bestBidP, ask: bestAskP, ts: Date.now() };
  } catch(e) { return null; }
}

function buildSubscribeMsg(tokens) {
  // exchangeType 5 = MCX
  return JSON.stringify({
    correlationID: 'rr_mcx',
    action: 1,
    params: {
      mode: 3,  // SNAP_QUOTE (has OHLC + bid/ask)
      tokenList: [{ exchangeType: 5, tokens }]
    }
  });
}

function pushSSE(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    try { client.write(msg); } catch(e) { sseClients.delete(client); }
  }
}

async function connectAngelWS() {
  if (wsConn && (wsConn.readyState === WebSocket.CONNECTING || wsConn.readyState === WebSocket.OPEN)) return;
  if (!isWeekday()) { console.log('📅 Sunday — skip WS'); return; }

  try {
    await ensureCache();
    const gC = getDynamicContracts('GOLD');
    const sC = getDynamicContracts('SILVER');
    if (!gC.length || !sC.length) { console.warn('⚠️ No contracts found'); scheduleWsReconnect(30000); return; }

    const { jwt, feedToken: ft } = await login();
    if (!ft) { console.warn('⚠️ No feedToken — WS unavailable, falling back to REST poll'); startRestFallbackPoll(); return; }

    const gCurTok  = tokenCache.GOLD[gC[0]];
    const gNxtTok  = gC[1] ? tokenCache.GOLD[gC[1]] : null;
    const sCurTok  = tokenCache.SILVER[sC[0]];
    const sNxtTok  = sC[1] ? tokenCache.SILVER[sC[1]] : null;

    tokenMap = {};
    activeTokens = [];
    for (const [t, label] of [
      [gCurTok, 'GOLD_CUR'], [gNxtTok, 'GOLD_NXT'],
      [sCurTok, 'SILVER_CUR'], [sNxtTok, 'SILVER_NXT']
    ]) {
      if (t) { tokenMap[t.symboltoken] = label; activeTokens.push(t.symboltoken); }
    }

    console.log(`🔌 Connecting Angel WS, tokens: ${activeTokens}`);
    const ws = new WebSocket('wss://smartapisocket.angelone.in/smart-stream', {
      headers: {
        Authorization: 'Bearer ' + jwt,
        'x-api-key': API_KEY,
        'x-client-code': CLIENT_ID,
        'x-feed-token': ft,
      }
    });

    ws.on('open', () => {
      console.log('✅ Angel WS connected');
      wsConnected = true;
      ws.send(buildSubscribeMsg(activeTokens));
      // Heartbeat every 25s
      ws._pingTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.ping();
      }, 25000);
    });

    ws.on('message', (raw) => {
      lastWsTick = Date.now();
      // Angel sends either JSON (ack) or binary (tick)
      if (typeof raw === 'string' || (Buffer.isBuffer(raw) && raw[0] === 123)) {
        try { const j = JSON.parse(raw.toString()); console.log('WS msg:', JSON.stringify(j).slice(0,100)); }
        catch(e) {}
        return;
      }
      const tick = parseTick(raw);
      if (!tick || tick.ltp <= 0) return;
      const label = tokenMap[tick.token];
      if (!label) return;
      tickStore[tick.token] = tick;
      // Push SSE on every tick
      pushSSE(buildRatesPayload());
    });

    ws.on('error', (e) => { console.error('❌ Angel WS error:', e.message); wsConnected = false; });
    ws.on('close', (code, reason) => {
      console.warn(`⚠️ Angel WS closed: ${code} ${reason}`);
      wsConnected = false;
      clearInterval(ws._pingTimer);
      scheduleWsReconnect(5000);
    });
    ws.on('pong', () => { lastWsTick = Date.now(); });

    wsConn = ws;
  } catch(e) {
    console.error('❌ connectAngelWS:', e.message);
    scheduleWsReconnect(15000);
  }
}

function scheduleWsReconnect(delay = 5000) {
  clearTimeout(wsReconnectTimer);
  wsReconnectTimer = setTimeout(() => { connectAngelWS(); }, delay);
}

// ═══════════════════════════════════════
// REST FALLBACK POLL (if no feedToken)
// ═══════════════════════════════════════
let restPollTimer = null;

async function getBulkQuotes(jwt, tokens) {
  const unique = [...new Set(tokens.filter(Boolean).map(String))];
  if (!unique.length) return {};
  const r = await makeAxios(true).post(
    'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
    { mode: 'FULL', exchangeTokens: { MCX: unique } },
    { headers: buildHeaders(jwt), timeout: 10000 }
  );
  const out = {};
  for (const d of (r.data?.data?.fetched || [])) {
    out[String(d.symbolToken)] = {
      ltp:   Number(d.ltp)                     || 0,
      bid:   Number(d.depth?.buy?.[0]?.price)  || Number(d.ltp) || 0,
      ask:   Number(d.depth?.sell?.[0]?.price) || Number(d.ltp) || 0,
      high:  Number(d.high)                    || 0,
      low:   Number(d.low)                     || 0,
      open:  Number(d.open)                    || 0,
      close: Number(d.close)                   || 0,
      ts:    Date.now()
    };
  }
  return out;
}

function isValidQuote(q) { return q && q.ltp > 100; }

function roundQuote(q) {
  if (!q || !isValidQuote(q)) return null;
  return {
    ltp:   Math.round(q.ltp),
    bid:   Math.round(q.bid)   || null,
    ask:   Math.round(q.ask)   || null,
    high:  q.high  > 0 ? Math.round(q.high)  : null,
    low:   q.low   > 0 ? Math.round(q.low)   : null,
    open:  q.open  > 0 ? Math.round(q.open)  : null,
    close: q.close > 0 ? Math.round(q.close) : null,
  };
}

async function restPollOnce() {
  try {
    await ensureCache();
    const gC = getDynamicContracts('GOLD');
    const sC = getDynamicContracts('SILVER');
    if (!gC.length || !sC.length) return;
    const { jwt } = await login();
    const gCurTok = tokenCache.GOLD[gC[0]];
    const gNxtTok = gC[1] ? tokenCache.GOLD[gC[1]] : null;
    const sCurTok = tokenCache.SILVER[sC[0]];
    const sNxtTok = sC[1] ? tokenCache.SILVER[sC[1]] : null;
    tokenMap = {};
    activeTokens = [];
    for (const [t, label] of [[gCurTok,'GOLD_CUR'],[gNxtTok,'GOLD_NXT'],[sCurTok,'SILVER_CUR'],[sNxtTok,'SILVER_NXT']]) {
      if (t) { tokenMap[t.symboltoken] = label; activeTokens.push(t.symboltoken); }
    }
    const quotes = await getBulkQuotes(jwt, activeTokens);
    for (const [tok, q] of Object.entries(quotes)) {
      if (isValidQuote(q)) tickStore[tok] = q;
    }
    pushSSE(buildRatesPayload());
  } catch(e) { console.error('❌ restPollOnce:', e.message); }
}

function startRestFallbackPoll() {
  clearInterval(restPollTimer);
  console.log('🔄 Starting REST fallback poll (5s interval)');
  restPollOnce();
  restPollTimer = setInterval(restPollOnce, 5000);
}

// ═══════════════════════════════════════
// SPOT RATES (poll every 30s)
// ═══════════════════════════════════════
async function getForex() {
  try {
    const r = await axios.get('https://api.exchangerate-api.com/v4/latest/USD', { timeout: 5000 });
    const v = r.data?.rates?.INR;
    if (v && v > 70) return v;
  } catch(e) {}
  return 84.5;
}

async function refreshSpotRates() {
  try {
    // Primary: gold-api.com
    const [gRes, sRes] = await Promise.all([
      axios.get('https://api.gold-api.com/price/XAU/INR', { timeout: 7000 }),
      axios.get('https://api.gold-api.com/price/XAG/INR', { timeout: 7000 }),
    ]);
    const xauInrPerOz = gRes.data?.price;
    const xagInrPerOz = sRes.data?.price;
    const usdInr      = gRes.data?.exchangeRate;
    const xauUsd      = xauInrPerOz / usdInr;
    const xagUsd      = xagInrPerOz / usdInr;
    // high/low from gold-api if available
    const xauUsdHigh = gRes.data?.high_price ? gRes.data.high_price / usdInr : null;
    const xauUsdLow  = gRes.data?.low_price  ? gRes.data.low_price  / usdInr : null;
    const xagUsdHigh = sRes.data?.high_price ? sRes.data.high_price / usdInr : null;
    const xagUsdLow  = sRes.data?.low_price  ? sRes.data.low_price  / usdInr : null;
    if (xauInrPerOz > 50000 && xagInrPerOz > 500 && usdInr > 70) {
      spotStore = { xauUsd, xagUsd, xauInrPerOz, xagInrPerOz, usdInr,
        xauUsdHigh, xauUsdLow, xagUsdHigh, xagUsdLow,
        usdInrHigh: null, usdInrLow: null, src: 'gold-api.com', ts: Date.now() };
      return;
    }
  } catch(e) {}

  try {
    // Secondary: goldprice.org (has H/L)
    const r = await axios.get('https://data-asg.goldprice.org/dbXRates/INR', {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://goldprice.org' }, timeout: 7000
    });
    const item = r.data?.items?.[0];
    const xauInrPerOz = item?.xauPrice;
    const xagInrPerOz = item?.xagPrice;
    const usdInr      = item?.usdInr || 84.5;
    const xauUsd      = xauInrPerOz / usdInr;
    const xagUsd      = xagInrPerOz / usdInr;
    if (xauInrPerOz > 50000 && xagInrPerOz > 500) {
      spotStore = { xauUsd, xagUsd, xauInrPerOz, xagInrPerOz, usdInr,
        xauUsdHigh: item?.xauHighPrice ? item.xauHighPrice / usdInr : null,
        xauUsdLow:  item?.xauLowPrice  ? item.xauLowPrice  / usdInr : null,
        xagUsdHigh: item?.xagHighPrice ? item.xagHighPrice / usdInr : null,
        xagUsdLow:  item?.xagLowPrice  ? item.xagLowPrice  / usdInr : null,
        usdInrHigh: null, usdInrLow: null, src: 'goldprice.org', ts: Date.now() };
      return;
    }
  } catch(e) {}
}

// ═══════════════════════════════════════
// BUILD RATES PAYLOAD (from tickStore)
// ═══════════════════════════════════════
function getTickByLabel(label) {
  for (const [tok, lb] of Object.entries(tokenMap)) {
    if (lb === label) return tickStore[tok] || null;
  }
  return null;
}

function fromSpotToMcx(sp) {
  const f = PREMIUM_FACTOR;
  if (!sp.xauUsd || !sp.usdInr) return { goldPer10g: null, silverPerKg: null };
  return {
    goldPer10g:  Math.round((sp.xauUsd / 31.1035) * 10 * sp.usdInr * f),
    silverPerKg: Math.round(sp.xagUsd * sp.usdInr * 1000 / 31.1035 * f),
  };
}

function buildRatesPayload() {
  const gCurr = getTickByLabel('GOLD_CUR');
  const sCurr = getTickByLabel('SILVER_CUR');
  const gNext = getTickByLabel('GOLD_NXT');
  const sNext = getTickByLabel('SILVER_NXT');
  const mcxLive = !!(gCurr && isValidQuote(gCurr) && sCurr && isValidQuote(sCurr));
  const source = mcxLive ? 'mcx_live' : 'mcx_unavailable';
  const sp = spotStore;
  const derived = fromSpotToMcx(sp);
  const gKeys = Object.keys(tokenCache.GOLD || {});
  const sKeys = Object.keys(tokenCache.SILVER || {});
  return {
    success: true, source,
    weekdaySession: isWeekday(),
    isHoliday: isMCXHoliday(),
    wsConnected,
    usdInr:     sp.usdInr      ? parseFloat(sp.usdInr.toFixed(4))   : null,
    xauUsd:     sp.xauUsd      ? parseFloat(sp.xauUsd.toFixed(2))    : null,
    xagUsd:     sp.xagUsd      ? parseFloat(sp.xagUsd.toFixed(4))    : null,
    spotSource: sp.src,
    spot: {
      xauUsd:      sp.xauUsd      ? parseFloat(sp.xauUsd.toFixed(2))    : null,
      xagUsd:      sp.xagUsd      ? parseFloat(sp.xagUsd.toFixed(4))    : null,
      xauInrPerOz: sp.xauInrPerOz ? Math.round(sp.xauInrPerOz)          : null,
      xagInrPerOz: sp.xagInrPerOz ? Math.round(sp.xagInrPerOz)          : null,
      xauUsdHigh:  sp.xauUsdHigh  ? parseFloat(sp.xauUsdHigh.toFixed(2)) : null,
      xauUsdLow:   sp.xauUsdLow   ? parseFloat(sp.xauUsdLow.toFixed(2))  : null,
      xagUsdHigh:  sp.xagUsdHigh  ? parseFloat(sp.xagUsdHigh.toFixed(4)) : null,
      xagUsdLow:   sp.xagUsdLow   ? parseFloat(sp.xagUsdLow.toFixed(4))  : null,
      usdInrHigh:  sp.usdInrHigh  ? parseFloat(sp.usdInrHigh.toFixed(4)) : null,
      usdInrLow:   sp.usdInrLow   ? parseFloat(sp.usdInrLow.toFixed(4))  : null,
    },
    derivedFromSpot: derived,
    goldPer10g:  mcxLive && gCurr ? Math.round(gCurr.ltp) : null,
    silverPerKg: mcxLive && sCurr ? Math.round(sCurr.ltp) : null,
    futures: {
      gold:       roundQuote(gCurr),
      silver:     roundQuote(sCurr),
      goldNext:   roundQuote(gNext),
      silverNext: roundQuote(sNext),
    },
    contracts: {
      gold:   { current: gKeys[0]||null, next: gKeys[1]||null },
      silver: { current: sKeys[0]||null, next: sKeys[1]||null },
    },
    _debug: { wsConnected, lastWsTick, wsMode: feedToken ? 'websocket' : 'rest_poll' },
    timestamp: new Date().toISOString(),
  };
}

// ═══════════════════════════════════════
// ROUTES
// ═══════════════════════════════════════

// Health
app.get('/', (req, res) => res.json({ status: 'R.R Jewellers Server v24 ✅', wsConnected, timestamp: new Date().toISOString() }));
app.get('/ping', (req, res) => res.json({ ok: true }));

// REST rates (instant from cache)
app.get('/rates', async (req, res) => {
  try {
    res.json(buildRatesPayload());
  } catch(e) {
    res.status(500).json({ success: false, source: 'error', error: e.message });
  }
});

// SSE stream — push on every tick
app.get('/stream', (req, res) => {
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();

  // Send current state immediately
  res.write(`data: ${JSON.stringify(buildRatesPayload())}\n\n`);

  sseClients.add(res);
  console.log(`📡 SSE client connected (total: ${sseClients.size})`);

  // Keep alive ping every 20s
  const keepAlive = setInterval(() => {
    try { res.write(': ping\n\n'); } catch(e) {}
  }, 20000);

  req.on('close', () => {
    sseClients.delete(res);
    clearInterval(keepAlive);
    console.log(`📡 SSE client disconnected (total: ${sseClients.size})`);
  });
});

// Debug
app.get('/debug', async (req, res) => {
  await ensureCache();
  res.json({
    weekdaySession: isWeekday(), isHoliday: isMCXHoliday(),
    wsConnected, lastWsTick: lastWsTick ? new Date(lastWsTick).toISOString() : null,
    wsMode: feedToken ? 'websocket' : 'rest_poll',
    sseClients: sseClients.size,
    tickStoreKeys: Object.keys(tickStore),
    tokenMap, activeTokens,
    cacheSize: { gold: Object.keys(tokenCache.GOLD).length, silver: Object.keys(tokenCache.SILVER).length },
  });
});

// Updates (Google Sheets)
app.get('/updates', async (req, res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID not set');
    const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Sheet1`;
    const r = await axios.get(url, { timeout: 8000 });
    const json = JSON.parse(r.data.replace(/^[^(]+\(|\);?$/g, ''));
    const rows = (json.table?.rows || []).map(row => ({
      title:   row.c[0]?.v || '', content: row.c[1]?.v || '',
      date:    row.c[2]?.v || '', active:  row.c[3]?.v !== false,
    }));
    res.json({ success: true, updates: rows.filter(r => r.title) });
  } catch(e) {
    res.json({ success: true, updates: [{ title: 'Welcome to R.R. Jewellers', content: 'Your trusted jewellers in Etawah since 1982.', date: '', active: true }] });
  }
});

// ═══════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════
async function startup() {
  await buildScripCache();
  await refreshSpotRates();

  // Start WebSocket connection (or REST fallback)
  if (isWeekday()) await connectAngelWS();

  // Reconnect WS if stale (no tick for 60s during market hours)
  setInterval(() => {
    if (!isWeekday()) return;
    const stale = lastWsTick && (Date.now() - lastWsTick > 60000);
    const neverConnected = !lastWsTick && wsConnected === false;
    if (stale || neverConnected) {
      console.log('🔄 WS stale/dead — reconnecting...');
      if (wsConn) { try { wsConn.terminate(); } catch(e) {} wsConn = null; }
      wsConnected = false;
      connectAngelWS();
    }
  }, 30000);

  // Spot rates refresh every 30s
  setInterval(refreshSpotRates, 30000);

  // RE-check WS every day at midnight IST (new contracts may be needed)
  setInterval(async () => {
    await buildScripCache();
    if (isWeekday()) {
      if (wsConn) { try { wsConn.terminate(); } catch(e) {} wsConn = null; }
      wsConnected = false;
      await connectAngelWS();
    }
  }, 22 * 60 * 60 * 1000);
}

// Keep-alive (Render free tier)
setInterval(() => {
  if (SELF_URL) axios.get(SELF_URL, { timeout: 5000 }).catch(() => {});
}, 13 * 60 * 1000);

app.listen(PORT, () => {
  console.log(`🚀 R.R Jewellers Server v24 on port ${PORT}`);
  startup().catch(e => console.error('❌ startup:', e.message));
});
