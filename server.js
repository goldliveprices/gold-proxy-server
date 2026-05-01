'use strict';

const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const http    = require('http');
const https   = require('https');
const app     = express();

app.use(cors());
app.use(express.json());

// ============================================================
// CONFIG — Move sensitive values to Render Environment Variables
// ============================================================
const CLIENT_ID   = process.env.CLIENT_ID   || 'AAAA238852';
const API_KEY     = process.env.API_KEY      || 'DPAHMIXr';
const TOTP_SECRET = process.env.TOTP_SECRET  || 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN   = process.env.ANGEL_PIN    || '1857';
const SHEET_ID    = process.env.SHEET_ID     || '';
const SELF_URL    = process.env.SELF_URL     || 'https://gold-proxy-server.onrender.com/ping';

// QuotaGuard Static IP proxy (set QUOTAGUARDSTATIC_URL in Render env vars)
// Format: http://user:pass@static.quotaguard.com:9293
const PROXY_URL   = process.env.QUOTAGUARDSTATIC_URL || null;

const MONTHS = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];

// ============================================================
// PROXY-AWARE AXIOS INSTANCE (routes Angel One calls via static IP)
// ============================================================
function makeAxios(useProxy = false) {
  if (useProxy && PROXY_URL) {
    try {
      const url  = new URL(PROXY_URL);
      const agent = new (url.protocol === 'https:' ? https : http).Agent({ keepAlive: true });
      return axios.create({
        proxy: {
          protocol: url.protocol.replace(':', ''),
          host:     url.hostname,
          port:     parseInt(url.port) || 9293,
          auth:     url.username ? { username: url.username, password: url.password } : undefined,
        },
        httpAgent:  agent,
        httpsAgent: agent,
      });
    } catch (e) {
      console.warn('⚠️  Invalid QUOTAGUARDSTATIC_URL, falling back to direct connection');
    }
  }
  return axios;
}

// ============================================================
// IST TIME HELPERS
// ============================================================
function getIST() {
  const now = new Date();
  const ist = new Date(now.getTime() + (5 * 60 + 30) * 60 * 1000);
  return {
    year:  ist.getUTCFullYear(),
    month: ist.getUTCMonth(),
    day:   ist.getUTCDate(),
    hour:  ist.getUTCHours(),
    min:   ist.getUTCMinutes(),
    dow:   ist.getUTCDay(),
  };
}

function getExpiryDate(year, month) {
  const fifth = new Date(Date.UTC(year, month, 5));
  const dow   = fifth.getUTCDay();
  if (dow === 0) return new Date(Date.UTC(year, month, 3));
  if (dow === 6) return new Date(Date.UTC(year, month, 4));
  return fifth;
}

function isMCXOpen() {
  const { dow, hour, min } = getIST();
  if (dow === 0 || dow === 6) return false;
  const timeM = hour * 60 + min;
  return timeM >= 9 * 60 && timeM < 23 * 60 + 55;
}

// ============================================================
// MCX HOLIDAY LIST (IST) — Add dates as needed (YYYY-MM-DD)
// ============================================================
const MCX_HOLIDAYS = new Set([
  '2026-01-26', '2026-03-25', '2026-04-02', '2026-04-14',
  '2026-04-30', '2026-05-01', '2026-08-15', '2026-10-02',
  '2026-10-20', '2026-11-04', '2026-12-25',
]);

function isMCXHoliday() {
  const { year, month, day } = getIST();
  const key = `${year}-${String(month + 1).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
  return MCX_HOLIDAYS.has(key);
}

// ============================================================
// SCRIP MASTER TOKEN CACHE
// ============================================================
let tokenCache   = { GOLD: {}, SILVER: {} };
let cacheBuiltAt = 0;
const CACHE_TTL  = 22 * 60 * 60 * 1000;

async function buildScripCache() {
  try {
    console.log('📋 Building ScripMaster cache...');
    const r = await axios.get(
      'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json',
      { timeout: 30000 }
    );
    const instruments = r.data;
    if (!Array.isArray(instruments) || instruments.length === 0) {
      console.warn('⚠️  ScripMaster returned empty array');
      return false;
    }

    const newCache = { GOLD: {}, SILVER: {} };

    for (const inst of instruments) {
      if (inst.exch_seg !== 'MCX' || inst.instrumenttype !== 'FUTCOM') continue;
      const sym = (inst.symbol  || '').toUpperCase().trim();
      const tok =  inst.token;
      const exp = (inst.expiry  || '').toUpperCase().trim();
      if (!sym || !tok) continue;

      let label = null;
      const expMatch = exp.match(/([A-Z]{3})20(\d{2})/);
      if (expMatch) {
        label = expMatch[1] + expMatch[2];
      } else {
        const symMatch = sym.match(/([A-Z]{3})(\d{2})/);
        if (symMatch) label = symMatch[1] + symMatch[2];
      }
      if (!label || !MONTHS.includes(label.slice(0, 3))) continue;

      // GOLD: exclude mini/petal/guinea/ten-gram variants
      if (
        sym.startsWith('GOLD') &&
        !sym.includes('GOLDM') &&
        !sym.includes('GOLDPETAL') &&
        !sym.includes('GOLDGUINEA') &&
        !sym.includes('TEN')
      ) {
        newCache['GOLD'][label] = { symboltoken: tok, tradingsymbol: sym };
      }

      // SILVER: prefer SILVER30, exclude mini/micro
      if (sym.startsWith('SILVER') && !sym.includes('SILVERM') && !sym.includes('SILVERMIC')) {
        const existing = newCache['SILVER'][label];
        if (!existing || sym.startsWith('SILVER30')) {
          newCache['SILVER'][label] = { symboltoken: tok, tradingsymbol: sym };
        }
      }
    }

    tokenCache   = newCache;
    cacheBuiltAt = Date.now();
    console.log(`✅ Cache built — GOLD: ${Object.keys(newCache.GOLD).length} contracts, SILVER: ${Object.keys(newCache.SILVER).length} contracts`);
    return true;
  } catch (e) {
    console.error('❌ buildScripCache failed:', e.message);
    return false;
  }
}

async function ensureCache() {
  if (cacheBuiltAt === 0 || Date.now() - cacheBuiltAt > CACHE_TTL) {
    await buildScripCache();
  }
}

function getDynamicContracts(base) {
  const cache = tokenCache[base] || {};
  const keys  = Object.keys(cache);
  if (keys.length === 0) return [];

  const now        = getIST();
  const currentYr  = parseInt(now.year.toString().slice(-2));
  const currentMon = now.month;

  const valid = keys.filter(k => {
    const m = MONTHS.indexOf(k.slice(0, 3));
    const y = parseInt(k.slice(3, 5));
    if (y < currentYr) return false;
    if (y === currentYr && m < currentMon) return false;
    if (y === currentYr && m === currentMon) {
      const expiry = getExpiryDate(now.year, m);
      const today  = new Date(Date.UTC(now.year, m, now.day));
      if (today > expiry) return false;
    }
    return true;
  });

  valid.sort((a, b) => {
    const yrA  = parseInt(a.slice(3, 5));
    const monA = MONTHS.indexOf(a.slice(0, 3));
    const yrB  = parseInt(b.slice(3, 5));
    const monB = MONTHS.indexOf(b.slice(0, 3));
    return yrA !== yrB ? yrA - yrB : monA - monB;
  });

  return valid.slice(0, 2);
}

// ============================================================
// TOTP GENERATOR
// ============================================================
function generateTOTP(secret, offset = 0) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const v = alpha.indexOf(c);
    if (v >= 0) bits += v.toString(2).padStart(5, '0');
  }
  const bytes = [];
  for (let i = 0; i + 8 <= bits.length; i += 8) {
    bytes.push(parseInt(bits.slice(i, i + 8), 2));
  }
  const key = Buffer.from(bytes);
  const t   = Math.floor(Date.now() / 1000 / 30) + offset;
  const tb  = Buffer.alloc(8);
  tb.writeUInt32BE(Math.floor(t / 0x100000000), 0);
  tb.writeUInt32BE(t >>> 0, 4);
  const hmac = crypto.createHmac('sha1', key).update(tb).digest();
  const off  = hmac[hmac.length - 1] & 0xf;
  const code = (
    ((hmac[off]     & 0x7f) << 24) |
    ((hmac[off + 1] & 0xff) << 16) |
    ((hmac[off + 2] & 0xff) <<  8) |
     (hmac[off + 3] & 0xff)
  ) % 1_000_000;
  return code.toString().padStart(6, '0');
}

// ============================================================
// ANGEL ONE AUTH — Routes through QuotaGuard proxy if available
// ============================================================
let JWT = null, JWT_EXP = 0;

const buildHeaders = (jwt) => ({
  'Content-Type':     'application/json',
  'Accept':           'application/json',
  'User-Agent':       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'X-UserType':       'USER',
  'X-SourceID':       'WEB',
  'X-ClientLocalIP':  '192.168.1.5',
  'X-ClientPublicIP': '106.193.155.12',
  'X-MACAddress':     '00:1A:2B:3C:4D:5E',
  'X-PrivateKey':     API_KEY,
  ...(jwt ? { Authorization: 'Bearer ' + jwt } : {}),
});

async function login() {
  if (JWT && Date.now() < JWT_EXP) return JWT;
  JWT = null; JWT_EXP = 0;

  const axiosProxy = makeAxios(true); // Use static IP proxy for Angel One

  for (const w of [-4, -3, -2, -1, 0, 1, 2, 3, 4]) {
    const pin = generateTOTP(TOTP_SECRET, w);
    try {
      const r = await axiosProxy.post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        { clientcode: CLIENT_ID, password: ANGEL_PIN, totp: pin },
        { headers: buildHeaders(), timeout: 10000 }
      );
      if (r.data?.status && r.data?.data?.jwtToken) {
        JWT     = r.data.data.jwtToken;
        JWT_EXP = Date.now() + 6 * 60 * 60 * 1000;
        console.log('✅ Angel One login successful');
        return JWT;
      }
    } catch (e) {
      const status = e.response?.status;
      if (status === 403) {
        console.error(`❌ Angel One 403 Forbidden — WAF IP block detected. Set QUOTAGUARDSTATIC_URL env var.`);
        throw new Error('403 Forbidden: IP blocked by Angel One WAF. Configure QuotaGuard static IP.');
      }
      if (status === 401) {
        console.warn(`⚠️  TOTP offset ${w} rejected (401), trying next...`);
        continue;
      }
      if (e.response) throw new Error(`Angel auth HTTP ${status}: ${e.response.statusText}`);
    }
  }
  throw new Error('Angel auth failed: All TOTP offsets exhausted');
}

// ============================================================
// MARKET DATA FETCHERS
// ============================================================
async function getBulkQuotes(jwt, tokens) {
  const validTokens = [...new Set(tokens.filter(Boolean).map(String))];
  if (validTokens.length === 0) return {};

  const axiosProxy = makeAxios(true);
  try {
    const r = await axiosProxy.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
      { mode: 'FULL', exchangeTokens: { MCX: validTokens } },
      { headers: buildHeaders(jwt), timeout: 10000 }
    );
    const fetched = r.data?.data?.fetched || [];
    const results = {};
    fetched.forEach(d => {
      results[String(d.symbolToken)] = {
        ltp:  Number(d.ltp)                       || 0,
        bid:  Number(d.depth?.buy?.[0]?.price)    || Number(d.ltp) || 0,
        ask:  Number(d.depth?.sell?.[0]?.price)   || Number(d.ltp) || 0,
        high: Number(d.high)                      || 0,
        low:  Number(d.low)                       || 0,
        open: Number(d.open)                      || 0,
      };
    });
    return results;
  } catch (e) {
    console.error('getBulkQuotes error:', e.message);
    return null;
  }
}

// ============================================================
// SPOT PRICE FALLBACK (International Gold/Silver + USD/INR)
// ============================================================
async function getSpotRates() {
  // Source 1: fxratesapi
  try {
    const r = await axios.get(
      'https://api.fxratesapi.com/latest?base=USD&currencies=XAU,XAG&format=json',
      { timeout: 7000 }
    );
    const xauUsd = r.data?.rates?.XAU ? parseFloat((1 / r.data.rates.XAU).toFixed(2)) : 0;
    const xagUsd = r.data?.rates?.XAG ? parseFloat((1 / r.data.rates.XAG).toFixed(4)) : 0;
    if (xauUsd > 2000 && xagUsd > 15) {
      console.log(`📡 Spot from fxratesapi — XAU: $${xauUsd}, XAG: $${xagUsd}`);
      return { xauUsd, xagUsd, src: 'fxratesapi' };
    }
  } catch (_) {}

  // Source 2: goldprice.org
  try {
    const r = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
      headers: { 'User-Agent': 'Mozilla/5.0' },
      timeout: 7000,
    });
    const gold   = r.data?.items?.[0]?.xauPrice;
    const silver = r.data?.items?.[0]?.xagPrice;
    if (gold > 2000 && silver > 15) {
      console.log(`📡 Spot from goldprice.org — XAU: $${gold}, XAG: $${silver}`);
      return { xauUsd: gold, xagUsd: silver, src: 'goldprice.org' };
    }
  } catch (_) {}

  // Source 3: metals.dev (free tier)
  try {
    const r = await axios.get('https://api.metals.dev/v1/latest?api_key=&unit=toz&currency=USD', { timeout: 7000 });
    const gold   = r.data?.metals?.gold;
    const silver = r.data?.metals?.silver;
    if (gold > 2000 && silver > 15) {
      console.log(`📡 Spot from metals.dev — XAU: $${gold}, XAG: $${silver}`);
      return { xauUsd: gold, xagUsd: silver, src: 'metals.dev' };
    }
  } catch (_) {}

  // Static fallback (update monthly)
  console.warn('⚠️  All spot APIs failed — using hardcoded fallback rates');
  return { xauUsd: 3340, xagUsd: 33.5, src: 'static_fallback_May2026' };
}

async function getForex() {
  // Source 1: Frankfurter
  try {
    const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 6000 });
    if (r.data?.rates?.INR > 70) return r.data.rates.INR;
  } catch (_) {}

  // Source 2: open.er-api
  try {
    const r = await axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 6000 });
    if (r.data?.rates?.INR > 70) return r.data.rates.INR;
  } catch (_) {}

  // Source 3: exchangerate.host
  try {
    const r = await axios.get('https://api.exchangerate.host/latest?base=USD&symbols=INR', { timeout: 6000 });
    if (r.data?.rates?.INR > 70) return r.data.rates.INR;
  } catch (_) {}

  console.warn('⚠️  All forex APIs failed — using hardcoded fallback USD/INR');
  return 84.50;
}

// MCX Equivalent formula:
// Gold  (10g): (xauUsd / 31.1035) * 10   * usdInr * premiumFactor
// Silver(1kg): (xagUsd / 31.1035) * 1000 * usdInr * premiumFactor
// premiumFactor = 1.103 (approx 10.3% customs + GST) — update via env var if budget changes
const PREMIUM_FACTOR = parseFloat(process.env.PREMIUM_FACTOR || '1.103');

function calcBackup(spotData, usdInr) {
  const goldPer10g  = Math.round((spotData.xauUsd / 31.1035) * 10   * usdInr * PREMIUM_FACTOR);
  const silverPerKg = Math.round((spotData.xagUsd / 31.1035) * 1000 * usdInr * PREMIUM_FACTOR);
  return { goldPer10g, silverPerKg };
}

// ============================================================
// API ROUTES
// ============================================================

// Health check / keep-alive ping
app.get('/', (req, res) => {
  res.json({
    status:    'R.R Jewellers Server is Running ✅',
    version:   'v20-quotaguard',
    mcxOpen:   isMCXOpen(),
    holiday:   isMCXHoliday(),
    proxyMode: PROXY_URL ? 'QuotaGuard Static IP ✅' : 'Direct (No Proxy) ⚠️',
    timestamp: new Date().toISOString(),
  });
});

// Lightweight keep-alive endpoint (no heavy logic)
app.get('/ping', (req, res) => res.json({ ok: true, t: Date.now() }));

// Main rates endpoint
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  const isHoliday  = isMCXHoliday();

  // Always fetch spot + forex in parallel (fast, no auth needed)
  const [spotRes, forexRes] = await Promise.allSettled([getSpotRates(), getForex()]);
  const spotData = spotRes.status  === 'fulfilled' ? spotRes.value  : { xauUsd: 3340, xagUsd: 33.5, src: 'error' };
  const usdInr   = forexRes.status === 'fulfilled' ? forexRes.value : 84.50;
  const backup   = calcBackup(spotData, usdInr);

  let quotes         = {};
  let gCurTok = null, gNxtTok = null, sCurTok = null, sNxtTok = null;
  let gC = [], sC = [];
  let angelError     = null;
  let angelSource    = 'not_attempted';

  // Only call Angel One if market is open and not a holiday
  if (marketOpen && !isHoliday) {
    try {
      await ensureCache();
      gC = getDynamicContracts('GOLD');
      sC = getDynamicContracts('SILVER');

      if (gC.length > 0 && sC.length > 0) {
        const jwt = await login();

        [gCurTok, gNxtTok, sCurTok, sNxtTok] = await Promise.all([
          tokenCache['GOLD'][gC[0]]   || null,
          gC[1] ? (tokenCache['GOLD'][gC[1]]   || null) : null,
          tokenCache['SILVER'][sC[0]] || null,
          sC[1] ? (tokenCache['SILVER'][sC[1]] || null) : null,
        ]);

        const tokensToFetch = [
          gCurTok?.symboltoken,
          sCurTok?.symboltoken,
          gNxtTok?.symboltoken,
          sNxtTok?.symboltoken,
        ].filter(Boolean);

        if (tokensToFetch.length > 0) {
          quotes      = await getBulkQuotes(jwt, tokensToFetch) || {};
          angelSource = 'angel_mcx_live';
        }
      }
    } catch (err) {
      angelError  = err.message;
      angelSource = 'angel_failed_spot_backup';
      console.error('Angel One error:', angelError);
    }
  } else {
    angelSource = isHoliday ? 'market_holiday_spot_backup' : 'market_closed_spot_backup';
  }

  // Resolve final prices — prefer live Angel data, fall back to spot calc
  const gCurrRaw = gCurTok ? quotes[String(gCurTok.symboltoken)] : null;
  const sCurrRaw = sCurTok ? quotes[String(sCurTok.symboltoken)] : null;
  const gNextRaw = gNxtTok ? quotes[String(gNxtTok.symboltoken)] : null;
  const sNextRaw = sNxtTok ? quotes[String(sNxtTok.symboltoken)] : null;

  const isValidLtp = (q) => q && typeof q.ltp === 'number' && q.ltp > 0;

  let usedBackup = false;

  let gCurr, sCurr;
  if (isValidLtp(gCurrRaw)) {
    gCurr = gCurrRaw;
  } else {
    usedBackup = true;
    gCurr = { ltp: backup.goldPer10g, bid: backup.goldPer10g, ask: backup.goldPer10g, high: backup.goldPer10g, low: backup.goldPer10g, open: backup.goldPer10g };
  }
  if (isValidLtp(sCurrRaw)) {
    sCurr = sCurrRaw;
  } else {
    usedBackup = true;
    sCurr = { ltp: backup.silverPerKg, bid: backup.silverPerKg, ask: backup.silverPerKg, high: backup.silverPerKg, low: backup.silverPerKg, open: backup.silverPerKg };
  }

  // Next-month contract: use live data or estimate from current + typical spread
  const gNext = isValidLtp(gNextRaw)
    ? gNextRaw
    : { ltp: gCurr.ltp + 150, bid: gCurr.bid + 150, ask: gCurr.ask + 150, high: gCurr.high + 150, low: gCurr.low + 150 };
  const sNext = isValidLtp(sNextRaw)
    ? sNextRaw
    : { ltp: sCurr.ltp + 500, bid: sCurr.bid + 500, ask: sCurr.ask + 500, high: sCurr.high + 500, low: sCurr.low + 500 };

  // Fallback contract labels if cache empty
  if (gC.length === 0) gC = ['JUN26', 'AUG26'];
  if (sC.length === 0) sC = ['MAY26', 'JUL26'];

  const payload = {
    success:    true,
    source:     usedBackup ? angelSource : 'angel_mcx_live',
    marketOpen,
    isHoliday,
    proxyActive: !!PROXY_URL,
    spotSource: spotData.src,
    usdInr,
    _debug: {
      angelError,
      goldContract:   gCurTok?.tradingsymbol  || null,
      silverContract: sCurTok?.tradingsymbol  || null,
    },
    contracts: {
      gold:   { current: gC[0], next: gC[1] || null, symbol: gCurTok?.tradingsymbol  || 'GOLD_BACKUP' },
      silver: { current: sC[0], next: sC[1] || null, symbol: sCurTok?.tradingsymbol  || 'SILVER_BACKUP' },
    },
    spot: { xauUsd: spotData.xauUsd, xagUsd: spotData.xagUsd },
    goldPer10g:  Math.round(gCurr.ltp),
    silverPerKg: Math.round(sCurr.ltp),
    futures: {
      gold:        roundQuote(gCurr),
      silver:      roundQuote(sCurr),
      goldNext:    roundQuote(gNext),
      silverNext:  roundQuote(sNext),
    },
    timestamp: new Date().toISOString(),
  };

  return res.json(payload);
});

function roundQuote(q) {
  return {
    ltp:  Math.round(q.ltp),
    bid:  Math.round(q.bid),
    ask:  Math.round(q.ask),
    high: Math.round(q.high),
    low:  Math.round(q.low),
  };
}

// Debug endpoint — shows cache state
app.get('/debug', async (req, res) => {
  await ensureCache();
  const gC = getDynamicContracts('GOLD');
  const sC = getDynamicContracts('SILVER');
  res.json({
    mcxOpen:     isMCXOpen(),
    isHoliday:   isMCXHoliday(),
    proxyActive: !!PROXY_URL,
    proxyUrl:    PROXY_URL ? PROXY_URL.replace(/:[^:@]*@/, ':***@') : null, // mask password
    cacheAge:    cacheBuiltAt ? `${Math.round((Date.now() - cacheBuiltAt) / 60000)} min ago` : 'not built',
    contracts: { gold: gC, silver: sC },
    goldToken:   gC[0] ? (tokenCache['GOLD'][gC[0]]   || null) : null,
    silverToken: sC[0] ? (tokenCache['SILVER'][sC[0]] || null) : null,
    cacheSize: {
      gold:   Object.keys(tokenCache.GOLD   || {}).length,
      silver: Object.keys(tokenCache.SILVER || {}).length,
    },
  });
});

// Updates from Google Sheet
app.get('/updates', async (req, res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID env not set');
    const url  = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r    = await axios.get(url, { timeout: 8000 });
    const json = r.data.replace(/^[^{]*({.*})[^}]*$/s, '$1');
    const data = JSON.parse(json);
    const rows = data.table.rows.map(row => ({
      date:    row.c[0]?.v || '',
      title:   row.c[1]?.v || '',
      content: row.c[2]?.v || '',
      image:   row.c[3]?.v || '',
    }));
    res.json({ success: true, updates: rows.filter(r => r.title) });
  } catch (e) {
    res.json({ success: true, updates: [{ title: 'Welcome to R.R. Jewellers', content: 'Your trusted jewellers in Etawah.' }] });
  }
});

// ============================================================
// SERVER STARTUP
// ============================================================
const PORT = process.env.PORT || 3000;

app.listen(PORT, async () => {
  console.log('═══════════════════════════════════════════════');
  console.log(' R.R Jewellers Gold Server v20 — QuotaGuard ');
  console.log('═══════════════════════════════════════════════');
  console.log(`🚀 Listening on port ${PORT}`);
  console.log(`🔗 Proxy: ${PROXY_URL ? 'QuotaGuard Static IP ✅' : 'None (Direct) ⚠️'}`);

  // Build scrip cache on startup
  await buildScripCache();

  // Refresh cache every 12 hours
  setInterval(buildScripCache, 12 * 60 * 60 * 1000);

  // Keep Render free tier awake — ping every 4 minutes
  setInterval(() => {
    https.get(SELF_URL.startsWith('https') ? SELF_URL : `https://gold-proxy-server.onrender.com/ping`, () => {})
         .on('error', () => {});
  }, 4 * 60 * 1000);
});
