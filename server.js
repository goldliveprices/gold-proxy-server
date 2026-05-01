'use strict';

/**
 * R.R Jewellers — Gold & Silver Rates Proxy Server v23
 *
 * THIS VERSION IS TUNED FOR: 24x7 LIVE FEED, ZERO MOCK MCX RATES.
 *
 * Behaviour:
 * - MCX prices (gold/silver) ONLY from Angel One Market Feeds.
 *   • If Angel returns valid quotes → app returns them as-is (LTP, open, high, low, close, bid, ask).
 *   • If Angel fails or returns nothing → app returns gold/silver = null (NO spot fallback).
 * - International spot + derived India-equivalent kept SEPARATE under `spot` and `derivedFromSpot`.
 *   • UI can show these only when you want, but they NEVER replace MCX.
 * - Angel is attempted 24x7 (no holiday/time blocking). If market closed, Angel will typically
 *   return last available OHLC; we still pass it through.
 */

const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const https   = require('https');
const app     = express();

app.use(cors());
app.use(express.json());

// ============================================================
// CONFIG
// ============================================================
const CLIENT_ID      = process.env.CLIENT_ID      || 'AAAA238852';
const API_KEY        = process.env.API_KEY        || 'DPAHMIXr';
const TOTP_SECRET    = process.env.TOTP_SECRET    || 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN      = process.env.ANGEL_PIN      || '1857';
const SHEET_ID       = process.env.SHEET_ID       || '';
const SELF_URL       = process.env.SELF_URL       || 'https://gold-proxy-server.onrender.com/ping';
const PROXY_URL      = process.env.QUOTAGUARDSTATIC_URL || null; // optional
const PREMIUM_FACTOR = parseFloat(process.env.PREMIUM_FACTOR || '1.103');

const MONTHS = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];

// ============================================================
// OPTIONAL PROXY SUPPORT (you are not using it now; leave env empty)
// ============================================================
function makeAxios(useProxy = false) {
  if (useProxy && PROXY_URL) {
    try {
      const u = new URL(PROXY_URL);
      return axios.create({
        proxy: {
          protocol: u.protocol.replace(':', ''),
          host: u.hostname,
          port: parseInt(u.port) || 9293,
          auth: u.username
            ? { username: decodeURIComponent(u.username), password: decodeURIComponent(u.password) }
            : undefined,
        },
      });
    } catch (e) {
      console.warn('⚠️  Invalid QUOTAGUARDSTATIC_URL — using direct connection');
    }
  }
  return axios;
}

// ============================================================
// IST HELPERS
// ============================================================
function getIST() {
  const ist = new Date(Date.now() + (5 * 60 + 30) * 60 * 1000);
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

// Simple weekday flag; we still attempt Angel even if closed/holiday
function isWeekdaySession() {
  const { dow } = getIST();
  return dow !== 0; // Sunday hard closed, rest we still call Angel
}

// Basic known MCX holidays only for information
const MCX_HOLIDAYS = new Set([
  '2026-01-26','2026-03-25','2026-04-02','2026-04-14',
  '2026-04-30','2026-05-01','2026-08-15','2026-10-02',
  '2026-10-20','2026-11-04','2026-12-25',
]);

function isMCXHoliday() {
  const { year, month, day } = getIST();
  const key = `${year}-${String(month + 1).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
  return MCX_HOLIDAYS.has(key);
}

// ============================================================
// SCRIP MASTER CACHE (same as v22)
// ============================================================
let tokenCache   = { GOLD: {}, SILVER: {} };
let cacheBuiltAt = 0;
const CACHE_TTL  = 22 * 60 * 60 * 1000;

async function buildScripCache() {
  try {
    console.log('📋 Fetching ScripMaster...');
    const r = await axios.get(
      'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json',
      { timeout: 30000 }
    );
    if (!Array.isArray(r.data) || r.data.length === 0) {
      console.warn('⚠️  ScripMaster empty — keeping existing cache');
      return false;
    }
    const nextCache = { GOLD: {}, SILVER: {} };
    for (const inst of r.data) {
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

      if (sym.startsWith('GOLD') &&
          !sym.includes('GOLDM') && !sym.includes('GOLDPETAL') &&
          !sym.includes('GOLDGUINEA') && !sym.includes('TEN')) {
        nextCache.GOLD[label] = { symboltoken: String(tok), tradingsymbol: sym };
      }
      if (sym.startsWith('SILVER') && !sym.includes('SILVERM') && !sym.includes('SILVERMIC')) {
        const existing = nextCache.SILVER[label];
        if (!existing || sym.startsWith('SILVER30')) {
          nextCache.SILVER[label] = { symboltoken: String(tok), tradingsymbol: sym };
        }
      }
    }
    tokenCache   = nextCache;
    cacheBuiltAt = Date.now();
    console.log(`✅ Cache — GOLD:${Object.keys(nextCache.GOLD).length} SILVER:${Object.keys(nextCache.SILVER).length}`);
    return true;
  } catch (e) {
    console.error('❌ buildScripCache:', e.message);
    return false;
  }
}

async function ensureCache() {
  if (!cacheBuiltAt || Date.now() - cacheBuiltAt > CACHE_TTL) await buildScripCache();
}

function getDynamicContracts(base) {
  const cache = tokenCache[base] || {};
  const keys  = Object.keys(cache);
  if (keys.length === 0) return [];
  const now    = getIST();
  const curYr2 = parseInt(now.year.toString().slice(-2));
  const curMon = now.month;
  const valid  = keys.filter(k => {
    const mon = MONTHS.indexOf(k.slice(0, 3));
    const yr2 = parseInt(k.slice(3, 5));
    if (yr2 < curYr2) return false;
    if (yr2 === curYr2 && mon < curMon) return false;
    if (yr2 === curYr2 && mon === curMon) {
      const expiry = getExpiryDate(now.year, mon);
      const today  = new Date(Date.UTC(now.year, mon, now.day));
      if (today > expiry) return false;
    }
    return true;
  });
  valid.sort((a, b) => {
    const yrA = parseInt(a.slice(3,5)), monA = MONTHS.indexOf(a.slice(0,3));
    const yrB = parseInt(b.slice(3,5)), monB = MONTHS.indexOf(b.slice(0,3));
    return yrA !== yrB ? yrA - yrB : monA - monB;
  });
  return valid.slice(0, 2);
}

// ============================================================
// TOTP + AUTH
// ============================================================
function generateTOTP(secret, offset = 0) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const v = alpha.indexOf(c);
    if (v >= 0) bits += v.toString(2).padStart(5, '0');
  }
  const bytes = [];
  for (let i = 0; i + 8 <= bits.length; i += 8) bytes.push(parseInt(bits.slice(i, i + 8), 2));
  const key = Buffer.from(bytes);
  const t   = Math.floor(Date.now() / 30000) + offset;
  const tb  = Buffer.alloc(8);
  tb.writeUInt32BE(Math.floor(t / 0x100000000), 0);
  tb.writeUInt32BE(t >>> 0, 4);
  const hmac = crypto.createHmac('sha1', key).update(tb).digest();
  const off  = hmac[hmac.length - 1] & 0x0f;
  const code = (
    ((hmac[off]     & 0x7f) << 24) |
    ((hmac[off + 1] & 0xff) << 16) |
    ((hmac[off + 2] & 0xff) <<  8) |
     (hmac[off + 3] & 0xff)
  ) % 1_000_000;
  return code.toString().padStart(6, '0');
}

let JWT = null, JWT_EXP = 0;

const buildHeaders = (jwt) => ({
  'Content-Type':     'application/json',
  'Accept':           'application/json',
  'User-Agent':       'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
  'X-UserType':       'USER',
  'X-SourceID':       'WEB',
  'X-ClientLocalIP':  '192.168.1.5',
  'X-ClientPublicIP': '74.220.52.100',
  'X-MACAddress':     '00:1A:2B:3C:4D:5E',
  'X-PrivateKey':     API_KEY,
  ...(jwt ? { Authorization: 'Bearer ' + jwt } : {}),
});

async function login() {
  if (JWT && Date.now() < JWT_EXP) return JWT;
  JWT = null; JWT_EXP = 0;
  const axInst  = makeAxios(true);
  let lastError = 'Unknown';
  for (const offset of [-2,-1,0,1,2]) {
    const totp = generateTOTP(TOTP_SECRET, offset);
    try {
      const r = await axInst.post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        { clientcode: CLIENT_ID, password: ANGEL_PIN, totp },
        { headers: buildHeaders(), timeout: 10000 }
      );
      if (r.data?.status === true && r.data?.data?.jwtToken) {
        JWT     = r.data.data.jwtToken;
        JWT_EXP = Date.now() + 6 * 60 * 60 * 1000;
        console.log(`✅ Angel login OK (offset ${offset})`);
        return JWT;
      }
      lastError = r.data?.message || r.data?.errorcode || 'no token';
    } catch (e) {
      const status = e.response?.status;
      if (status === 403) { lastError = '403 forbidden / rate limit'; break; }
      lastError = e.response?.data?.message || e.message;
    }
  }
  throw new Error(`Angel login failed: ${lastError}`);
}

// ============================================================
// MARKET QUOTES (Angel One)
// ============================================================
async function getBulkQuotes(jwt, tokens) {
  const unique = [...new Set(tokens.filter(Boolean).map(String))];
  if (unique.length === 0) return {};
  try {
    const r = await makeAxios(true).post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
      { mode: 'FULL', exchangeTokens: { MCX: unique } },
      { headers: buildHeaders(jwt), timeout: 10000 }
    );
    const out = {};
    for (const d of (r.data?.data?.fetched || [])) {
      out[String(d.symbolToken)] = {
        ltp:  Number(d.ltp)                     || 0,
        bid:  Number(d.depth?.buy?.[0]?.price)  || Number(d.ltp) || 0,
        ask:  Number(d.depth?.sell?.[0]?.price) || Number(d.ltp) || 0,
        high: Number(d.high)                    || 0,
        low:  Number(d.low)                     || 0,
        open: Number(d.open)                    || 0,
        close:Number(d.close)                   || 0,
      };
    }
    return out;
  } catch (e) {
    console.error('getBulkQuotes:', e.message);
    throw e;
  }
}

// ============================================================
// INTERNATIONAL SPOT + FOREX (for reference only)
// ============================================================
async function getSpotRates() {
  try {
    const [goldRes, silverRes] = await Promise.all([
      axios.get('https://api.gold-api.com/price/XAU/INR', { timeout: 7000 }),
      axios.get('https://api.gold-api.com/price/XAG/INR', { timeout: 7000 }),
    ]);
    const xauInrPerOz = goldRes.data?.price;
    const xagInrPerOz = silverRes.data?.price;
    const usdInr      = goldRes.data?.exchangeRate;
    const xauUsd      = xauInrPerOz / usdInr;
    const xagUsd      = xagInrPerOz / usdInr;
    if (xauInrPerOz > 50000 && xagInrPerOz > 500 && usdInr > 70) {
      return { xauUsd, xagUsd, xauInrPerOz, xagInrPerOz, usdInr, src: 'gold-api.com' };
    }
  } catch (e) {}

  try {
    const r = await axios.get(
      'https://api.fxratesapi.com/latest?base=USD&currencies=XAU,XAG,INR&format=json',
      { timeout: 7000 }
    );
    const xauUsd      = r.data?.rates?.XAU ? parseFloat((1 / r.data.rates.XAU).toFixed(2)) : 0;
    const xagUsd      = r.data?.rates?.XAG ? parseFloat((1 / r.data.rates.XAG).toFixed(4)) : 0;
    const usdInr      = r.data?.rates?.INR || 0;
    const xauInrPerOz = xauUsd * usdInr;
    const xagInrPerOz = xagUsd * usdInr;
    if (xauUsd > 1000 && xagUsd > 5 && usdInr > 70) {
      return { xauUsd, xagUsd, xauInrPerOz, xagInrPerOz, usdInr, src: 'fxratesapi' };
    }
  } catch (e) {}

  try {
    const r = await axios.get('https://data-asg.goldprice.org/dbXRates/INR', {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://goldprice.org' },
      timeout: 7000,
    });
    const item        = r.data?.items?.[0];
    const xauInrPerOz = item?.xauPrice;
    const xagInrPerOz = item?.xagPrice;
    const usdInr      = item?.usdInr || 94.97;
    const xauUsd      = xauInrPerOz / usdInr;
    const xagUsd      = xagInrPerOz / usdInr;
    if (xauInrPerOz > 50000 && xagInrPerOz > 500) {
      return { xauUsd, xagUsd, xauInrPerOz, xagInrPerOz, usdInr, src: 'goldprice.org' };
    }
  } catch (e) {}

  const xauUsd = 4644, xagUsd = 76.5, usdInr = 94.97;
  return {
    xauUsd, xagUsd,
    xauInrPerOz: xauUsd * usdInr,
    xagInrPerOz: xagUsd * usdInr,
    usdInr,
    src: 'static_fallback_May2026',
  };
}

async function getForex() {
  try {
    const r = await axios.get('https://api.fxratesapi.com/latest?base=USD&currencies=INR&format=json', { timeout: 6000 });
    if (r.data?.rates?.INR > 70) return r.data.rates.INR;
  } catch (_) {}
  try {
    const r = await axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 6000 });
    if (r.data?.rates?.INR > 70) return r.data.rates.INR;
  } catch (_) {}
  try {
    const r = await axios.get('https://api.exchangerate.host/latest?base=USD&symbols=INR', { timeout: 6000 });
    if (r.data?.rates?.INR > 70) return r.data.rates.INR;
  } catch (_) {}
  return 94.97;
}

function fromSpotToMcx(spot, overrideUsdInr) {
  const usdInr      = overrideUsdInr || spot.usdInr || 94.97;
  const xauInrPerOz = spot.xauInrPerOz || (spot.xauUsd * usdInr);
  const xagInrPerOz = spot.xagInrPerOz || (spot.xagUsd * usdInr);
  const goldPer10g  = Math.round((xauInrPerOz / 31.1035) * 10   * PREMIUM_FACTOR);
  const silverPerKg = Math.round((xagInrPerOz / 31.1035) * 1000 * PREMIUM_FACTOR);
  return { goldPer10g, silverPerKg };
}

function isValidQuote(q) {
  return q && typeof q.ltp === 'number' && q.ltp > 0 && typeof q.high === 'number' && q.high > 0;
}

function roundQuote(q) {
  return q ? {
    ltp:   Math.round(q.ltp),
    bid:   Math.round(q.bid),
    ask:   Math.round(q.ask),
    high:  Math.round(q.high),
    low:   Math.round(q.low),
    open:  Math.round(q.open),
    close: Math.round(q.close || 0),
  } : null;
}

// ============================================================
// ROUTES
// ============================================================
app.get('/', (req, res) => {
  res.json({
    status: 'R.R Jewellers Server ✅',
    version: 'v23',
    weekdaySession: isWeekdaySession(),
    holiday: isMCXHoliday(),
    proxy: PROXY_URL ? 'QuotaGuard ✅' : 'Direct',
    timestamp: new Date().toISOString(),
  });
});

app.get('/ping', (req, res) => res.json({ ok: true }));

// MAIN RATES ENDPOINT — MCX LIVE ONLY, NO MOCK LTP
app.get('/rates', async (req, res) => {
  try {
  // 1) Always fetch international spot + forex (24x7)
  const spotRes = await getSpotRates();
  const usdInr  = (spotRes.usdInr && spotRes.usdInr > 70) ? spotRes.usdInr : await getForex();
  const derived = fromSpotToMcx(spotRes, usdInr);

  // 2) Always TRY Angel MCX (except maybe Sunday)
  let angelError  = null;
  let angelSource = 'not_attempted';
  let gC = [], sC = [], gCurTok = null, gNxtTok = null, sCurTok = null, sNxtTok = null;
  let quotes = {};

  if (isWeekdaySession()) {
    try {
      await ensureCache();
      gC = getDynamicContracts('GOLD');
      sC = getDynamicContracts('SILVER');
      if (gC.length > 0 && sC.length > 0) {
        const jwt = await login();
        gCurTok = tokenCache.GOLD[gC[0]]   || null;
        gNxtTok = gC[1] ? (tokenCache.GOLD[gC[1]]   || null) : null;
        sCurTok = tokenCache.SILVER[sC[0]] || null;
        sNxtTok = sC[1] ? (tokenCache.SILVER[sC[1]] || null) : null;
        const tokensToFetch = [
          gCurTok?.symboltoken,
          sCurTok?.symboltoken,
          gNxtTok?.symboltoken,
          sNxtTok?.symboltoken,
        ].filter(Boolean);
        if (tokensToFetch.length > 0) {
          quotes      = await getBulkQuotes(jwt, tokensToFetch);
          angelSource = 'angel_mcx_attempted';
        } else {
          angelSource = 'no_tokens';
        }
      } else {
        angelSource = 'cache_empty';
      }
    } catch (err) {
      angelError  = err.message;
      angelSource = 'angel_failed';
    }
  } else {
    angelSource = 'sunday_closed';
  }

  const gCurrRaw = gCurTok ? quotes[gCurTok.symboltoken] : null;
  const sCurrRaw = sCurTok ? quotes[sCurTok.symboltoken] : null;
  const gNextRaw = gNxtTok ? quotes[gNxtTok.symboltoken] : null;
  const sNextRaw = sNxtTok ? quotes[sNxtTok.symboltoken] : null;

  const gCurr = isValidQuote(gCurrRaw) ? gCurrRaw : null;
  const sCurr = isValidQuote(sCurrRaw) ? sCurrRaw : null;
  const gNext = isValidQuote(gNextRaw) ? gNextRaw : null;
  const sNext = isValidQuote(sNextRaw) ? sNextRaw : null;

  const source = (gCurr && sCurr) ? 'mcx_live' : 'mcx_unavailable';

  if (gC.length === 0) gC = ['JUN26', 'AUG26'];
  if (sC.length === 0) sC = ['MAY26', 'JUL26'];

  res.json({
    success: true,
    source,
    weekdaySession: isWeekdaySession(),
    isHoliday: isMCXHoliday(),
    proxyActive: !!PROXY_URL,
    spotSource: spotRes.src,
    usdInr: parseFloat(usdInr.toFixed(4)),
    _debug: {
      angelError,
      angelSource,
      goldContract:   gCurTok?.tradingsymbol || null,
      silverContract: sCurTok?.tradingsymbol || null,
    },
    contracts: {
      gold:   { current: gC[0], next: gC[1] || null, symbol: gCurTok?.tradingsymbol || 'GOLD_FUT' },
      silver: { current: sC[0], next: sC[1] || null, symbol: sCurTok?.tradingsymbol || 'SILVER_FUT' },
    },
    // Top-level for index.html direct access
    xauUsd: parseFloat(spotRes.xauUsd.toFixed(2)),
    xagUsd: parseFloat(spotRes.xagUsd.toFixed(4)),
    // usdInr already at top level below
    spot: {
      xauUsd:      parseFloat(spotRes.xauUsd.toFixed(2)),
      xagUsd:      parseFloat(spotRes.xagUsd.toFixed(4)),
      xauInrPerOz: Math.round(spotRes.xauInrPerOz),
      xagInrPerOz: Math.round(spotRes.xagInrPerOz),
      // Day High/Low — passed through from gold-api if available
      xauUsdHigh:  spotRes.xauUsdHigh ? parseFloat(spotRes.xauUsdHigh.toFixed(2)) : null,
      xauUsdLow:   spotRes.xauUsdLow  ? parseFloat(spotRes.xauUsdLow.toFixed(2))  : null,
      xagUsdHigh:  spotRes.xagUsdHigh ? parseFloat(spotRes.xagUsdHigh.toFixed(4)) : null,
      xagUsdLow:   spotRes.xagUsdLow  ? parseFloat(spotRes.xagUsdLow.toFixed(4))  : null,
      usdInrHigh:  spotRes.usdInrHigh ? parseFloat(spotRes.usdInrHigh.toFixed(4)) : null,
      usdInrLow:   spotRes.usdInrLow  ? parseFloat(spotRes.usdInrLow.toFixed(4))  : null,
    },
    derivedFromSpot: {
      goldPer10g:  derived.goldPer10g,
      silverPerKg: derived.silverPerKg,
    },
    // PURE MCX — these are NULL if Angel fails; NO mocked backup
    goldPer10g:  gCurr ? Math.round(gCurr.ltp) : null,
    silverPerKg: sCurr ? Math.round(sCurr.ltp) : null,
    futures: {
      gold:       roundQuote(gCurr),
      silver:     roundQuote(sCurr),
      goldNext:   roundQuote(gNext),
      silverNext: roundQuote(sNext),
    },
    timestamp: new Date().toISOString(),
  });

  } catch (e) {
    console.error('❌ /rates fatal:', e.message);
    return res.status(500).json({ success: false, source: 'error', error: e.message });
  }
});

// Debug + updates endpoints kept same as before
app.get('/debug', async (req, res) => {
  await ensureCache();
  const gC = getDynamicContracts('GOLD');
  const sC = getDynamicContracts('SILVER');
  res.json({
    weekdaySession: isWeekdaySession(),
    isHoliday: isMCXHoliday(),
    proxyActive: !!PROXY_URL,
    proxyUrl: PROXY_URL ? PROXY_URL.replace(/\/\/([^:]+):[^@]+@/, '//$1:***@') : null,
    cacheAge: cacheBuiltAt ? `${Math.round((Date.now()-cacheBuiltAt)/60000)}m ago` : 'not built',
    contracts: { gold: gC, silver: sC },
    goldToken:   gC[0] ? (tokenCache.GOLD[gC[0]]   || null) : null,
    silverToken: sC[0] ? (tokenCache.SILVER[sC[0]] || null) : null,
    cacheSize: { gold: Object.keys(tokenCache.GOLD).length, silver: Object.keys(tokenCache.SILVER).length },
  });
});

app.get('/updates', async (req, res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID not set');
    const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r   = await axios.get(url, { timeout: 8000 });
    const jsonStr = r.data.replace(/^[^(]*\((.+)\);\s*$/s, '$1');
    const data = JSON.parse(jsonStr);
    const rows = data.table.rows.map(row => ({
      date: row.c[0]?.v || '',
      title: row.c[1]?.v || '',
      content: row.c[2]?.v || '',
      image: row.c[3]?.v || '',
    }));
    res.json({ success: true, updates: rows.filter(r => r.title) });
  } catch (e) {
    res.json({ success: true, updates: [{ title: 'Welcome to R.R. Jewellers', content: 'Your trusted jewellers in Etawah.' }] });
  }
});

// ============================================================
// START SERVER
// ============================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log('═══════════════════════════════════════════');
  console.log(' R.R Jewellers Gold Server v23             ');
  console.log('═══════════════════════════════════════════');
  console.log(`🚀 Port: ${PORT} | Proxy: ${PROXY_URL ? 'QuotaGuard ✅' : 'Direct'}`);
  await buildScripCache();
  setInterval(buildScripCache, 12 * 60 * 60 * 1000);
  const pingUrl = SELF_URL.startsWith('https') ? SELF_URL : 'https://gold-proxy-server.onrender.com/ping';
  setInterval(() => { https.get(pingUrl, () => {}).on('error', () => {}); }, 4 * 60 * 1000);
});

