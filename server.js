'use strict';

/**
 * R.R Jewellers — Gold & Silver Rates Proxy Server v21
 * 
 * BUGS FIXED from v20:
 * 1. [CRITICAL] 403 in login() threw immediately, stopping all 9 TOTP retries — now continues
 * 2. [BUG] source field showed wrong value when Angel returned data but LTP=0 (backup kicked in)
 * 3. [BUG] Promise.all on non-promises for token lookup — replaced with direct sync assignment
 * 4. [BUG] Google Sheets regex was fragile — fixed to match Angel's actual response format
 * 5. [BUG] login() silently swallowed "status=true but no jwtToken" — now logs & continues
 * 6. [INFO] Angel One April 1 2026 rule: Static IP mandatory ONLY for ORDER APIs, NOT for
 *           market data/login. So 403 on login = API key issue, NOT IP block.
 */

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
// CONFIG — Set these in Render → Environment Variables
// ============================================================
const CLIENT_ID      = process.env.CLIENT_ID      || 'AAAA238852';
const API_KEY        = process.env.API_KEY         || 'DPAHMIXr';
const TOTP_SECRET    = process.env.TOTP_SECRET     || 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN      = process.env.ANGEL_PIN       || '1857';
const SHEET_ID       = process.env.SHEET_ID        || '';
const SELF_URL       = process.env.SELF_URL        || 'https://gold-proxy-server.onrender.com/ping';
const PROXY_URL      = process.env.QUOTAGUARDSTATIC_URL || null; // optional
const PREMIUM_FACTOR = parseFloat(process.env.PREMIUM_FACTOR || '1.103');

const MONTHS = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];

// ============================================================
// PROXY-AWARE AXIOS (only used if QUOTAGUARDSTATIC_URL is set)
// ============================================================
function makeAxios(useProxy = false) {
  if (useProxy && PROXY_URL) {
    try {
      const u = new URL(PROXY_URL);
      return axios.create({
        proxy: {
          protocol: u.protocol.replace(':', ''),
          host:     u.hostname,
          port:     parseInt(u.port) || 9293,
          auth:     u.username ? { username: decodeURIComponent(u.username), password: decodeURIComponent(u.password) } : undefined,
        },
      });
    } catch (e) {
      console.warn('⚠️  Invalid QUOTAGUARDSTATIC_URL — using direct connection');
    }
  }
  return axios;
}

// ============================================================
// IST TIME HELPERS
// ============================================================
function getIST() {
  const ist = new Date(Date.now() + (5 * 60 + 30) * 60 * 1000);
  return {
    year:  ist.getUTCFullYear(),
    month: ist.getUTCMonth(),       // 0-indexed
    day:   ist.getUTCDate(),
    hour:  ist.getUTCHours(),
    min:   ist.getUTCMinutes(),
    dow:   ist.getUTCDay(),         // 0=Sun, 6=Sat
  };
}

function getExpiryDate(year, month) {
  // MCX expiry = 5th of month, or prior Friday/Thursday if weekend
  const fifth = new Date(Date.UTC(year, month, 5));
  const dow   = fifth.getUTCDay();
  if (dow === 0) return new Date(Date.UTC(year, month, 3)); // Sun → Fri
  if (dow === 6) return new Date(Date.UTC(year, month, 4)); // Sat → Fri
  return fifth;
}

function isMCXOpen() {
  const { dow, hour, min } = getIST();
  if (dow === 0 || dow === 6) return false;
  const t = hour * 60 + min;
  return t >= 9 * 60 && t < 23 * 60 + 55;
}

// MCX Holidays 2026 — update annually
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
    console.log('📋 Fetching ScripMaster...');
    const r = await axios.get(
      'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json',
      { timeout: 30000 }
    );
    if (!Array.isArray(r.data) || r.data.length === 0) {
      console.warn('⚠️  ScripMaster empty — keeping existing cache');
      return false;
    }

    const newCache = { GOLD: {}, SILVER: {} };

    for (const inst of r.data) {
      if (inst.exch_seg !== 'MCX' || inst.instrumenttype !== 'FUTCOM') continue;
      const sym = (inst.symbol || '').toUpperCase().trim();
      const tok =  inst.token;
      const exp = (inst.expiry || '').toUpperCase().trim();
      if (!sym || !tok) continue;

      // Parse label from expiry field first, fall back to symbol name
      let label = null;
      const expMatch = exp.match(/([A-Z]{3})20(\d{2})/);
      if (expMatch) {
        label = expMatch[1] + expMatch[2];
      } else {
        const symMatch = sym.match(/([A-Z]{3})(\d{2})/);
        if (symMatch) label = symMatch[1] + symMatch[2];
      }
      if (!label || !MONTHS.includes(label.slice(0, 3))) continue;

      // GOLD 1kg contract only (exclude Mini, Petal, Guinea, TenGram)
      if (sym.startsWith('GOLD') &&
          !sym.includes('GOLDM') && !sym.includes('GOLDPETAL') &&
          !sym.includes('GOLDGUINEA') && !sym.includes('TEN')) {
        newCache.GOLD[label] = { symboltoken: String(tok), tradingsymbol: sym };
      }

      // SILVER 30kg contract (prefer SILVER30 prefix, exclude Mini/Micro)
      if (sym.startsWith('SILVER') && !sym.includes('SILVERM') && !sym.includes('SILVERMIC')) {
        const existing = newCache.SILVER[label];
        if (!existing || sym.startsWith('SILVER30')) {
          newCache.SILVER[label] = { symboltoken: String(tok), tradingsymbol: sym };
        }
      }
    }

    tokenCache   = newCache;
    cacheBuiltAt = Date.now();
    console.log(`✅ Cache ready — GOLD: ${Object.keys(newCache.GOLD).length}, SILVER: ${Object.keys(newCache.SILVER).length}`);
    return true;
  } catch (e) {
    console.error('❌ buildScripCache:', e.message);
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

  const now       = getIST();
  const curYr2    = parseInt(now.year.toString().slice(-2));
  const curMon    = now.month; // 0-indexed

  const valid = keys.filter(k => {
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
    const yrA  = parseInt(a.slice(3, 5));
    const monA = MONTHS.indexOf(a.slice(0, 3));
    const yrB  = parseInt(b.slice(3, 5));
    const monB = MONTHS.indexOf(b.slice(0, 3));
    return yrA !== yrB ? yrA - yrB : monA - monB;
  });

  return valid.slice(0, 2);
}

// ============================================================
// TOTP GENERATOR (RFC 6238 / HOTP)
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

// ============================================================
// ANGEL ONE AUTH
// FIXED: 403 no longer immediately throws — loop continues to try all TOTP offsets
// Note (Apr 2026): Static IP is ONLY mandatory for ORDER APIs, NOT for login/market data
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

  const axInst  = makeAxios(true);
  let lastError = 'Unknown error';
  let got403    = false;

  for (const offset of [-4, -3, -2, -1, 0, 1, 2, 3, 4]) {
    const totp = generateTOTP(TOTP_SECRET, offset);
    try {
      const r = await axInst.post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        { clientcode: CLIENT_ID, password: ANGEL_PIN, totp },
        { headers: buildHeaders(), timeout: 10000 }
      );

      // SUCCESS path
      if (r.data?.status === true && r.data?.data?.jwtToken) {
        JWT     = r.data.data.jwtToken;
        JWT_EXP = Date.now() + 6 * 60 * 60 * 1000;
        console.log(`✅ Angel One login OK (TOTP offset ${offset})`);
        return JWT;
      }

      // Angel returned 200 but no token — log it and keep trying
      const msg = r.data?.message || r.data?.errorcode || JSON.stringify(r.data).slice(0, 80);
      console.warn(`⚠️  Login offset ${offset}: status=${r.data?.status}, msg=${msg}`);
      lastError = `status false: ${msg}`;

    } catch (e) {
      const status = e.response?.status;
      const msg    = e.response?.data?.message || e.message;

      if (status === 403) {
        // FIX: do NOT throw immediately — keep trying other TOTP offsets
        got403    = true;
        lastError = `403 Forbidden (offset ${offset})`;
        console.warn(`⚠️  403 on offset ${offset} — continuing retries...`);
        continue;
      }
      if (status === 429) {
        lastError = 'Rate limited (429) — too many login attempts';
        console.error('❌ Angel One rate limit hit');
        break; // no point retrying
      }

      lastError = `HTTP ${status || 'network'}: ${msg}`;
      console.warn(`⚠️  Login offset ${offset} error: ${lastError}`);
    }
  }

  // After all retries exhausted
  if (got403) {
    throw new Error(
      '403 Forbidden on all TOTP offsets. Possible causes: ' +
      '(1) API Key invalid/expired — create new key at smartapi.angelone.in, ' +
      '(2) Account deactivated, ' +
      '(3) IP blocked — set QUOTAGUARDSTATIC_URL env var with a static IP proxy.'
    );
  }
  throw new Error(`Angel One login failed: ${lastError}`);
}

// ============================================================
// MARKET QUOTES
// ============================================================
async function getBulkQuotes(jwt, tokens) {
  const valid = [...new Set(tokens.filter(Boolean).map(String))];
  if (valid.length === 0) return {};

  const axInst = makeAxios(true);
  try {
    const r = await axInst.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
      { mode: 'FULL', exchangeTokens: { MCX: valid } },
      { headers: buildHeaders(jwt), timeout: 10000 }
    );
    const fetched = r.data?.data?.fetched || [];
    const results = {};
    for (const d of fetched) {
      results[String(d.symbolToken)] = {
        ltp:  Number(d.ltp)                     || 0,
        bid:  Number(d.depth?.buy?.[0]?.price)  || Number(d.ltp) || 0,
        ask:  Number(d.depth?.sell?.[0]?.price) || Number(d.ltp) || 0,
        high: Number(d.high)                    || 0,
        low:  Number(d.low)                     || 0,
        open: Number(d.open)                    || 0,
      };
    }
    return results;
  } catch (e) {
    console.error('getBulkQuotes error:', e.message);
    return null;
  }
}

// ============================================================
// SPOT PRICE SOURCES (3 fallbacks)
// ============================================================
async function getSpotRates() {
  try {
    const r = await axios.get(
      'https://api.fxratesapi.com/latest?base=USD&currencies=XAU,XAG&format=json',
      { timeout: 7000 }
    );
    const xauUsd = r.data?.rates?.XAU ? parseFloat((1 / r.data.rates.XAU).toFixed(2)) : 0;
    const xagUsd = r.data?.rates?.XAG ? parseFloat((1 / r.data.rates.XAG).toFixed(4)) : 0;
    if (xauUsd > 2000 && xagUsd > 15) return { xauUsd, xagUsd, src: 'fxratesapi' };
  } catch (_) {}

  try {
    const r = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
      headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 7000,
    });
    const g = r.data?.items?.[0]?.xauPrice;
    const s = r.data?.items?.[0]?.xagPrice;
    if (g > 2000 && s > 15) return { xauUsd: g, xagUsd: s, src: 'goldprice.org' };
  } catch (_) {}

  try {
    const r = await axios.get(
      'https://api.metals.dev/v1/latest?api_key=&unit=toz&currency=USD',
      { timeout: 7000 }
    );
    const g = r.data?.metals?.gold;
    const s = r.data?.metals?.silver;
    if (g > 2000 && s > 15) return { xauUsd: g, xagUsd: s, src: 'metals.dev' };
  } catch (_) {}

  console.warn('⚠️  All spot APIs failed — using static fallback');
  return { xauUsd: 3340, xagUsd: 33.5, src: 'static_fallback_May2026' };
}

// ============================================================
// FOREX USD/INR (3 fallbacks)
// ============================================================
async function getForex() {
  try {
    const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 6000 });
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

  console.warn('⚠️  All forex APIs failed — using static fallback');
  return 84.50;
}

// Spot → MCX equivalent
// Gold  per 10g : (xauUsd / 31.1035) * 10   * usdInr * premiumFactor
// Silver per 1kg: (xagUsd / 31.1035) * 1000 * usdInr * premiumFactor
function calcBackup(spot, usdInr) {
  return {
    goldPer10g:  Math.round((spot.xauUsd / 31.1035) * 10   * usdInr * PREMIUM_FACTOR),
    silverPerKg: Math.round((spot.xagUsd / 31.1035) * 1000 * usdInr * PREMIUM_FACTOR),
  };
}

function isValidLtp(q) {
  return q != null && typeof q.ltp === 'number' && q.ltp > 0;
}

function roundQuote(q) {
  return {
    ltp:  Math.round(q.ltp),
    bid:  Math.round(q.bid),
    ask:  Math.round(q.ask),
    high: Math.round(q.high),
    low:  Math.round(q.low),
  };
}

// ============================================================
// ROUTES
// ============================================================

app.get('/', (req, res) => res.json({
  status:    'R.R Jewellers Server ✅',
  version:   'v21',
  mcxOpen:   isMCXOpen(),
  holiday:   isMCXHoliday(),
  proxy:     PROXY_URL ? 'QuotaGuard ✅' : 'Direct (no proxy)',
  timestamp: new Date().toISOString(),
}));

app.get('/ping', (req, res) => res.json({ ok: true }));

app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  const isHoliday  = isMCXHoliday();

  // Always get spot + forex (doesn't need Angel One auth)
  const [spotRes, forexRes] = await Promise.allSettled([getSpotRates(), getForex()]);
  const spot   = spotRes.status  === 'fulfilled' ? spotRes.value  : { xauUsd: 3340, xagUsd: 33.5, src: 'error' };
  const usdInr = forexRes.status === 'fulfilled' ? forexRes.value : 84.50;
  const backup = calcBackup(spot, usdInr);

  let quotes      = {};
  let gCurTok     = null, gNxtTok = null, sCurTok = null, sNxtTok = null;
  let gC          = [], sC = [];
  let angelError  = null;
  let angelSource = 'not_attempted';

  if (marketOpen && !isHoliday) {
    try {
      await ensureCache();
      gC = getDynamicContracts('GOLD');
      sC = getDynamicContracts('SILVER');

      if (gC.length > 0 && sC.length > 0) {
        const jwt = await login();

        // FIX: direct sync assignment, no fake Promise.all
        gCurTok = tokenCache.GOLD[gC[0]]     || null;
        gNxtTok = gC[1] ? (tokenCache.GOLD[gC[1]]     || null) : null;
        sCurTok = tokenCache.SILVER[sC[0]]   || null;
        sNxtTok = sC[1] ? (tokenCache.SILVER[sC[1]]   || null) : null;

        const tokensToFetch = [
          gCurTok?.symboltoken, sCurTok?.symboltoken,
          gNxtTok?.symboltoken, sNxtTok?.symboltoken,
        ].filter(Boolean);

        if (tokensToFetch.length > 0) {
          quotes      = await getBulkQuotes(jwt, tokensToFetch) || {};
          angelSource = 'angel_mcx_live';
        }
      } else {
        angelSource = 'cache_empty_spot_backup';
      }
    } catch (err) {
      angelError  = err.message;
      angelSource = 'angel_failed_spot_backup';
      console.error('Angel One error:', angelError);
    }
  } else {
    angelSource = isHoliday ? 'market_holiday_spot_backup' : 'market_closed_spot_backup';
  }

  // Resolve final prices
  const gCurrRaw = gCurTok ? quotes[gCurTok.symboltoken] : null;
  const sCurrRaw = sCurTok ? quotes[sCurTok.symboltoken] : null;
  const gNextRaw = gNxtTok ? quotes[gNxtTok.symboltoken] : null;
  const sNextRaw = sNxtTok ? quotes[sNxtTok.symboltoken] : null;

  let usedBackup = false;

  // FIX: build backup objects once, reuse cleanly
  const gBackupQ = { ltp: backup.goldPer10g,  bid: backup.goldPer10g,  ask: backup.goldPer10g,  high: backup.goldPer10g,  low: backup.goldPer10g  };
  const sBackupQ = { ltp: backup.silverPerKg, bid: backup.silverPerKg, ask: backup.silverPerKg, high: backup.silverPerKg, low: backup.silverPerKg };

  const gCurr = isValidLtp(gCurrRaw) ? gCurrRaw : (usedBackup = true, gBackupQ);
  const sCurr = isValidLtp(sCurrRaw) ? sCurrRaw : (usedBackup = true, sBackupQ);

  const gNext = isValidLtp(gNextRaw)
    ? gNextRaw
    : { ltp: gCurr.ltp + 150, bid: gCurr.bid + 150, ask: gCurr.ask + 150, high: gCurr.high + 150, low: gCurr.low + 150 };
  const sNext = isValidLtp(sNextRaw)
    ? sNextRaw
    : { ltp: sCurr.ltp + 500, bid: sCurr.bid + 500, ask: sCurr.ask + 500, high: sCurr.high + 500, low: sCurr.low + 500 };

  if (gC.length === 0) gC = ['JUN26', 'AUG26'];
  if (sC.length === 0) sC = ['MAY26', 'JUL26'];

  // FIX: source field now correctly reflects backup usage
  const finalSource = usedBackup ? 'spot_backup_active' : (angelSource === 'angel_mcx_live' ? 'angel_mcx_live' : angelSource);

  res.json({
    success:     true,
    source:      finalSource,        // FIXED: was always 'angel_mcx_live' even on backup
    marketOpen,
    isHoliday,
    proxyActive: !!PROXY_URL,
    spotSource:  spot.src,
    usdInr,
    _debug: {
      angelError,
      angelSource,
      goldContract:   gCurTok?.tradingsymbol || null,
      silverContract: sCurTok?.tradingsymbol || null,
    },
    contracts: {
      gold:   { current: gC[0], next: gC[1] || null, symbol: gCurTok?.tradingsymbol || 'GOLD_BACKUP'   },
      silver: { current: sC[0], next: sC[1] || null, symbol: sCurTok?.tradingsymbol || 'SILVER_BACKUP' },
    },
    spot:        { xauUsd: spot.xauUsd, xagUsd: spot.xagUsd },
    goldPer10g:  Math.round(gCurr.ltp),
    silverPerKg: Math.round(sCurr.ltp),
    futures: {
      gold:       roundQuote(gCurr),
      silver:     roundQuote(sCurr),
      goldNext:   roundQuote(gNext),
      silverNext: roundQuote(sNext),
    },
    timestamp: new Date().toISOString(),
  });
});

// Debug — cache state + proxy info
app.get('/debug', async (req, res) => {
  await ensureCache();
  const gC = getDynamicContracts('GOLD');
  const sC = getDynamicContracts('SILVER');
  res.json({
    mcxOpen:     isMCXOpen(),
    isHoliday:   isMCXHoliday(),
    proxyActive: !!PROXY_URL,
    proxyUrl:    PROXY_URL ? PROXY_URL.replace(/\/\/([^:]+):[^@]+@/, '//$1:***@') : null,
    cacheAge:    cacheBuiltAt ? `${Math.round((Date.now() - cacheBuiltAt) / 60000)}m ago` : 'not built',
    contracts:   { gold: gC, silver: sC },
    goldToken:   gC[0] ? (tokenCache.GOLD[gC[0]]   || null) : null,
    silverToken: sC[0] ? (tokenCache.SILVER[sC[0]] || null) : null,
    cacheSize:   { gold: Object.keys(tokenCache.GOLD).length, silver: Object.keys(tokenCache.SILVER).length },
  });
});

// Google Sheets updates
app.get('/updates', async (req, res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID not set');
    const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r   = await axios.get(url, { timeout: 8000 });
    // FIX: correct regex for Google's response format: /*O_o*/\ngoogle.visualization.Query.setResponse({...});
    const jsonStr = r.data.replace(/^[^(]*\((.+)\);\s*$/s, '$1');
    const data    = JSON.parse(jsonStr);
    const rows    = data.table.rows.map(row => ({
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
// START
// ============================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log('════════════════════════════════════════════');
  console.log('  R.R Jewellers Gold Server v21            ');
  console.log('════════════════════════════════════════════');
  console.log(`🚀 Port: ${PORT}`);
  console.log(`🔗 Proxy: ${PROXY_URL ? 'QuotaGuard ✅' : 'Direct (no proxy)'}`);
  console.log(`📅 Holiday today: ${isMCXHoliday()} | Market open: ${isMCXOpen()}`);

  await buildScripCache();
  setInterval(buildScripCache, 12 * 60 * 60 * 1000);

  // Keep Render free tier alive (ping every 4 min)
  const pingUrl = SELF_URL.startsWith('https') ? SELF_URL : 'https://gold-proxy-server.onrender.com/ping';
  setInterval(() => {
    https.get(pingUrl, () => {}).on('error', () => {});
  }, 4 * 60 * 1000);
});
