/**
 * RR Jewellers Gold Server v18  -  PRODUCTION FINAL BUILD
 * ═══════════════════════════════════════════════════════════════
 *
 * FIXES in v18 (Micro-optimizations & Safety):
 * 1. Safe Cache Initialization: Prevents TypeError crash on /debug 
 * if Angel's Scrip Master API is temporarily down.
 * 2. Restored detailed error capturing in Angel login loop.
 * 3. Bulletproof Optional Chaining added to token mapping.
 *
 * ═══════════════════════════════════════════════════════════════
 */

'use strict';

const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const app     = express();

app.use(cors());
app.use(express.json());

// ───────────────────────────────────────────────────────────────
// CONFIG & CREDENTIALS
// ───────────────────────────────────────────────────────────────
const CLIENT_ID   = 'AAAA238852';
const API_KEY     = 'DPAHMIXr';
const TOTP_SECRET = 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN   = '1857';
const SHEET_ID    = process.env.SHEET_ID || '';
const SELF_URL    = process.env.SELF_URL || 'https://gold-proxy-server.onrender.com/';

const MONTHS = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];

function getIST() {
  const now = new Date();
  const ist = new Date(now.getTime() + (5 * 60 + 30) * 60 * 1000);
  return { year: ist.getUTCFullYear(), month: ist.getUTCMonth(), day: ist.getUTCDate(), hour: ist.getUTCHours(), min: ist.getUTCMinutes(), dow: ist.getUTCDay() };
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

// ───────────────────────────────────────────────────────────────
// SMART TOKEN CACHE BUILDER
// ───────────────────────────────────────────────────────────────
// FIX: Safely initialized with inner objects to prevent undefined crashes
let tokenCache   = { GOLD: {}, SILVER: {} }; 
let cacheBuiltAt = 0;
const CACHE_TTL  = 22 * 60 * 60 * 1000;
let lastKnownRates = null;

async function buildScripCache() {
  try {
    const r = await axios.get(
      'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json',
      { timeout: 30000 }
    );

    const instruments = r.data;
    if (!Array.isArray(instruments) || instruments.length === 0) return false;

    const newCache = { GOLD: {}, SILVER: {} };

    for (const inst of instruments) {
      if (inst.exch_seg !== 'MCX' || inst.instrumenttype !== 'FUTCOM') continue;

      const sym = (inst.symbol || '').toUpperCase().trim();
      const tok = inst.token;
      const exp = (inst.expiry || '').toUpperCase().trim(); 

      if (!sym || !tok) continue;

      let label = null;
      const expMatch = exp.match(/([A-Z]{3})20(\d{2})/);
      if (expMatch) {
        label = expMatch[1] + expMatch[2]; 
      } else {
        const symMatch = sym.match(/([A-Z]{3})(\d{2})/);
        if (symMatch) label = symMatch[1] + symMatch[2];
      }

      if (!label || !MONTHS.includes(label.slice(0,3))) continue;

      if (sym.startsWith('GOLD') && !sym.includes('GOLDM') && !sym.includes('GOLDPETAL') && !sym.includes('GOLDGUINEA') && !sym.includes('TEN')) {
        newCache['GOLD'][label] = { symboltoken: tok, tradingsymbol: sym };
      }

      if (sym.startsWith('SILVER') && !sym.includes('SILVERM') && !sym.includes('SILVERMIC')) {
        const existing = newCache['SILVER'][label];
        if (!existing || sym.startsWith('SILVER30')) {
          newCache['SILVER'][label] = { symboltoken: tok, tradingsymbol: sym };
        }
      }
    }

    tokenCache = newCache;
    cacheBuiltAt = Date.now();
    return true;
  } catch (e) {
    return false;
  }
}

async function ensureCache() {
  if (cacheBuiltAt === 0 || Date.now() - cacheBuiltAt > CACHE_TTL) await buildScripCache();
}

function getDynamicContracts(base) {
  const cache = tokenCache[base] || {};
  const keys = Object.keys(cache);
  if (keys.length === 0) return [];

  const now = getIST();
  const currentYr = parseInt(now.year.toString().slice(-2));
  const currentMon = now.month;

  const valid = keys.filter(k => {
    const m = MONTHS.indexOf(k.slice(0,3));
    const y = parseInt(k.slice(3,5));
    if (y < currentYr) return false;
    if (y === currentYr && m < currentMon) return false;
    if (y === currentYr && m === currentMon) {
       const expiry = getExpiryDate(now.year, m);
       const today = new Date(Date.UTC(now.year, m, now.day));
       if (today > expiry) return false;
    }
    return true;
  });

  valid.sort((a, b) => {
    const monA = MONTHS.indexOf(a.slice(0,3));
    const yrA = parseInt(a.slice(3,5));
    const monB = MONTHS.indexOf(b.slice(0,3));
    const yrB = parseInt(b.slice(3,5));
    if (yrA !== yrB) return yrA - yrB;
    return monA - monB;
  });

  return valid.slice(0, 2);
}

async function findToken(jwt, base, contractLabel) {
  await ensureCache();
  return tokenCache[base]?.[contractLabel] || null;
}

// ───────────────────────────────────────────────────────────────
// TOTP GENERATOR & ANGEL AUTH
// ───────────────────────────────────────────────────────────────
function generateTOTP(secret, offset) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const v = alpha.indexOf(c);
    if (v >= 0) bits += v.toString(2).padStart(5, '0');
  }
  const bytes = [];
  for (let i = 0; i + 8 <= bits.length; i += 8) bytes.push(parseInt(bits.slice(i, i + 8), 2));
  const key  = Buffer.from(bytes);
  const t    = Math.floor(Date.now() / 1000 / 30) + (offset || 0);
  const tb   = Buffer.alloc(8);
  tb.writeUInt32BE(Math.floor(t / 0x100000000), 0);
  tb.writeUInt32BE(t >>> 0, 4);
  const hmac = crypto.createHmac('sha1', key).update(tb).digest();
  const off  = hmac[hmac.length - 1] & 0xf;
  const code = ((hmac[off] & 0x7f) << 24 | (hmac[off + 1] & 0xff) << 16 | (hmac[off + 2] & 0xff) << 8 | (hmac[off + 3] & 0xff)) % 1000000;
  return code.toString().padStart(6, '0');
}

let JWT = null, JWT_EXP = 0;

const HDR = (jwt) => ({
  'Content-Type':     'application/json',
  'Accept':           'application/json',
  'X-UserType':       'USER',
  'X-SourceID':       'WEB',
  'X-ClientLocalIP':  '127.0.0.1',
  'X-ClientPublicIP': '127.0.0.1', 
  'X-MACAddress':     'fe:80:00:00:00:00',
  'X-PrivateKey':     API_KEY,
  ...(jwt ? { 'Authorization': 'Bearer ' + jwt } : {}),
});

async function login() {
  if (JWT && Date.now() < JWT_EXP) return JWT;
  JWT = null; JWT_EXP = 0;
  
  let lastError = "Unknown Error";

  for (const w of [-4, -3, -2, -1, 0, 1, 2, 3, 4]) {
    const pin = generateTOTP(TOTP_SECRET, w);
    try {
      const r = await axios.post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        { clientcode: CLIENT_ID, password: ANGEL_PIN, totp: pin },
        { headers: HDR(), timeout: 10000 }
      );
      if (r.data.status && r.data.data?.jwtToken) {
        JWT = r.data.data.jwtToken;
        JWT_EXP = Date.now() + 6 * 60 * 60 * 1000;
        return JWT;
      }
    } catch (e) {
      // FIX: Capture exact error to know why Angel One rejected login
      lastError = e.response?.data?.message || e.message; 
    }
  }
  throw new Error(`Angel login failed. Reason: ${lastError}`);
}

// ───────────────────────────────────────────────────────────────
// DATA FETCHERS
// ───────────────────────────────────────────────────────────────
async function getBulkQuotes(jwt, tokens) {
  const validTokens = tokens.filter(t => t).map(String); 
  if (validTokens.length === 0) return {};

  try {
    const r = await axios.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
      { mode: 'FULL', exchangeTokens: { MCX: validTokens } },
      { headers: HDR(jwt), timeout: 8000 }
    );
    const fetched = r.data.data?.fetched || [];
    const results = {};
    fetched.forEach(d => {
      results[d.symbolToken] = {
        ltp:  Number(d.ltp) || 0, bid:  Number(d.depth?.buy?.[0]?.price) || Number(d.ltp) || 0,
        ask:  Number(d.depth?.sell?.[0]?.price) || Number(d.ltp) || 0,
        high: Number(d.high) || 0, low: Number(d.low) || 0, open: Number(d.open) || 0,
      };
    });
    return results;
  } catch (e) {
    return null;
  }
}

async function getSpotRates() {
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout: 6000 });
    const gold = r.data.find(x => x.gold)?.gold; const silver = r.data.find(x => x.silver)?.silver;
    if (gold && silver) return { xauUsd: gold, xagUsd: silver, src: 'metals.live' };
  } catch (e) {}
  return { xauUsd: 2300, xagUsd: 28.0, src: 'fallback' };
}

async function getForex() {
  try {
    const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 5000 });
    if (r.data?.rates?.INR) return r.data.rates.INR;
  } catch (e) {}
  return 83.50;
}

// ───────────────────────────────────────────────────────────────
// API ROUTES
// ───────────────────────────────────────────────────────────────

app.get('/', (req, res) => {
    res.json({
        status: "R.R Jewellers Server is Running smoothly! Ready for App Connection.",
        endpoints: ["/rates", "/debug", "/updates"],
        mcxMarketOpen: isMCXOpen()
    });
});

app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();

  try {
    await ensureCache();
    const jwt = await login();

    const gC = getDynamicContracts('GOLD');
    const sC = getDynamicContracts('SILVER');

    if (!gC.length || !sC.length) throw new Error('No active contracts found in Cache.');

    const [gCurTok, gNxtTok, sCurTok, sNxtTok] = await Promise.all([
      findToken(jwt, 'GOLD', gC[0]),
      gC[1] ? findToken(jwt, 'GOLD', gC[1]) : Promise.resolve(null),
      findToken(jwt, 'SILVER', sC[0]),
      sC[1] ? findToken(jwt, 'SILVER', sC[1]) : Promise.resolve(null),
    ]);

    if (!gCurTok || !sCurTok) throw new Error(`Primary Tokens missing from Angel`);

    const tokensToFetch = [gCurTok.symboltoken, sCurTok.symboltoken];
    if (gNxtTok) tokensToFetch.push(gNxtTok.symboltoken);
    if (sNxtTok) tokensToFetch.push(sNxtTok.symboltoken);

    const [quotesRes, spotRes, forexRes] = await Promise.allSettled([
      getBulkQuotes(jwt, tokensToFetch),
      getSpotRates(),
      getForex()
    ]);

    const quotes = quotesRes.status === 'fulfilled' && quotesRes.value ? quotesRes.value : {};
    const spotData = spotRes.status === 'fulfilled' ? spotRes.value : { xauUsd: 0, xagUsd: 0, src: 'error' };
    const usdInr = forexRes.status === 'fulfilled' ? forexRes.value : 83.50;

    let gCurr = quotes[gCurTok.symboltoken] || { ltp: 0 };
    let sCurr = quotes[sCurTok.symboltoken] || { ltp: 0 };
    let gNextRaw = gNxtTok ? quotes[gNxtTok.symboltoken] : null;
    let sNextRaw = sNxtTok ? quotes[sNxtTok.symboltoken] : null;

    let usedBackup = false;

    // ANTI-CRASH SYSTEM: If Angel returns 0 (Holiday/Glitch), seamlessly calculate Spot Backup
    if (gCurr.ltp === 0 || sCurr.ltp === 0) {
        usedBackup = true;
        const gBackup = Math.round((spotData.xauUsd / 31.1035) * 10 * usdInr * 1.103);
        const sBackup = Math.round((spotData.xagUsd / 31.1035) * 1000 * usdInr * 1.103);

        if (gCurr.ltp === 0) gCurr = { ltp: gBackup, bid: gBackup, ask: gBackup, high: gBackup, low: gBackup, open: gBackup };
        if (sCurr.ltp === 0) sCurr = { ltp: sBackup, bid: sBackup, ask: sBackup, high: sBackup, low: sBackup, open: sBackup };
    }

    const gNext = gNextRaw?.ltp > 0 ? gNextRaw : gCurr;
    const sNext = sNextRaw?.ltp > 0 ? sNextRaw : sCurr;

    const payload = {
      success:    true,
      source:     usedBackup ? 'spot_backup_active' : 'angel_mcx_live',
      marketOpen: marketOpen,
      spotSource: spotData.src,
      usdInr:     usdInr,
      contracts: {
        gold:   { current: gC[0], next: gC[1], currentSymbol: gCurTok.tradingsymbol },
        silver: { current: sC[0], next: sC[1], currentSymbol: sCurTok.tradingsymbol },
      },
      spot: { xauUsd: spotData.xauUsd, xagUsd: spotData.xagUsd },
      goldPer10g:  Math.round(gCurr.ltp),
      silverPerKg: Math.round(sCurr.ltp),
      futures: {
        gold: { ltp: Math.round(gCurr.ltp), bid: Math.round(gCurr.bid), ask: Math.round(gCurr.ask), high: Math.round(gCurr.high), low: Math.round(gCurr.low) },
        silver: { ltp: Math.round(sCurr.ltp), bid: Math.round(sCurr.bid), ask: Math.round(sCurr.ask), high: Math.round(sCurr.high), low: Math.round(sCurr.low) },
        goldNext: { ltp: Math.round(gNext.ltp), bid: Math.round(gNext.bid), ask: Math.round(gNext.ask), high: Math.round(gNext.high), low: Math.round(gNext.low) },
        silverNext: { ltp: Math.round(sNext.ltp), bid: Math.round(sNext.bid), ask: Math.round(sNext.ask), high: Math.round(sNext.high), low: Math.round(sNext.low) },
      },
      timestamp: new Date().toISOString(),
    };

    lastKnownRates = payload;
    return res.json(payload);

  } catch (err) {
    if (lastKnownRates) {
      return res.json({ ...lastKnownRates, source: 'last_known_rates', marketOpen: marketOpen });
    }
    return res.status(500).json({ success: false, source: 'error', error: err.message, marketOpen });
  }
});

app.get('/debug', async (req, res) => {
  await ensureCache();
  const gC = getDynamicContracts('GOLD');
  const sC = getDynamicContracts('SILVER');
  res.json({
    mcxOpen: isMCXOpen(),
    contracts: { gold: gC, silver: sC },
    // FIX: Added optional chaining (?.) so it never crashes if cache is empty
    goldTokenFound: gC[0] ? (tokenCache['GOLD']?.[gC[0]] || null) : null,
    silverTokenFound: sC[0] ? (tokenCache['SILVER']?.[sC[0]] || null) : null,
    cacheSize: { gold: Object.keys(tokenCache.GOLD || {}).length, silver: Object.keys(tokenCache.SILVER || {}).length }
  });
});

app.get('/updates', async (req, res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID env not set');
    const url  = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r    = await axios.get(url, { timeout: 8000 });
    const json = r.data.replace(/.*?({.*}).*/s, '$1');
    const data = JSON.parse(json);
    const rows = data.table.rows.map(row => ({ date: row.c[0]?.v || '', title: row.c[1]?.v || '', content: row.c[2]?.v || '', image: row.c[3]?.v || '' }));
    res.json({ success: true, updates: rows.filter(r => r.title) });
  } catch (e) { res.json({ success: true, updates: [{ title: 'Welcome to R.R. Jewellers' }] }); }
});

// ───────────────────────────────────────────────────────────────
// SERVER START
// ───────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log('═══════════════════════════════════════════');
  console.log(' RR Jewellers Server v18 - Final Production');
  console.log('═══════════════════════════════════════════');
  await buildScripCache();
  setInterval(async () => { await buildScripCache(); }, 12 * 60 * 60 * 1000);
  setInterval(() => { require('https').get(SELF_URL, () => {}).on('error', () => {}); }, 4 * 60 * 1000);
});
