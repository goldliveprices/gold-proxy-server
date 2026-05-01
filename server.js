/**
 * RR Jewellers Gold Server v13  -  PRODUCTION FIXED BUILD
 * ═══════════════════════════════════════════════════════════════
 *
 * FIXES in v13:
 *  1. Silver token regex: Angel names Silver as SILVER30JUN26FUT (30kg lot)
 *     Old regex missed the "30" suffix → now handles SILVER<lot><MON><YR>FUT
 *  2. Forex fallback updated: USD/INR ~84.50, added ExchangeRate-API source
 *  3. Cold-start fallback: on LTP=0, immediately tries spot-derived instead
 *     of just erroring when lastKnownRates is null
 *  4. buildScripCache now strictly filters ONLY main contracts:
 *     GOLD (1kg) = no mini suffix, SILVER 30kg = has "30" in name
 *     Excludes: GOLDM, GOLDPETAL, SILVERM, SILVERMIC (mini variants)
 *  5. isMCXOpen: extended to 11:55 PM IST (was 11:30)
 *  6. getContracts: fixed - now always finds current month correctly
 *     using IST date not UTC
 *
 * VERIFIED MCX CONTRACT RULES (from official MCX circulars):
 *
 *  GOLD  (1 kg)  : Contracts in FEB, APR, JUN, AUG, OCT, DEC
 *                  Expiry = 5th of contract month
 *                  (if 5th is holiday/weekend → previous business day)
 *
 *  SILVER (30 kg): Contracts in MAR, MAY, JUL, SEP, DEC
 *                  Expiry = 5th of contract month
 *                  (if 5th is holiday/weekend → previous business day)
 *
 * FALLBACK CHAIN for /rates:
 *  1. Angel MCX live quote
 *  2. lastKnownRates (last real price cached in memory)
 *  3. Spot-derived estimate (international XAU/XAG → INR)
 *  4. Error response
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

// ───────────────────────────────────────────────────────────────
// MCX CONTRACT MONTHS  (0-indexed JS months)
//
//  GOLD  : FEB=1, APR=3, JUN=5, AUG=7, OCT=9, DEC=11
//  SILVER: MAR=2, MAY=4, JUL=6, SEP=8, DEC=11
// ───────────────────────────────────────────────────────────────
const MONTHS   = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
const GOLD_M   = [1, 3, 5, 7, 9, 11];   // FEB APR JUN AUG OCT DEC
const SILVER_M = [2, 4, 6, 8, 11];      // MAR MAY JUL SEP DEC

// ───────────────────────────────────────────────────────────────
// HELPER: Current IST date components
// ───────────────────────────────────────────────────────────────
function getIST() {
  const now = new Date();
  const ist = new Date(now.getTime() + (5 * 60 + 30) * 60 * 1000);
  return {
    year:  ist.getUTCFullYear(),
    month: ist.getUTCMonth(),   // 0-indexed
    day:   ist.getUTCDate(),
    hour:  ist.getUTCHours(),
    min:   ist.getUTCMinutes(),
    dow:   ist.getUTCDay(),     // 0=Sun
  };
}

/**
 * MCX Gold & Silver expire on the 5th of their contract month.
 * If 5th is Saturday → Friday(4th), if Sunday → Friday(3rd).
 */
function getExpiryDate(year, month) {
  const fifth = new Date(Date.UTC(year, month, 5));
  const dow   = fifth.getUTCDay();
  if (dow === 0) return new Date(Date.UTC(year, month, 3)); // Sun → Fri
  if (dow === 6) return new Date(Date.UTC(year, month, 4)); // Sat → Fri
  return fifth;
}

/**
 * Returns next 2 active contract labels e.g. ["JUN26","AUG26"]
 * Uses IST date so rollover happens correctly for Indian traders.
 * Rolls to next contract if today is ON or AFTER expiry date.
 */
function getContracts(validM) {
  const ist = getIST();
  let m = ist.month;
  let y = ist.year;

  // Check if current month's contract has expired (or doesn't exist)
  if (validM.includes(m)) {
    const expiry   = getExpiryDate(y, m);
    // Compare IST date (not UTC) - use IST day for rollover decision
    const todayIST = new Date(Date.UTC(y, m, ist.day));
    if (todayIST > expiry) {
      // Contract has expired (strictly after expiry date) → advance
      // NOTE: On expiry day itself (todayIST === expiry), MCX still trades
      // until 11:30 PM, so we keep the current contract active that day.
      m++; if (m > 11) { m = 0; y++; }
    }
  } else {
    // Current month is not a valid contract month → advance
    m++; if (m > 11) { m = 0; y++; }
  }

  const out = [];
  for (let i = 0; i < 24 && out.length < 2; i++) {
    if (validM.includes(m)) out.push(MONTHS[m] + y.toString().slice(-2));
    m++; if (m > 11) { m = 0; y++; }
  }
  return out;
}

// ───────────────────────────────────────────────────────────────
// MCX MARKET HOURS CHECK  (IST = UTC+5:30)
//   Normal  : Mon–Fri  09:00 – 23:55 IST
//   The real "closed" signal is LTP=0 from Angel API.
// ───────────────────────────────────────────────────────────────
function isMCXOpen() {
  const { dow, hour, min } = getIST();
  if (dow === 0 || dow === 6) return false;
  const timeM = hour * 60 + min;
  return timeM >= 9 * 60 && timeM < 23 * 60 + 55;
}

// ───────────────────────────────────────────────────────────────
// LAST-KNOWN RATES CACHE
// ───────────────────────────────────────────────────────────────
let lastKnownRates = null;

// ───────────────────────────────────────────────────────────────
// ANGEL ONE SCRIP MASTER CACHE
// tokenCache['GOLD']['JUN26']   = { symboltoken, tradingsymbol }
// tokenCache['SILVER']['JUL26'] = { symboltoken, tradingsymbol }
// ───────────────────────────────────────────────────────────────
let tokenCache   = {};
let cacheBuiltAt = 0;
const CACHE_TTL  = 22 * 60 * 60 * 1000; // 22 hours

async function buildScripCache() {
  try {
    console.log('[CACHE] Downloading Angel scrip master...');
    const r = await axios.get(
      'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json',
      { timeout: 30000 }
    );

    const instruments = r.data;
    if (!Array.isArray(instruments) || instruments.length === 0)
      throw new Error('Empty scrip master response');

    // Only MCX commodity futures (FUTCOM)
    const mcx = instruments.filter(
      i => i.exch_seg === 'MCX' && i.instrumenttype === 'FUTCOM'
    );

    const newCache = {};

    for (const inst of mcx) {
      // Angel uses 'symbol' field; 'name' is sometimes shorter
      const sym = (inst.symbol || '').toUpperCase().trim();
      const tok = inst.token;
      if (!sym || !tok) continue;

      // ── GOLD (1 kg main contract) ──────────────────────────────
      // Symbol formats seen in Angel scrip master:
      //   GOLD<YY><MON>FUT   e.g. GOLD26JUNFUT
      //   GOLD<MON><YY>FUT   e.g. GOLDJUN26FUT
      // Exclude: GOLDM (mini 100g), GOLDPETAL, GOLDGUINEA, etc.
      if (sym.startsWith('GOLD') && !sym.startsWith('GOLDM') &&
          !sym.startsWith('GOLDPETAL') && !sym.startsWith('GOLDGUINEA')) {

        // Must match exactly GOLD + (2-digit year + 3-letter month OR 3-letter month + 2-digit year) + FUT
        const mm = sym.match(/^GOLD(\d{2})([A-Z]{3})FUT$|^GOLD([A-Z]{3})(\d{2})FUT$/);
        if (!mm) continue;

        let mon, yr2;
        if (mm[1] && mm[2]) { yr2 = mm[1]; mon = mm[2]; }  // GOLD26JUNFUT
        else                 { mon = mm[3]; yr2 = mm[4]; }  // GOLDJUN26FUT
        if (!MONTHS.includes(mon)) continue;

        const label = mon + yr2;
        if (!newCache['GOLD']) newCache['GOLD'] = {};
        if (!newCache['GOLD'][label]) {
          newCache['GOLD'][label] = { symboltoken: tok, tradingsymbol: sym };
        }
        continue;
      }

      // ── SILVER (30 kg main contract) ──────────────────────────
      // Symbol formats in Angel:
      //   SILVER30<MON><YY>FUT  e.g. SILVER30JUN26FUT
      //   SILVER<YY><MON>FUT    e.g. SILVER26JUNFUT
      //   SILVER<MON><YY>FUT    e.g. SILVERJUN26FUT
      // Exclude: SILVERM, SILVERMIC (mini variants)
      if (sym.startsWith('SILVER') && !sym.startsWith('SILVERM') &&
          !sym.startsWith('SILVERMIC')) {

        // Try all known Silver main-contract formats
        const mm = sym.match(
          /^SILVER30([A-Z]{3})(\d{2})FUT$|^SILVER(\d{2})([A-Z]{3})FUT$|^SILVER([A-Z]{3})(\d{2})FUT$/
        );
        if (!mm) continue;

        let mon, yr2;
        if (mm[1] && mm[2]) { mon = mm[1]; yr2 = mm[2]; }  // SILVER30JUN26FUT
        else if (mm[3] && mm[4]) { yr2 = mm[3]; mon = mm[4]; }  // SILVER26JUNFUT
        else { mon = mm[5]; yr2 = mm[6]; }                        // SILVERJUN26FUT
        if (!MONTHS.includes(mon)) continue;

        const label = mon + yr2;
        if (!newCache['SILVER']) newCache['SILVER'] = {};
        // Prefer the SILVER30* format (actual 30kg MCX contract)
        const existing = newCache['SILVER'][label];
        if (!existing || sym.startsWith('SILVER30')) {
          newCache['SILVER'][label] = { symboltoken: tok, tradingsymbol: sym };
        }
      }
    }

    tokenCache   = newCache;
    cacheBuiltAt = Date.now();

    const gCount = Object.keys(newCache['GOLD']   || {}).length;
    const sCount = Object.keys(newCache['SILVER'] || {}).length;
    console.log('[CACHE] Built OK - GOLD:' + gCount + '  SILVER:' + sCount);
    console.log('[CACHE] Gold contracts:', Object.keys(newCache['GOLD'] || {}).join(', '));
    console.log('[CACHE] Silver contracts:', Object.keys(newCache['SILVER'] || {}).join(', '));
    return true;

  } catch (e) {
    console.log('[CACHE] Build failed:', e.message);
    return false;
  }
}

async function ensureCache() {
  if (cacheBuiltAt === 0 || Date.now() - cacheBuiltAt > CACHE_TTL) {
    await buildScripCache();
  }
}

/**
 * Resolve symboltoken for base + contractLabel.
 * Priority: scrip-master cache → searchScrip exact → searchScrip broad
 */
async function findToken(jwt, base, contractLabel) {
  await ensureCache();

  const cached = tokenCache[base]?.[contractLabel];
  if (cached) {
    console.log('[TOKEN] cache hit: ' + base + contractLabel +
      ' -> ' + cached.tradingsymbol + ' (' + cached.symboltoken + ')');
    return cached;
  }

  console.log('[TOKEN] cache miss: ' + base + contractLabel + ' - trying searchScrip...');
  const mon = contractLabel.slice(0, 3); // e.g. "JUN"
  const yr2 = contractLabel.slice(3, 5); // e.g. "26"

  // Try exact symbol name variants
  const queries = base === 'SILVER'
    ? [
        'SILVER30' + mon + yr2 + 'FUT',  // SILVER30JUN26FUT  ← primary MCX 30kg
        'SILVER' + yr2 + mon + 'FUT',    // SILVER26JUNFUT
        'SILVER' + mon + yr2 + 'FUT',    // SILVERJUN26FUT
        'SILVER' + mon + yr2,            // SILVERJUN26
      ]
    : [
        base + yr2 + mon + 'FUT',        // GOLD26JUNFUT
        base + mon + yr2 + 'FUT',        // GOLDJUN26FUT
        base + mon + yr2,                // GOLDJUN26
      ];

  for (const q of queries) {
    const results = await searchScrip(jwt, q);
    if (results.length > 0) {
      const hit = results[0];
      console.log('[TOKEN] searchScrip hit: "' + q + '" -> ' +
        hit.tradingsymbol + ' (' + hit.symboltoken + ')');
      return { symboltoken: hit.symboltoken, tradingsymbol: hit.tradingsymbol };
    }
  }

  // Broad search - scan all results for matching month+year in name
  const broad  = await searchScrip(jwt, base);
  const needle = (mon + yr2).toUpperCase();
  // Prefer 30kg Silver contract in broad search
  const match  = broad
    .filter(r =>
      (r.tradingsymbol || '').toUpperCase().includes(needle) ||
      (r.name          || '').toUpperCase().includes(needle)
    )
    .sort((a, b) => {
      // Prefer SILVER30* over SILVERM* in broad results
      const aIs30 = (a.tradingsymbol || '').startsWith('SILVER30');
      const bIs30 = (b.tradingsymbol || '').startsWith('SILVER30');
      return (bIs30 ? 1 : 0) - (aIs30 ? 1 : 0);
    })[0];

  if (match) {
    console.log('[TOKEN] broad match: ' + base + contractLabel +
      ' -> ' + match.tradingsymbol);
    return { symboltoken: match.symboltoken, tradingsymbol: match.tradingsymbol };
  }

  console.log('[TOKEN] FAILED - no token for ' + base + contractLabel);
  return null;
}

// ───────────────────────────────────────────────────────────────
// TOTP GENERATOR  (RFC 6238, HMAC-SHA1, 30-second window)
// ───────────────────────────────────────────────────────────────
function generateTOTP(secret, offset) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const v = alpha.indexOf(c);
    if (v >= 0) bits += v.toString(2).padStart(5, '0');
  }
  const bytes = [];
  for (let i = 0; i + 8 <= bits.length; i += 8)
    bytes.push(parseInt(bits.slice(i, i + 8), 2));
  const key  = Buffer.from(bytes);
  const t    = Math.floor(Date.now() / 1000 / 30) + (offset || 0);
  const tb   = Buffer.alloc(8);
  tb.writeUInt32BE(Math.floor(t / 0x100000000), 0);
  tb.writeUInt32BE(t >>> 0, 4);
  const hmac = crypto.createHmac('sha1', key).update(tb).digest();
  const off  = hmac[hmac.length - 1] & 0xf;
  const code = (
    (hmac[off]     & 0x7f) << 24 |
    (hmac[off + 1] & 0xff) << 16 |
    (hmac[off + 2] & 0xff) << 8  |
    (hmac[off + 3] & 0xff)
  ) % 1000000;
  return code.toString().padStart(6, '0');
}

// ───────────────────────────────────────────────────────────────
// ANGEL ONE AUTH  -  JWT cached 6 hours
// Tries ±4 TOTP windows to handle server clock drift
// ───────────────────────────────────────────────────────────────
let JWT = null, JWT_EXP = 0;

const HDR = (jwt) => ({
  'Content-Type':     'application/json',
  'Accept':           'application/json',
  'X-UserType':       'USER',
  'X-SourceID':       'WEB',
  'X-ClientLocalIP':  '127.0.0.1',
  'X-ClientPublicIP': '74.220.52.100',
  'X-MACAddress':     'fe:80:00:00:00:00',
  'X-PrivateKey':     API_KEY,
  ...(jwt ? { 'Authorization': 'Bearer ' + jwt } : {}),
});

async function login() {
  if (JWT && Date.now() < JWT_EXP) return JWT;
  JWT = null; JWT_EXP = 0;

  for (const w of [-4, -3, -2, -1, 0, 1, 2, 3, 4]) {
    const pin = generateTOTP(TOTP_SECRET, w);
    try {
      const r = await axios.post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        { clientcode: CLIENT_ID, password: ANGEL_PIN, totp: pin },
        { headers: HDR(), timeout: 10000 }
      );
      if (r.data.status && r.data.data?.jwtToken) {
        JWT     = r.data.data.jwtToken;
        JWT_EXP = Date.now() + 6 * 60 * 60 * 1000;
        console.log('[AUTH] Login OK  window=' + w);
        return JWT;
      }
    } catch (e) { /* try next TOTP window */ }
  }
  throw new Error('Angel login failed - check credentials / IP whitelist');
}

// ───────────────────────────────────────────────────────────────
// ANGEL ONE API HELPERS
// ───────────────────────────────────────────────────────────────

async function searchScrip(jwt, q) {
  try {
    const r = await axios.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/searchScrip',
      { exchange: 'MCX', searchscrip: q },
      { headers: HDR(jwt), timeout: 8000 }
    );
    return r.data.data || [];
  } catch (e) {
    console.log('[SEARCH] error "' + q + '": ' + e.message.slice(0, 60));
    return [];
  }
}

/**
 * Fetch market quote for a symbol token.
 * Returns null on network error.
 * Returns object with ltp=0 when market is closed.
 */
async function getQuote(jwt, symboltoken) {
  try {
    const r = await axios.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
      { mode: 'FULL', exchangeTokens: { MCX: [String(symboltoken)] } },
      { headers: HDR(jwt), timeout: 8000 }
    );
    const d = r.data.data?.fetched?.[0] || {};
    return {
      ltp:  Number(d.ltp)                     || 0,
      bid:  Number(d.depth?.buy?.[0]?.price)  || Number(d.ltp) || 0,
      ask:  Number(d.depth?.sell?.[0]?.price) || Number(d.ltp) || 0,
      high: Number(d.high)                     || 0,
      low:  Number(d.low)                      || 0,
      open: Number(d.open)                     || 0,
    };
  } catch (e) {
    console.log('[QUOTE] error token ' + symboltoken + ': ' + e.message.slice(0, 60));
    return null;
  }
}

// ───────────────────────────────────────────────────────────────
// INTERNATIONAL SPOT RATES  (USD/oz)  – 3 sources with fallback
// ───────────────────────────────────────────────────────────────
async function getSpotRates() {
  // Source 1: metals.live
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout: 6000 });
    if (Array.isArray(r.data)) {
      const gold   = r.data.find(x => x.gold)?.gold;
      const silver = r.data.find(x => x.silver)?.silver;
      if (gold > 2000 && gold < 10000 && silver > 20 && silver < 500) {
        console.log('[SPOT] metals.live ok: xau=' + gold + ' xag=' + silver);
        return { xauUsd: gold, xagUsd: silver, src: 'metals.live' };
      }
    }
  } catch (e) { console.log('[SPOT] metals.live fail'); }

  // Source 2: goldprice.org
  try {
    const r = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://goldprice.org/', 'Accept': 'application/json' },
      timeout: 8000,
    });
    const gold   = r.data?.items?.[0]?.xauPrice;
    const silver = r.data?.items?.[0]?.xagPrice;
    if (gold > 2000 && gold < 10000 && silver > 20 && silver < 500) {
      console.log('[SPOT] goldprice.org ok: xau=' + gold + ' xag=' + silver);
      return { xauUsd: gold, xagUsd: silver, src: 'goldprice.org' };
    }
  } catch (e) { console.log('[SPOT] goldprice.org fail'); }

  // Source 3: fxratesapi (XAU/XAG per USD → invert)
  try {
    const r = await axios.get(
      'https://api.fxratesapi.com/latest?base=USD&currencies=XAU,XAG&format=json',
      { timeout: 6000 }
    );
    const xauUsd = r.data?.rates?.XAU ? parseFloat((1 / r.data.rates.XAU).toFixed(2)) : 0;
    const xagUsd = r.data?.rates?.XAG ? parseFloat((1 / r.data.rates.XAG).toFixed(4)) : 0;
    if (xauUsd > 2000 && xauUsd < 10000 && xagUsd > 20 && xagUsd < 500) {
      console.log('[SPOT] fxratesapi ok: xau=' + xauUsd + ' xag=' + xagUsd);
      return { xauUsd, xagUsd, src: 'fxratesapi' };
    }
  } catch (e) { console.log('[SPOT] fxratesapi fail'); }

  // Fixed fallback (approximate May 2026 levels)
  console.log('[SPOT] All sources failed - using fixed fallback');
  return { xauUsd: 3330, xagUsd: 33.0, src: 'fixed_fallback' };
}

// ───────────────────────────────────────────────────────────────
// USD → INR FOREX  (3 live sources + hardcoded fallback)
// ───────────────────────────────────────────────────────────────
async function getForex() {
  // Source 1: Frankfurter (ECB data)
  try {
    const r    = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 5000 });
    const rate = r.data?.rates?.INR;
    if (rate > 75 && rate < 100) { console.log('[FOREX] frankfurter: ' + rate); return rate; }
  } catch (e) { console.log('[FOREX] frankfurter fail'); }

  // Source 2: Open Exchange Rates (free, no key)
  try {
    const r    = await axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 5000 });
    const rate = r.data?.rates?.INR;
    if (rate > 75 && rate < 100) { console.log('[FOREX] open.er-api: ' + rate); return rate; }
  } catch (e) { console.log('[FOREX] open.er-api fail'); }

  // Source 3: ExchangeRate-API free endpoint
  try {
    const r    = await axios.get('https://api.exchangerate-api.com/v4/latest/USD', { timeout: 5000 });
    const rate = r.data?.rates?.INR;
    if (rate > 75 && rate < 100) { console.log('[FOREX] exchangerate-api: ' + rate); return rate; }
  } catch (e) { console.log('[FOREX] exchangerate-api fail'); }

  console.log('[FOREX] All failed - using 84.50');
  return 84.50;
}

/**
 * Estimate MCX price from international spot rates.
 * MCX Gold  = XAU_USD/oz ÷ 31.1035 g/oz × 10g × USD_INR × MCX_premium
 * MCX Silver= XAG_USD/oz ÷ 31.1035 g/oz × 1000g × USD_INR × MCX_premium
 *
 * MCX premium factor includes:
 *   - 15% customs duty on gold
 *   - 3% GST + insurance/freight
 *   - MCX lot-size spot basis premium (~3-5%)
 */
async function getSpotDerivedRates() {
  try {
    const [spot, usdInr] = await Promise.all([getSpotRates(), getForex()]);
    // Gold MCX premium ~15% customs + 3% GST + ~4% other = ~1.235 factor
    const goldPer10g  = Math.round((spot.xauUsd / 31.1035) * 10   * usdInr * 1.235);
    // Silver MCX premium ~15% + 3% GST + ~4% = ~1.22 factor
    const silverPerKg = Math.round((spot.xagUsd / 31.1035) * 1000 * usdInr * 1.22);
    console.log('[SPOT_DERIVED] usdInr=' + usdInr + ' gold=' + goldPer10g + ' silver=' + silverPerKg);
    return { goldPer10g, silverPerKg, spotSrc: spot.src, usdInr };
  } catch (e) {
    console.log('[SPOT_DERIVED] failed:', e.message);
    return null;
  }
}

// ───────────────────────────────────────────────────────────────
// ROUTES
// ───────────────────────────────────────────────────────────────

// Root - server status
app.get('/', (req, res) => res.json({
  server:       'RR Jewellers Gold Server v13 - PRODUCTION',
  endpoints:    ['/rates', '/debug', '/cache-status', '/login-test', '/spot-test', '/updates'],
  cacheBuilt:   cacheBuiltAt ? new Date(cacheBuiltAt).toISOString() : 'not yet',
  mcxOpen:      isMCXOpen(),
  lastRateAt:   lastKnownRates?.timestamp || null,
  goldContracts:   getContracts(GOLD_M),
  silverContracts: getContracts(SILVER_M),
  ist:          (() => { const i = getIST(); return i.year+'-'+(i.month+1)+'-'+i.day+' '+i.hour+':'+String(i.min).padStart(2,'0')+' IST'; })(),
}));

// Angel login health check
app.get('/login-test', async (req, res) => {
  try {
    const jwt = await login();
    res.json({ success: true, preview: jwt.slice(0, 20) + '...' });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// International spot price check (no Angel auth needed)
app.get('/spot-test', async (req, res) => {
  try {
    const [spot, usdInr] = await Promise.all([getSpotRates(), getForex()]);
    const g = Math.round((spot.xauUsd / 31.1035) * 10   * usdInr * 1.235);
    const s = Math.round((spot.xagUsd / 31.1035) * 1000 * usdInr * 1.22);
    res.json({ spot, usdInr, goldMCX_approx: g, silverMCX_approx: s });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// View all cached contract tokens from scrip master
app.get('/cache-status', async (req, res) => {
  await ensureCache();
  const gKeys = Object.keys(tokenCache['GOLD']   || {}).sort();
  const sKeys = Object.keys(tokenCache['SILVER'] || {}).sort();
  res.json({
    cacheBuiltAt:    cacheBuiltAt ? new Date(cacheBuiltAt).toISOString() : null,
    goldContracts:   gKeys.map(k => ({ label: k, ...tokenCache['GOLD'][k]   })),
    silverContracts: sKeys.map(k => ({ label: k, ...tokenCache['SILVER'][k] })),
    activeGold:      getContracts(GOLD_M),
    activeSilver:    getContracts(SILVER_M),
  });
});

// Full debug endpoint
app.get('/debug', async (req, res) => {
  try {
    const jwt = await login();
    const gC  = getContracts(GOLD_M);
    const sC  = getContracts(SILVER_M);

    const [gTok, sTok] = await Promise.all([
      findToken(jwt, 'GOLD',   gC[0]),
      findToken(jwt, 'SILVER', sC[0]),
    ]);
    const [gRaw, sRaw] = await Promise.all([
      searchScrip(jwt, 'GOLD'),
      searchScrip(jwt, 'SILVER'),
    ]);

    res.json({
      wantedContracts:  { gold: gC, silver: sC },
      goldTokenFound:   gTok,
      silverTokenFound: sTok,
      goldRawSearch:    gRaw.map(r => ({ sym: r.tradingsymbol, tok: r.symboltoken })),
      silverRawSearch:  sRaw.map(r => ({ sym: r.tradingsymbol, tok: r.symboltoken })),
      cacheGoldKeys:    Object.keys(tokenCache['GOLD']   || {}),
      cacheSilverKeys:  Object.keys(tokenCache['SILVER'] || {}),
      mcxOpen:          isMCXOpen(),
      lastKnownRatesAt: lastKnownRates?.timestamp || null,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Google Sheets announcements / updates
app.get('/updates', async (req, res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID env not set');
    const url  = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r    = await axios.get(url, { timeout: 8000 });
    const json = r.data.replace(/.*?({.*}).*/s, '$1');
    const data = JSON.parse(json);
    const rows = data.table.rows.map(row => ({
      date:    row.c[0]?.v || '',
      title:   row.c[1]?.v || '',
      content: row.c[2]?.v || '',
      image:   row.c[3]?.v || '',
    }));
    res.json({ success: true, updates: rows.filter(r => r.title) });
  } catch (e) {
    res.json({
      success: true,
      updates: [{
        date:    'Today',
        title:   'Welcome to R.R. Jewellers',
        content: 'Live gold & silver rates. Contact us for best prices!',
        image:   '',
      }],
    });
  }
});

// ───────────────────────────────────────────────────────────────
// /rates  -  MAIN ENDPOINT
//
// Fallback chain:
//   1. Angel MCX live quote  (real-time market price, LTP > 0)
//   2. lastKnownRates        (last good MCX price from this session)
//   3. Spot-derived estimate (XAU/XAG → INR, market closed)
//   4. Error response        (all sources failed)
// ───────────────────────────────────────────────────────────────
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  let liveErr = null;

  try {
    // Step 1: Authenticate
    const jwt = await login();

    // Step 2: Determine active contracts
    const gC = getContracts(GOLD_M);
    const sC = getContracts(SILVER_M);
    console.log('[RATES] Contracts - GOLD:', gC, '  SILVER:', sC);

    // Step 3: Resolve symbol tokens
    const [gCurTok, gNxtTok, sCurTok, sNxtTok] = await Promise.all([
      findToken(jwt, 'GOLD',   gC[0]),
      findToken(jwt, 'GOLD',   gC[1]),
      findToken(jwt, 'SILVER', sC[0]),
      findToken(jwt, 'SILVER', sC[1]),
    ]);

    if (!gCurTok?.symboltoken)
      throw new Error('GOLD ' + gC[0] + ' token not found - check /debug');
    if (!sCurTok?.symboltoken)
      throw new Error('SILVER ' + sC[0] + ' token not found - check /debug');

    console.log('[RATES] Tokens OK - GOLD=' + gCurTok.symboltoken +
      '(' + gCurTok.tradingsymbol + ')  SILVER=' + sCurTok.symboltoken +
      '(' + sCurTok.tradingsymbol + ')');

    // Step 4: Fetch live quotes
    const [gCurr, sCurr, gNextRaw, sNextRaw] = await Promise.all([
      getQuote(jwt, gCurTok.symboltoken),
      getQuote(jwt, sCurTok.symboltoken),
      gNxtTok?.symboltoken ? getQuote(jwt, gNxtTok.symboltoken) : Promise.resolve(null),
      sNxtTok?.symboltoken ? getQuote(jwt, sNxtTok.symboltoken) : Promise.resolve(null),
    ]);

    if (!gCurr) throw new Error('GOLD quote network error - Angel API unreachable');
    if (!sCurr) throw new Error('SILVER quote network error - Angel API unreachable');

    if (gCurr.ltp === 0 || sCurr.ltp === 0) {
      console.log('[RATES] LTP=0 received - market closed/pre-open');
      throw new Error('GOLD quote returned LTP=0 — market may be closed');
    }

    const gNext = gNextRaw?.ltp > 0 ? gNextRaw : gCurr;
    const sNext = sNextRaw?.ltp > 0 ? sNextRaw : sCurr;

    // Step 5: Build success response
    const payload = {
      success:    true,
      source:     'angel_mcx_live',
      marketOpen: true,
      contracts: {
        gold:   { current: gC[0], next: gC[1], currentSymbol: gCurTok.tradingsymbol },
        silver: { current: sC[0], next: sC[1], currentSymbol: sCurTok.tradingsymbol },
      },
      goldPer10g:  Math.round(gCurr.ltp),
      silverPerKg: Math.round(sCurr.ltp),
      futures: {
        gold: {
          ltp:  Math.round(gCurr.ltp),
          bid:  Math.round(gCurr.bid),
          ask:  Math.round(gCurr.ask),
          high: Math.round(gCurr.high),
          low:  Math.round(gCurr.low),
          open: Math.round(gCurr.open),
        },
        silver: {
          ltp:  Math.round(sCurr.ltp),
          bid:  Math.round(sCurr.bid),
          ask:  Math.round(sCurr.ask),
          high: Math.round(sCurr.high),
          low:  Math.round(sCurr.low),
          open: Math.round(sCurr.open),
        },
        goldNext: {
          ltp:  Math.round(gNext.ltp),
          bid:  Math.round(gNext.bid),
          ask:  Math.round(gNext.ask),
          high: Math.round(gNext.high  || gNext.ltp * 1.003),
          low:  Math.round(gNext.low   || gNext.ltp * 0.994),
        },
        silverNext: {
          ltp:  Math.round(sNext.ltp),
          bid:  Math.round(sNext.bid),
          ask:  Math.round(sNext.ask),
          high: Math.round(sNext.high  || sNext.ltp * 1.012),
          low:  Math.round(sNext.low   || sNext.ltp * 0.984),
        },
      },
      timestamp: new Date().toISOString(),
    };

    lastKnownRates = payload;
    return res.json(payload);

  } catch (err) {
    liveErr = err;
    console.log('[RATES] Live failed:', err.message);
  }

  // ── Fallback 1: Last known real MCX price ──────────────────
  if (lastKnownRates) {
    console.log('[RATES] Using lastKnownRates from', lastKnownRates.timestamp);
    return res.json({
      ...lastKnownRates,
      source:     'last_known_rates',
      marketOpen: marketOpen,
      note:       marketOpen
        ? 'Live fetch failed - showing last known MCX price'
        : 'MCX market closed - showing last closing price',
      priceAsOf:  lastKnownRates.timestamp,
      timestamp:  new Date().toISOString(),
    });
  }

  // ── Fallback 2: Spot-derived estimate ─────────────────────
  console.log('[RATES] No lastKnownRates - trying spot-derived...');
  const derived = await getSpotDerivedRates();
  if (derived) {
    const g = derived.goldPer10g;
    const s = derived.silverPerKg;
    const gC = getContracts(GOLD_M);
    const sC = getContracts(SILVER_M);
    return res.json({
      success:    true,
      source:     'spot_derived',
      marketOpen: marketOpen,
      note:       'MCX data unavailable - price estimated from international spot rates (XAU/XAG → INR)',
      spotSource: derived.spotSrc,
      usdInr:     derived.usdInr,
      contracts: {
        gold:   { current: gC[0], next: gC[1] },
        silver: { current: sC[0], next: sC[1] },
      },
      goldPer10g:  g,
      silverPerKg: s,
      futures: {
        gold:       { ltp: g, bid: g, ask: g, high: 0, low: 0, open: 0 },
        silver:     { ltp: s, bid: s, ask: s, high: 0, low: 0, open: 0 },
        goldNext:   { ltp: g, bid: g, ask: g, high: 0, low: 0 },
        silverNext: { ltp: s, bid: s, ask: s, high: 0, low: 0 },
      },
      timestamp: new Date().toISOString(),
    });
  }

  // ── Fallback 3: Total failure ──────────────────────────────
  console.log('[RATES ERROR] All sources exhausted:', liveErr?.message);
  return res.status(500).json({
    success:    false,
    source:     'error',
    error:      liveErr?.message || 'All data sources failed',
    marketOpen: marketOpen,
    debug_url:  '/debug',
    cache_url:  '/cache-status',
    timestamp:  new Date().toISOString(),
  });
});

// ───────────────────────────────────────────────────────────────
// SERVER START
// ───────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log('═══════════════════════════════════════════');
  console.log(' RR Jewellers Gold Server v13 - port ' + PORT);
  console.log('═══════════════════════════════════════════');

  // Build scrip token cache immediately on startup
  await buildScripCache();

  // Rebuild daily at ~8 AM IST (2:30 UTC) and if cache goes stale
  setInterval(async () => {
    const utcH = new Date().getUTCHours();
    const utcM = new Date().getUTCMinutes();
    if ((utcH === 2 && utcM >= 30) || Date.now() - cacheBuiltAt > CACHE_TTL) {
      await buildScripCache();
    }
  }, 30 * 60 * 1000);

  // Ping self to prevent Render free-tier cold starts (every 4 min)
  setInterval(() => {
    require('https').get(SELF_URL, () => {
      console.log('[PING] awake');
    }).on('error', () => {});
  }, 4 * 60 * 1000);
});
