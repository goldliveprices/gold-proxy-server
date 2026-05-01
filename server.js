/**
 * RR Jewellers Gold Server v14 - FULLY DEBUGGED
 * ═══════════════════════════════════════════════════════════════
 * BUGS FIXED vs v13:
 * 1.  getIST()           — missing closing `}`
 * 2.  getExpiryDate()    — missing closing `}`
 * 3.  getContracts()     — broken if/else brace structure → wrong contract
 * 4.  isMCXOpen()        — missing closing `}` + Saturday hours added
 * 5.  buildScripCache()  — GOLD/SILVER if-blocks missing closing `}`
 * 6.  ensureCache()      — missing closing `}` for if + function
 * 7.  findToken()        — SILVER array missing `]` (SyntaxError)
 * 8.  findToken()        — .filter() callback missing closing `)`
 * 9.  generateTOTP()     — for-loop + function missing closing `}`
 * 10. login()            — if/for/function all missing closing `}`
 * 11. searchScrip()      — try/catch missing closing `}`
 * 12. getQuote()         — try/catch missing closing `}`
 * 13. getSpotRates()     — 3 try/catch blocks missing closing `}`
 * 14. getForex()         — try/catch blocks missing closing `}`
 * 15. getSpotDerivedRates() — try/catch missing closing `}`
 * 16. /login-test route  — catch block missing closing `}`
 * 17. /rates             — LTP=0 if-block missing closing `}`
 * 18. frankfurter.app    — replaced (ECB has no INR). Now uses gold-api.com
 * 19. Self-ping          — switched to axios (proper HTTPS + error handling)
 * 20. Saturday hours     — MCX Commodity trades Sat 9AM-2PM IST (now enabled)
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
const CLIENT_ID   = process.env.CLIENT_ID   || 'AAAA238852';
const API_KEY     = process.env.API_KEY     || 'DPAHMIXr';
const TOTP_SECRET = process.env.TOTP_SECRET || 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN   = process.env.ANGEL_PIN   || '1857';
const SHEET_ID    = process.env.SHEET_ID    || '';
const SELF_URL    = process.env.SELF_URL    || 'https://gold-proxy-server.onrender.com';

// ───────────────────────────────────────────────────────────────
// MCX CONTRACT MONTHS (0-indexed JS months)
// GOLD  : FEB=1, APR=3, JUN=5, AUG=7, OCT=9, DEC=11
// SILVER: MAR=2, MAY=4, JUL=6, SEP=8, DEC=11
// ───────────────────────────────────────────────────────────────
const MONTHS   = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
const GOLD_M   = [1, 3, 5, 7, 9, 11];
const SILVER_M = [2, 4, 6, 8, 11];

// ───────────────────────────────────────────────────────────────
// FIX #1 + #2 + #3 + #4: All helper functions properly closed
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
} // FIX #1: closing brace added

function getExpiryDate(year, month) {
  const fifth = new Date(Date.UTC(year, month, 5));
  const dow   = fifth.getUTCDay();
  if (dow === 0) return new Date(Date.UTC(year, month, 3)); // Sun → Fri
  if (dow === 6) return new Date(Date.UTC(year, month, 4)); // Sat → Fri
  return fifth;
} // FIX #2: closing brace added

function getContracts(validM) {
  const ist = getIST();
  let m = ist.month;
  let y = ist.year;

  if (validM.includes(m)) {
    const expiry    = getExpiryDate(y, m);
    const todayIST  = new Date(Date.UTC(y, m, ist.day));
    if (todayIST > expiry) {
      m++; if (m > 11) { m = 0; y++; }
    }
    // else: current month contract still active — keep m as is
  } else {
    // FIX #3: this else was missing — without it both branches advanced m
    m++; if (m > 11) { m = 0; y++; }
  }

  const out = [];
  for (let i = 0; i < 24 && out.length < 2; i++) {
    if (validM.includes(m)) out.push(MONTHS[m] + y.toString().slice(-2));
    m++; if (m > 11) { m = 0; y++; }
  }
  return out;
} // FIX #3: proper brace structure

// FIX #4 + #20: isMCXOpen now supports Saturday 9AM-2PM
function isMCXOpen() {
  const { dow, hour, min } = getIST();
  if (dow === 0) return false; // Sunday always closed
  const timeM = hour * 60 + min;
  if (dow === 6) return timeM >= 9 * 60 && timeM < 14 * 60; // Sat 9AM-2PM
  return timeM >= 9 * 60 && timeM < 23 * 60 + 55;           // Mon-Fri 9AM-11:55PM
} // FIX #4: closing brace added

// ───────────────────────────────────────────────────────────────
// LAST-KNOWN RATES CACHE
// ───────────────────────────────────────────────────────────────
let lastKnownRates = null;

// ───────────────────────────────────────────────────────────────
// SCRIP MASTER CACHE
// ───────────────────────────────────────────────────────────────
let tokenCache   = {};
let cacheBuiltAt = 0;
const CACHE_TTL  = 22 * 60 * 60 * 1000;

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

    const mcx      = instruments.filter(i => i.exch_seg === 'MCX' && i.instrumenttype === 'FUTCOM');
    const newCache = {};

    for (const inst of mcx) {
      const sym = (inst.symbol || '').toUpperCase().trim();
      const tok = inst.token;
      if (!sym || !tok) continue;

      // ── GOLD (1 kg main contract)
      if (sym.startsWith('GOLD') && !sym.startsWith('GOLDM') &&
          !sym.startsWith('GOLDPETAL') && !sym.startsWith('GOLDGUINEA')) {
        const mm = sym.match(/^GOLD(\d{2})([A-Z]{3})FUT$|^GOLD([A-Z]{3})(\d{2})FUT$/);
        if (!mm) continue;
        let mon, yr2;
        if (mm[1] && mm[2]) { yr2 = mm[1]; mon = mm[2]; }
        else                 { mon = mm[3]; yr2 = mm[4]; }
        if (!MONTHS.includes(mon)) continue;
        const label = mon + yr2;
        if (!newCache['GOLD']) newCache['GOLD'] = {};
        if (!newCache['GOLD'][label])
          newCache['GOLD'][label] = { symboltoken: tok, tradingsymbol: sym };
        continue;
      } // FIX #5: GOLD if-block properly closed

      // ── SILVER (30 kg main contract)
      if (sym.startsWith('SILVER') && !sym.startsWith('SILVERM') &&
          !sym.startsWith('SILVERMIC')) {
        const mm = sym.match(
          /^SILVER30([A-Z]{3})(\d{2})FUT$|^SILVER(\d{2})([A-Z]{3})FUT$|^SILVER([A-Z]{3})(\d{2})FUT$/
        );
        if (!mm) continue;
        let mon, yr2;
        if (mm[1] && mm[2])      { mon = mm[1]; yr2 = mm[2]; }
        else if (mm[3] && mm[4]) { yr2 = mm[3]; mon = mm[4]; }
        else                     { mon = mm[5]; yr2 = mm[6]; }
        if (!MONTHS.includes(mon)) continue;
        const label    = mon + yr2;
        if (!newCache['SILVER']) newCache['SILVER'] = {};
        const existing = newCache['SILVER'][label];
        if (!existing || sym.startsWith('SILVER30'))
          newCache['SILVER'][label] = { symboltoken: tok, tradingsymbol: sym };
      } // FIX #5: SILVER if-block properly closed
    }

    tokenCache   = newCache;
    cacheBuiltAt = Date.now();
    const gCount = Object.keys(newCache['GOLD']   || {}).length;
    const sCount = Object.keys(newCache['SILVER'] || {}).length;
    console.log('[CACHE] Built OK - GOLD:' + gCount + ' SILVER:' + sCount);
    return true;
  } catch (e) {
    console.log('[CACHE] Build failed:', e.message);
    return false;
  }
} // FIX #5: function properly closed

async function ensureCache() {
  if (cacheBuiltAt === 0 || Date.now() - cacheBuiltAt > CACHE_TTL) {
    await buildScripCache();
  } // FIX #6: if-block closed
} // FIX #6: function closed

async function findToken(jwt, base, contractLabel) {
  await ensureCache();
  const cached = tokenCache[base]?.[contractLabel];
  if (cached) {
    console.log('[TOKEN] cache hit: ' + base + contractLabel + ' -> ' + cached.tradingsymbol);
    return cached;
  }

  console.log('[TOKEN] cache miss: ' + base + contractLabel + ' - trying searchScrip...');
  const mon = contractLabel.slice(0, 3);
  const yr2 = contractLabel.slice(3, 5);

  // FIX #7: SILVER array was missing closing `]`
  const queries = base === 'SILVER'
    ? [
        'SILVER30' + mon + yr2 + 'FUT',
        'SILVER'   + yr2 + mon + 'FUT',
        'SILVER'   + mon + yr2 + 'FUT',
        'SILVER'   + mon + yr2,
      ]
    : [
        base + yr2 + mon + 'FUT',
        base + mon + yr2 + 'FUT',
        base + mon + yr2,
      ];

  for (const q of queries) {
    const results = await searchScrip(jwt, q);
    if (results.length > 0) {
      const hit = results[0];
      console.log('[TOKEN] searchScrip hit: "' + q + '" -> ' + hit.tradingsymbol);
      return { symboltoken: hit.symboltoken, tradingsymbol: hit.tradingsymbol };
    }
  }

  const broad  = await searchScrip(jwt, base);
  const needle = (mon + yr2).toUpperCase();
  // FIX #8: .filter() callback properly closed with `)`
  const match  = broad
    .filter(r =>
      (r.tradingsymbol || '').toUpperCase().includes(needle) ||
      (r.name         || '').toUpperCase().includes(needle)
    )
    .sort((a, b) => {
      const aIs30 = (a.tradingsymbol || '').startsWith('SILVER30');
      const bIs30 = (b.tradingsymbol || '').startsWith('SILVER30');
      return (bIs30 ? 1 : 0) - (aIs30 ? 1 : 0);
    })[0];

  if (match) {
    console.log('[TOKEN] broad match: ' + base + contractLabel + ' -> ' + match.tradingsymbol);
    return { symboltoken: match.symboltoken, tradingsymbol: match.tradingsymbol };
  }

  console.log('[TOKEN] FAILED - no token for ' + base + contractLabel);
  return null;
} // FIX #8

// ───────────────────────────────────────────────────────────────
// TOTP GENERATOR — FIX #9: properly closed
// ───────────────────────────────────────────────────────────────
function generateTOTP(secret, offset) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const v = alpha.indexOf(c);
    if (v >= 0) bits += v.toString(2).padStart(5, '0');
  } // FIX #9: for-loop closed
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
} // FIX #9: function closed

// ───────────────────────────────────────────────────────────────
// AUTH — FIX #10: all blocks properly closed
// ───────────────────────────────────────────────────────────────
let JWT = null, JWT_EXP = 0;

const HDR = (jwt) => ({
  'Content-Type':      'application/json',
  'Accept':            'application/json',
  'X-UserType':        'USER',
  'X-SourceID':        'WEB',
  'X-ClientLocalIP':   '127.0.0.1',
  'X-ClientPublicIP':  '74.220.52.100',
  'X-MACAddress':      'fe:80:00:00:00:00',
  'X-PrivateKey':      API_KEY,
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
        console.log('[AUTH] Login OK window=' + w);
        return JWT;
      } // FIX #10: if-block closed
    } catch (e) { /* try next TOTP window */ }
  } // FIX #10: for-loop closed
  throw new Error('Angel login failed - check credentials / IP whitelist');
} // FIX #10: function closed

// ───────────────────────────────────────────────────────────────
// ANGEL API HELPERS — FIX #11 #12: try/catch properly closed
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
} // FIX #11

async function getQuote(jwt, symboltoken) {
  try {
    const r = await axios.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
      { mode: 'FULL', exchangeTokens: { MCX: [String(symboltoken)] } },
      { headers: HDR(jwt), timeout: 8000 }
    );
    const d = r.data.data?.fetched?.[0] || {};
    return {
      ltp:  Number(d.ltp)                      || 0,
      bid:  Number(d.depth?.buy?.[0]?.price)   || Number(d.ltp) || 0,
      ask:  Number(d.depth?.sell?.[0]?.price)  || Number(d.ltp) || 0,
      high: Number(d.high)                     || 0,
      low:  Number(d.low)                      || 0,
      open: Number(d.open)                     || 0,
    };
  } catch (e) {
    console.log('[QUOTE] error token ' + symboltoken + ': ' + e.message.slice(0, 60));
    return null;
  }
} // FIX #12

// ───────────────────────────────────────────────────────────────
// SPOT RATES — FIX #13 + #18: try/catch closed, frankfurter replaced
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
  } catch (e) { console.log('[SPOT] metals.live fail'); } // FIX #13

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
  } catch (e) { console.log('[SPOT] goldprice.org fail'); } // FIX #13

  // Source 3: gold-api.com (FIX #18: replaces fxratesapi which inverts badly)
  try {
    const [gR, sR] = await Promise.all([
      axios.get('https://www.gold-api.com/price/XAU', { timeout: 6000 }),
      axios.get('https://www.gold-api.com/price/XAG', { timeout: 6000 }),
    ]);
    const xauUsd = gR.data?.price;
    const xagUsd = sR.data?.price;
    if (xauUsd > 2000 && xauUsd < 10000 && xagUsd > 20 && xagUsd < 500) {
      console.log('[SPOT] gold-api.com ok: xau=' + xauUsd + ' xag=' + xagUsd);
      return { xauUsd, xagUsd, src: 'gold-api.com' };
    }
  } catch (e) { console.log('[SPOT] gold-api.com fail'); } // FIX #13

  console.log('[SPOT] All sources failed - using fixed fallback');
  return { xauUsd: 3330, xagUsd: 33.0, src: 'fixed_fallback' };
}

// ───────────────────────────────────────────────────────────────
// FOREX — FIX #14 + #18: frankfurter replaced with reliable sources
// ───────────────────────────────────────────────────────────────
async function getForex() {
  const FX_MIN = 82, FX_MAX = 88;

  // Source 1: open.er-api (free, no key, reliable INR data)
  try {
    const r    = await axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 5000 });
    const rate = r.data?.rates?.INR;
    if (rate > FX_MIN && rate < FX_MAX) { console.log('[FOREX] open.er-api: ' + rate); return rate; }
    else console.log('[FOREX] open.er-api out-of-range: ' + rate);
  } catch (e) { console.log('[FOREX] open.er-api fail'); } // FIX #14

  // Source 2: exchangerate-api.com
  try {
    const r    = await axios.get('https://api.exchangerate-api.com/v4/latest/USD', { timeout: 5000 });
    const rate = r.data?.rates?.INR;
    if (rate > FX_MIN && rate < FX_MAX) { console.log('[FOREX] exchangerate-api: ' + rate); return rate; }
    else console.log('[FOREX] exchangerate-api out-of-range: ' + rate);
  } catch (e) { console.log('[FOREX] exchangerate-api fail'); } // FIX #14

  // Source 3: gold-api.com INR (FIX #18: replaces non-functional frankfurter)
  try {
    const r    = await axios.get('https://www.gold-api.com/price/XAU?currency=INR', { timeout: 5000 });
    const xauInr = r.data?.price;
    // Derive USD/INR: XAU/INR ÷ XAU/USD (use ~3330 as approx XAU/USD)
    if (xauInr > 200000) {
      const derived = parseFloat((xauInr / 3330).toFixed(2));
      if (derived > FX_MIN && derived < FX_MAX) {
        console.log('[FOREX] gold-api derived INR: ' + derived);
        return derived;
      }
    }
  } catch (e) { console.log('[FOREX] gold-api INR fail'); } // FIX #14 #18

  console.log('[FOREX] All failed - using hardcoded 84.50');
  return 84.50;
}

async function getSpotDerivedRates() {
  try {
    const [spot, usdInr] = await Promise.all([getSpotRates(), getForex()]);
    const goldPer10g  = Math.round((spot.xauUsd / 31.1035) * 10   * usdInr * 1.103);
    const silverPerKg = Math.round((spot.xagUsd / 31.1035) * 1000 * usdInr * 1.103);
    console.log('[SPOT_DERIVED] usdInr=' + usdInr + ' xau=' + spot.xauUsd + ' gold=' + goldPer10g + ' silver=' + silverPerKg);
    return { goldPer10g, silverPerKg, spotSrc: spot.src, usdInr };
  } catch (e) {
    console.log('[SPOT_DERIVED] failed:', e.message);
    return null;
  }
} // FIX #15

// ───────────────────────────────────────────────────────────────
// ROUTES
// ───────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.json({
  server:          'RR Jewellers Gold Server v14 - PRODUCTION',
  endpoints:       ['/rates', '/debug', '/cache-status', '/login-test', '/spot-test', '/forex-test', '/updates'],
  cacheBuilt:      cacheBuiltAt ? new Date(cacheBuiltAt).toISOString() : 'not yet',
  mcxOpen:         isMCXOpen(),
  lastRateAt:      lastKnownRates?.timestamp || null,
  goldContracts:   getContracts(GOLD_M),
  silverContracts: getContracts(SILVER_M),
  ist: (() => { const i = getIST(); return i.year+'-'+(i.month+1)+'-'+i.day+' '+i.hour+':'+String(i.min).padStart(2,'0')+' IST'; })(),
}));

// FIX #16: catch block properly closed
app.get('/login-test', async (req, res) => {
  try {
    const jwt = await login();
    res.json({ success: true, preview: jwt.slice(0, 20) + '...' });
  } catch (e) {
    res.json({ success: false, error: e.message });
  } // FIX #16
});

app.get('/forex-test', async (req, res) => {
  const rate = await getForex();
  res.json({ usdInr: rate, note: 'Valid range: 82-88. Fallback: 84.50' });
});

app.get('/spot-test', async (req, res) => {
  try {
    const [spot, usdInr] = await Promise.all([getSpotRates(), getForex()]);
    const g = Math.round((spot.xauUsd / 31.1035) * 10   * usdInr * 1.103);
    const s = Math.round((spot.xagUsd / 31.1035) * 1000 * usdInr * 1.103);
    res.json({ spot, usdInr, goldMCX_approx: g, silverMCX_approx: s });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/cache-status', async (req, res) => {
  await ensureCache();
  const gKeys = Object.keys(tokenCache['GOLD']   || {}).sort();
  const sKeys = Object.keys(tokenCache['SILVER'] || {}).sort();
  res.json({
    cacheBuiltAt:    cacheBuiltAt ? new Date(cacheBuiltAt).toISOString() : null,
    goldContracts:   gKeys.map(k => ({ label: k, ...tokenCache['GOLD'][k] })),
    silverContracts: sKeys.map(k => ({ label: k, ...tokenCache['SILVER'][k] })),
    activeGold:      getContracts(GOLD_M),
    activeSilver:    getContracts(SILVER_M),
  });
});

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

app.get('/updates', async (req, res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID env not set');
    const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r   = await axios.get(url, { timeout: 8000 });
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
    res.json({ success: true, updates: [{
      date: 'Today', title: 'Welcome to R.R. Jewellers',
      content: 'Live gold & silver rates. Contact us for best prices!', image: '',
    }]});
  }
});

// ───────────────────────────────────────────────────────────────
// /rates — MAIN ENDPOINT — FIX #17: LTP=0 if-block closed
// ───────────────────────────────────────────────────────────────
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  let liveErr = null;

  try {
    const jwt = await login();
    const gC  = getContracts(GOLD_M);
    const sC  = getContracts(SILVER_M);
    console.log('[RATES] Contracts - GOLD:', gC, ' SILVER:', sC);

    const [gCurTok, gNxtTok, sCurTok, sNxtTok] = await Promise.all([
      findToken(jwt, 'GOLD',   gC[0]),
      findToken(jwt, 'GOLD',   gC[1]),
      findToken(jwt, 'SILVER', sC[0]),
      findToken(jwt, 'SILVER', sC[1]),
    ]);

    if (!gCurTok?.symboltoken) throw new Error('GOLD ' + gC[0] + ' token not found');
    if (!sCurTok?.symboltoken) throw new Error('SILVER ' + sC[0] + ' token not found');

    const [gCurr, sCurr, gNextRaw, sNextRaw] = await Promise.all([
      getQuote(jwt, gCurTok.symboltoken),
      getQuote(jwt, sCurTok.symboltoken),
      gNxtTok?.symboltoken ? getQuote(jwt, gNxtTok.symboltoken) : Promise.resolve(null),
      sNxtTok?.symboltoken ? getQuote(jwt, sNxtTok.symboltoken) : Promise.resolve(null),
    ]);

    if (!gCurr) throw new Error('GOLD quote network error');
    if (!sCurr) throw new Error('SILVER quote network error');

    if (gCurr.ltp === 0 || sCurr.ltp === 0) {
      console.log('[RATES] LTP=0 received - market closed/pre-open');
      throw new Error('LTP=0 — market may be closed');
    } // FIX #17: if-block properly closed

    const gNext = gNextRaw?.ltp > 0 ? gNextRaw : gCurr;
    const sNext = sNextRaw?.ltp > 0 ? sNextRaw : sCurr;

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
        gold:       { ltp: Math.round(gCurr.ltp), bid: Math.round(gCurr.bid), ask: Math.round(gCurr.ask), high: Math.round(gCurr.high), low: Math.round(gCurr.low), open: Math.round(gCurr.open) },
        silver:     { ltp: Math.round(sCurr.ltp), bid: Math.round(sCurr.bid), ask: Math.round(sCurr.ask), high: Math.round(sCurr.high), low: Math.round(sCurr.low), open: Math.round(sCurr.open) },
        goldNext:   { ltp: Math.round(gNext.ltp), bid: Math.round(gNext.bid), ask: Math.round(gNext.ask), high: Math.round(gNext.high || gNext.ltp * 1.003), low: Math.round(gNext.low || gNext.ltp * 0.994) },
        silverNext: { ltp: Math.round(sNext.ltp), bid: Math.round(sNext.bid), ask: Math.round(sNext.ask), high: Math.round(sNext.high || sNext.ltp * 1.012), low: Math.round(sNext.low || sNext.ltp * 0.984) },
      },
      timestamp: new Date().toISOString(),
    };

    lastKnownRates = payload;
    return res.json(payload);

  } catch (err) {
    liveErr = err;
    console.log('[RATES] Live failed:', err.message);

    if (lastKnownRates) {
      console.log('[RATES] Using lastKnownRates from', lastKnownRates.timestamp);
      return res.json({
        ...lastKnownRates,
        source:     'last_known_rates',
        marketOpen: marketOpen,
        note:       marketOpen ? 'Live fetch failed - showing last known MCX price' : 'MCX market closed - showing last closing price',
        priceAsOf:  lastKnownRates.timestamp,
        timestamp:  new Date().toISOString(),
      });
    }

    console.log('[RATES] No lastKnownRates - trying spot-derived...');
    const derived = await getSpotDerivedRates();
    if (derived) {
      const g  = derived.goldPer10g;
      const s  = derived.silverPerKg;
      const gC = getContracts(GOLD_M);
      const sC = getContracts(SILVER_M);
      return res.json({
        success: true, source: 'spot_derived', marketOpen,
        note:       'MCX data unavailable - estimated from international spot rates',
        spotSource: derived.spotSrc, usdInr: derived.usdInr,
        contracts:  { gold: { current: gC[0], next: gC[1] }, silver: { current: sC[0], next: sC[1] } },
        goldPer10g: g, silverPerKg: s,
        futures: {
          gold:       { ltp: g, bid: g, ask: g, high: 0, low: 0, open: 0 },
          silver:     { ltp: s, bid: s, ask: s, high: 0, low: 0, open: 0 },
          goldNext:   { ltp: g, bid: g, ask: g, high: 0, low: 0 },
          silverNext: { ltp: s, bid: s, ask: s, high: 0, low: 0 },
        },
        timestamp: new Date().toISOString(),
      });
    }

    return res.status(500).json({
      success: false, source: 'error',
      error:      liveErr?.message || 'All data sources failed',
      marketOpen: marketOpen,
      debug_url:  '/debug',
      cache_url:  '/cache-status',
      timestamp:  new Date().toISOString(),
    });
  }
});

// ───────────────────────────────────────────────────────────────
// SERVER START — FIX #19: self-ping uses axios
// ───────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log('═══════════════════════════════════════════');
  console.log(' RR Jewellers Gold Server v14 - port ' + PORT);
  console.log('═══════════════════════════════════════════');

  await buildScripCache();

  setInterval(async () => {
    const utcH = new Date().getUTCHours();
    const utcM = new Date().getUTCMinutes();
    if ((utcH === 2 && utcM >= 30) || Date.now() - cacheBuiltAt > CACHE_TTL) {
      await buildScripCache();
    }
  }, 30 * 60 * 1000);

  // FIX #19: use axios instead of inline require('https')
  setInterval(() => {
    axios.get(SELF_URL + '/ping').catch(() => {});
  }, 4 * 60 * 1000);
});

// /ping for self-keep-alive
app.get('/ping', (req, res) => res.json({ ok: true, ts: Date.now() }));
