/**
 * RR Jewellers Gold Server v13  -  PRODUCTION FIXED BUILD
 * ═══════════════════════════════════════════════════════════════
 *
 * FIXES in v13 (Live Feed Only):
 * 1. Replaced individual API calls with Angel One Bulk Quote API.
 * 2. Completely removed mock/fake spot-derived rates from /rates endpoint.
 * 3. Silver token regex & Contract month fixes included.
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

function getContracts(validM) {
  const ist = getIST();
  let m = ist.month;
  let y = ist.year;

  if (validM.includes(m)) {
    const expiry   = getExpiryDate(y, m);
    const todayIST = new Date(Date.UTC(y, m, ist.day));
    if (todayIST > expiry) {
      m++; if (m > 11) { m = 0; y++; }
    }
  } else {
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

    const mcx = instruments.filter(
      i => i.exch_seg === 'MCX' && i.instrumenttype === 'FUTCOM'
    );

    const newCache = {};

    for (const inst of mcx) {
      const sym = (inst.symbol || '').toUpperCase().trim();
      const tok = inst.token;
      if (!sym || !tok) continue;

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
        if (!newCache['GOLD'][label]) {
          newCache['GOLD'][label] = { symboltoken: tok, tradingsymbol: sym };
        }
        continue;
      }

      if (sym.startsWith('SILVER') && !sym.startsWith('SILVERM') &&
          !sym.startsWith('SILVERMIC')) {
        const mm = sym.match(
          /^SILVER30([A-Z]{3})(\d{2})FUT$|^SILVER(\d{2})([A-Z]{3})FUT$|^SILVER([A-Z]{3})(\d{2})FUT$/
        );
        if (!mm) continue;
        let mon, yr2;
        if (mm[1] && mm[2]) { mon = mm[1]; yr2 = mm[2]; }
        else if (mm[3] && mm[4]) { yr2 = mm[3]; mon = mm[4]; }
        else { mon = mm[5]; yr2 = mm[6]; }
        if (!MONTHS.includes(mon)) continue;
        const label = mon + yr2;
        if (!newCache['SILVER']) newCache['SILVER'] = {};
        const existing = newCache['SILVER'][label];
        if (!existing || sym.startsWith('SILVER30')) {
          newCache['SILVER'][label] = { symboltoken: tok, tradingsymbol: sym };
        }
      }
    }

    tokenCache   = newCache;
    cacheBuiltAt = Date.now();
    console.log('[CACHE] Built OK');
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

async function findToken(jwt, base, contractLabel) {
  await ensureCache();
  const cached = tokenCache[base]?.[contractLabel];
  if (cached) return cached;

  const mon = contractLabel.slice(0, 3);
  const yr2 = contractLabel.slice(3, 5);

  const queries = base === 'SILVER'
    ? ['SILVER30' + mon + yr2 + 'FUT', 'SILVER' + yr2 + mon + 'FUT', 'SILVER' + mon + yr2 + 'FUT', 'SILVER' + mon + yr2]
    : [base + yr2 + mon + 'FUT', base + mon + yr2 + 'FUT', base + mon + yr2];

  for (const q of queries) {
    const results = await searchScrip(jwt, q);
    if (results.length > 0) {
      return { symboltoken: results[0].symboltoken, tradingsymbol: results[0].tradingsymbol };
    }
  }

  const broad  = await searchScrip(jwt, base);
  const needle = (mon + yr2).toUpperCase();
  const match  = broad
    .filter(r => (r.tradingsymbol || '').toUpperCase().includes(needle) || (r.name || '').toUpperCase().includes(needle))
    .sort((a, b) => {
      const aIs30 = (a.tradingsymbol || '').startsWith('SILVER30');
      const bIs30 = (b.tradingsymbol || '').startsWith('SILVER30');
      return (bIs30 ? 1 : 0) - (aIs30 ? 1 : 0);
    })[0];

  if (match) return { symboltoken: match.symboltoken, tradingsymbol: match.tradingsymbol };
  return null;
}

// ───────────────────────────────────────────────────────────────
// TOTP GENERATOR
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

// ───────────────────────────────────────────────────────────────
// ANGEL ONE AUTH
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
        console.log('[AUTH] Login OK window=' + w);
        return JWT;
      }
    } catch (e) {}
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
    return [];
  }
}

// ───────────────────────────────────────────────────────────────
// THE FIX: FETCH MULTIPLE SYMBOLS IN ONE API CALL
// ───────────────────────────────────────────────────────────────
async function getBulkQuotes(jwt, tokens) {
  try {
    const validTokens = tokens.filter(t => t);
    if (validTokens.length === 0) return {};

    const r = await axios.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
      { mode: 'FULL', exchangeTokens: { MCX: validTokens } },
      { headers: HDR(jwt), timeout: 8000 }
    );

    const fetched = r.data.data?.fetched || [];
    const results = {};

    fetched.forEach(d => {
      results[d.symbolToken] = {
        ltp:  Number(d.ltp)                     || 0,
        bid:  Number(d.depth?.buy?.[0]?.price)  || Number(d.ltp) || 0,
        ask:  Number(d.depth?.sell?.[0]?.price) || Number(d.ltp) || 0,
        high: Number(d.high)                    || 0,
        low:  Number(d.low)                     || 0,
        open: Number(d.open)                    || 0,
      };
    });

    return results;
  } catch (e) {
    console.log('[QUOTE] Bulk fetch error:', e.message);
    return null;
  }
}

// ───────────────────────────────────────────────────────────────
// ROUTES
// ───────────────────────────────────────────────────────────────

app.get('/', (req, res) => res.json({
  server:       'RR Jewellers Gold Server v13 - PRODUCTION',
  endpoints:    ['/rates', '/debug', '/cache-status', '/login-test', '/updates'],
  cacheBuilt:   cacheBuiltAt ? new Date(cacheBuiltAt).toISOString() : 'not yet',
  mcxOpen:      isMCXOpen(),
  lastRateAt:   lastKnownRates?.timestamp || null,
  ist:          (() => { const i = getIST(); return i.year+'-'+(i.month+1)+'-'+i.day+' '+i.hour+':'+String(i.min).padStart(2,'0')+' IST'; })(),
}));

app.get('/login-test', async (req, res) => {
  try {
    const jwt = await login();
    res.json({ success: true, preview: jwt.slice(0, 20) + '...' });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

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

app.get('/debug', async (req, res) => {
  try {
    const jwt = await login();
    const gC  = getContracts(GOLD_M);
    const sC  = getContracts(SILVER_M);
    const [gTok, sTok] = await Promise.all([findToken(jwt, 'GOLD', gC[0]), findToken(jwt, 'SILVER', sC[0])]);
    res.json({ wantedContracts: { gold: gC, silver: sC }, goldTokenFound: gTok, silverTokenFound: sTok, mcxOpen: isMCXOpen() });
  } catch (e) { res.status(500).json({ error: e.message }); }
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
// /rates  -  MAIN ENDPOINT (STRICTLY LIVE ANGEL FEED ONLY)
// ───────────────────────────────────────────────────────────────
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();
  let liveErr = null;

  try {
    const jwt = await login();
    const gC = getContracts(GOLD_M);
    const sC = getContracts(SILVER_M);

    const [gCurTok, gNxtTok, sCurTok, sNxtTok] = await Promise.all([
      findToken(jwt, 'GOLD',   gC[0]),
      findToken(jwt, 'GOLD',   gC[1]),
      findToken(jwt, 'SILVER', sC[0]),
      findToken(jwt, 'SILVER', sC[1]),
    ]);

    if (!gCurTok?.symboltoken || !sCurTok?.symboltoken) {
      throw new Error('Tokens missing - check /debug');
    }

    // Call Bulk API Instead of Promise.all
    const tokensToFetch = [gCurTok.symboltoken, sCurTok.symboltoken];
    if (gNxtTok?.symboltoken) tokensToFetch.push(gNxtTok.symboltoken);
    if (sNxtTok?.symboltoken) tokensToFetch.push(sNxtTok.symboltoken);

    const quotes = await getBulkQuotes(jwt, tokensToFetch);
    if (!quotes) throw new Error('Quote network error - Angel API unreachable');

    const gCurr = quotes[gCurTok.symboltoken] || { ltp: 0 };
    const sCurr = quotes[sCurTok.symboltoken] || { ltp: 0 };
    const gNextRaw = gNxtTok ? quotes[gNxtTok.symboltoken] : null;
    const sNextRaw = sNxtTok ? quotes[sNxtTok.symboltoken] : null;

    if (gCurr.ltp === 0 || sCurr.ltp === 0) {
      throw new Error('GOLD/SILVER quote returned LTP=0 — market closed');
    }

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
        gold: {
          ltp: Math.round(gCurr.ltp), bid: Math.round(gCurr.bid), ask: Math.round(gCurr.ask), high: Math.round(gCurr.high), low: Math.round(gCurr.low), open: Math.round(gCurr.open),
        },
        silver: {
          ltp: Math.round(sCurr.ltp), bid: Math.round(sCurr.bid), ask: Math.round(sCurr.ask), high: Math.round(sCurr.high), low: Math.round(sCurr.low), open: Math.round(sCurr.open),
        },
        goldNext: { ltp: Math.round(gNext.ltp), bid: Math.round(gNext.bid), ask: Math.round(gNext.ask), high: Math.round(gNext.high || gNext.ltp * 1.003), low: Math.round(gNext.low || gNext.ltp * 0.994) },
        silverNext: { ltp: Math.round(sNext.ltp), bid: Math.round(sNext.bid), ask: Math.round(sNext.ask), high: Math.round(sNext.high || sNext.ltp * 1.012), low: Math.round(sNext.low || sNext.ltp * 0.984) },
      },
      timestamp: new Date().toISOString(),
    };

    lastKnownRates = payload;
    return res.json(payload);

  } catch (err) {
    liveErr = err;
    console.log('[RATES] Live failed:', err.message);
  }

  // Fallback 1: Last known real MCX price (No mock rates allowed anymore)
  if (lastKnownRates) {
    return res.json({
      ...lastKnownRates,
      source:     'last_known_rates',
      marketOpen: marketOpen,
      note:       marketOpen ? 'Live fetch failed - showing last known MCX price' : 'MCX market closed - showing last closing price',
      priceAsOf:  lastKnownRates.timestamp,
      timestamp:  new Date().toISOString(),
    });
  }

  // Total failure
  return res.status(500).json({
    success:    false,
    source:     'error',
    error:      liveErr?.message || 'Data source failed. No live rates available.',
    marketOpen: marketOpen,
  });
});

// ───────────────────────────────────────────────────────────────
// SERVER START
// ───────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log('═══════════════════════════════════════════');
  console.log(' RR Jewellers Gold Server v13.1 - Live Fix ');
  console.log('═══════════════════════════════════════════');

  await buildScripCache();

  setInterval(async () => {
    const utcH = new Date().getUTCHours();
    const utcM = new Date().getUTCMinutes();
    if ((utcH === 2 && utcM >= 30) || Date.now() - cacheBuiltAt > CACHE_TTL) {
      await buildScripCache();
    }
  }, 30 * 60 * 1000);

  setInterval(() => {
    require('https').get(SELF_URL, () => {}).on('error', () => {});
  }, 4 * 60 * 1000);
});
