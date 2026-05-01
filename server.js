'use strict';

const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const app     = express();

app.use(cors());
app.use(express.json());

// ─── CREDENTIALS ────────────────────────────────────────────────
const CLIENT_ID   = process.env.CLIENT_ID   || 'AAAA238852';
const API_KEY     = process.env.API_KEY     || 'DPAHMIXr';
const TOTP_SECRET = process.env.TOTP_SECRET || 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN   = process.env.ANGEL_PIN   || '1857';
const SHEET_ID    = process.env.SHEET_ID    || '';
const SELF_URL    = process.env.SELF_URL    || 'https://gold-proxy-server.onrender.com';
const PORT        = process.env.PORT        || 3000;

// ─── MCX CONTRACT CONFIG ─────────────────────────────────────────
const MONTHS   = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
const GOLD_M   = [1, 3, 5, 7, 9, 11];
const SILVER_M = [2, 4, 6, 8, 11];

// ─── HARDCODED FALLBACK TOKENS ───────────────────────────────────
const FALLBACK_TOKENS = {
  GOLD:   {
    JUN26: { symboltoken: '234230', tradingsymbol: 'GOLD26JUNFUT'    },
    AUG26: { symboltoken: '234232', tradingsymbol: 'GOLD26AUGFUT'    },
  },
  SILVER: {
    MAY26: { symboltoken: '234250', tradingsymbol: 'SILVER30MAY26FUT' },
    JUL26: { symboltoken: '234252', tradingsymbol: 'SILVER30JUL26FUT' },
  },
};

// ─── IST HELPERS ────────────────────────────────────────────────
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

function getContracts(validM) {
  const ist = getIST();
  let m = ist.month, y = ist.year;
  if (validM.includes(m)) {
    const expiry   = getExpiryDate(y, m);
    const todayIST = new Date(Date.UTC(y, m, ist.day));
    if (todayIST > expiry) { m++; if (m > 11) { m = 0; y++; } }
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

function isMCXOpen() {
  const { dow, hour, min } = getIST();
  if (dow === 0) return false;
  const t = hour * 60 + min;
  if (dow === 6) return t >= 540 && t < 840;  // Sat 9AM-2PM IST
  return t >= 540 && t < 1435;                 // Mon-Fri 9AM-11:55PM IST
}

// ─── TOTP ────────────────────────────────────────────────────────
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
  const key = Buffer.from(bytes);
  const t   = Math.floor(Date.now() / 30000) + (offset || 0);
  const tb  = Buffer.alloc(8);
  tb.writeUInt32BE(Math.floor(t / 0x100000000), 0);
  tb.writeUInt32BE(t >>> 0, 4);
  const hmac = crypto.createHmac('sha1', key).update(tb).digest();
  const off  = hmac[hmac.length - 1] & 0xf;
  return (((hmac[off] & 0x7f) << 24 | (hmac[off+1] & 0xff) << 16 |
           (hmac[off+2] & 0xff) << 8  | (hmac[off+3] & 0xff)) % 1000000)
    .toString().padStart(6, '0');
}

// ─── AUTH ────────────────────────────────────────────────────────
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
  for (const w of [-4,-3,-2,-1,0,1,2,3,4]) {
    try {
      const r = await axios.post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        { clientcode: CLIENT_ID, password: ANGEL_PIN, totp: generateTOTP(TOTP_SECRET, w) },
        { headers: HDR(), timeout: 12000 }
      );
      if (r.data?.status && r.data?.data?.jwtToken) {
        JWT     = r.data.data.jwtToken;
        JWT_EXP = Date.now() + 6 * 60 * 60 * 1000;
        console.log('[AUTH] OK window=' + w);
        return JWT;
      }
    } catch (e) { /* try next window */ }
  }
  throw new Error('Angel login failed — Render IP not whitelisted in Angel SmartAPI. Use /spot-test for live rates.');
}

// ─── SCRIP CACHE ─────────────────────────────────────────────────
let tokenCache = {}, cacheBuiltAt = 0;
const CACHE_TTL = 22 * 3600 * 1000;

async function buildScripCache() {
  try {
    console.log('[CACHE] Downloading scrip master...');
    const r = await axios.get(
      'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json',
      { timeout: 30000 }
    );
    if (!Array.isArray(r.data) || !r.data.length) throw new Error('Empty scrip master');

    const nc = { GOLD: {}, SILVER: {} };
    for (const inst of r.data) {
      if (inst.exch_seg !== 'MCX' || inst.instrumenttype !== 'FUTCOM') continue;
      const sym = (inst.symbol || '').toUpperCase().trim();
      const tok = String(inst.token || '');
      if (!sym || !tok) continue;

      if (sym.startsWith('GOLD') && !sym.startsWith('GOLDM') &&
          !sym.startsWith('GOLDPETAL') && !sym.startsWith('GOLDGUINEA')) {
        const mm = sym.match(/^GOLD(\d{2})([A-Z]{3})FUT$|^GOLD([A-Z]{3})(\d{2})FUT$/);
        if (!mm) continue;
        const mon = mm[2] || mm[3], yr2 = mm[1] || mm[4];
        if (!MONTHS.includes(mon)) continue;
        const lbl = mon + yr2;
        if (!nc.GOLD[lbl]) nc.GOLD[lbl] = { symboltoken: tok, tradingsymbol: sym };
        continue;
      }

      if (sym.startsWith('SILVER') && !sym.startsWith('SILVERM') &&
          !sym.startsWith('SILVERMIC')) {
        const mm = sym.match(
          /^SILVER30([A-Z]{3})(\d{2})FUT$|^SILVER(\d{2})([A-Z]{3})FUT$|^SILVER([A-Z]{3})(\d{2})FUT$/
        );
        if (!mm) continue;
        let mon, yr2;
        if (mm[1] && mm[2])      { mon=mm[1]; yr2=mm[2]; }
        else if (mm[3] && mm[4]) { yr2=mm[3]; mon=mm[4]; }
        else                     { mon=mm[5]; yr2=mm[6]; }
        if (!MONTHS.includes(mon)) continue;
        const lbl = mon + yr2;
        if (!nc.SILVER[lbl] || sym.startsWith('SILVER30'))
          nc.SILVER[lbl] = { symboltoken: tok, tradingsymbol: sym };
      }
    }

    // Merge fallbacks
    for (const [lbl, val] of Object.entries(FALLBACK_TOKENS.GOLD))
      if (!nc.GOLD[lbl]) nc.GOLD[lbl] = val;
    for (const [lbl, val] of Object.entries(FALLBACK_TOKENS.SILVER))
      if (!nc.SILVER[lbl]) nc.SILVER[lbl] = val;

    tokenCache   = nc;
    cacheBuiltAt = Date.now();
    console.log('[CACHE] GOLD:' + Object.keys(nc.GOLD).length + ' SILVER:' + Object.keys(nc.SILVER).length);
    return true;
  } catch (e) {
    console.log('[CACHE] Failed — using fallback tokens:', e.message);
    tokenCache   = { GOLD: { ...FALLBACK_TOKENS.GOLD }, SILVER: { ...FALLBACK_TOKENS.SILVER } };
    cacheBuiltAt = Date.now();
    return false;
  }
}

async function ensureCache() {
  if (!cacheBuiltAt || Date.now() - cacheBuiltAt > CACHE_TTL) {
    await buildScripCache();
  }
}

async function findToken(jwt, base, label) {
  await ensureCache();
  return tokenCache[base]?.[label] || FALLBACK_TOKENS[base]?.[label] || null;
}

// ─── ANGEL QUOTE ─────────────────────────────────────────────────
async function getQuote(jwt, symboltoken) {
  try {
    const r = await axios.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
      { mode: 'FULL', exchangeTokens: { MCX: [String(symboltoken)] } },
      { headers: HDR(jwt), timeout: 10000 }
    );
    const d = r.data?.data?.fetched?.[0] || {};
    return {
      ltp:  Number(d.ltp)  || 0,
      bid:  Number(d.depth?.buy?.[0]?.price)  || Number(d.ltp) || 0,
      ask:  Number(d.depth?.sell?.[0]?.price) || Number(d.ltp) || 0,
      high: Number(d.high) || 0,
      low:  Number(d.low)  || 0,
      open: Number(d.open) || 0,
    };
  } catch (e) {
    console.log('[QUOTE] err:', e.message.slice(0, 50));
    return null;
  }
}

// ─── SPOT RATES ───────────────────────────────────────────────────
async function getSpotRates() {
  // Source 1: gold-api.com (has 24h high/low)
  try {
    const [gR, sR] = await Promise.all([
      axios.get('https://www.gold-api.com/price/XAU', { timeout: 7000 }),
      axios.get('https://www.gold-api.com/price/XAG', { timeout: 7000 }),
    ]);
    const xau = gR.data?.price, xag = sR.data?.price;
    if (xau > 2000 && xau < 10000 && xag > 20 && xag < 500) {
      console.log('[SPOT] gold-api.com: xau=' + xau + ' xag=' + xag);
      return {
        xauUsd: xau, xagUsd: xag,
        xauHigh: gR.data?.prev_close_price ? Math.max(xau, gR.data.prev_close_price) : xau * 1.005,
        xauLow:  gR.data?.prev_close_price ? Math.min(xau, gR.data.prev_close_price) : xau * 0.995,
        xagHigh: sR.data?.prev_close_price ? Math.max(xag, sR.data.prev_close_price) : xag * 1.008,
        xagLow:  sR.data?.prev_close_price ? Math.min(xag, sR.data.prev_close_price) : xag * 0.992,
        src: 'gold-api.com',
      };
    }
  } catch (e) { console.log('[SPOT] gold-api fail'); }

  // Source 2: metals.live
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout: 7000 });
    if (Array.isArray(r.data)) {
      const xau = r.data.find(x => x.gold)?.gold;
      const xag = r.data.find(x => x.silver)?.silver;
      if (xau > 2000 && xag > 20) {
        console.log('[SPOT] metals.live: xau=' + xau + ' xag=' + xag);
        return { xauUsd:xau, xagUsd:xag, xauHigh:xau*1.005, xauLow:xau*0.995,
                 xagHigh:xag*1.008, xagLow:xag*0.992, src:'metals.live' };
      }
    }
  } catch (e) { console.log('[SPOT] metals.live fail'); }

  // Source 3: goldprice.org
  try {
    const r = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://goldprice.org/' }, timeout: 8000,
    });
    const xau = r.data?.items?.[0]?.xauPrice, xag = r.data?.items?.[0]?.xagPrice;
    if (xau > 2000 && xag > 20) {
      console.log('[SPOT] goldprice.org: xau=' + xau + ' xag=' + xag);
      return { xauUsd:xau, xagUsd:xag, xauHigh:xau*1.005, xauLow:xau*0.995,
               xagHigh:xag*1.008, xagLow:xag*0.992, src:'goldprice.org' };
    }
  } catch (e) { console.log('[SPOT] goldprice.org fail'); }

  console.log('[SPOT] All failed - hardcoded fallback');
  return { xauUsd:3310, xagUsd:32.8, xauHigh:3326, xauLow:3294,
           xagHigh:33.06, xagLow:32.54, src:'hardcoded_fallback' };
}

// ─── FOREX ───────────────────────────────────────────────────────
async function getForex() {
  const MIN = 82, MAX = 88;
  try {
    const r = await axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 6000 });
    const v = r.data?.rates?.INR;
    if (v > MIN && v < MAX) { console.log('[FOREX] open.er-api: ' + v); return v; }
  } catch (e) { console.log('[FOREX] open.er-api fail'); }

  try {
    const r = await axios.get('https://api.exchangerate-api.com/v4/latest/USD', { timeout: 6000 });
    const v = r.data?.rates?.INR;
    if (v > MIN && v < MAX) { console.log('[FOREX] exchangerate-api: ' + v); return v; }
  } catch (e) { console.log('[FOREX] exchangerate-api fail'); }

  try {
    const r = await axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json', { timeout: 6000 });
    const v = r.data?.usd?.inr;
    if (v > MIN && v < MAX) { console.log('[FOREX] jsdelivr: ' + v); return v; }
  } catch (e) { console.log('[FOREX] jsdelivr fail'); }

  console.log('[FOREX] All failed - 84.50');
  return 84.50;
}

// ─── SPOT DERIVED (with real High/Low) ───────────────────────────
async function getSpotDerived() {
  try {
    const [spot, usdInr] = await Promise.all([getSpotRates(), getForex()]);
    const F = 1.103;
    const gLtp  = Math.round(spot.xauUsd  / 31.1035 * 10   * usdInr * F);
    const gHigh = Math.round(spot.xauHigh / 31.1035 * 10   * usdInr * F);
    const gLow  = Math.round(spot.xauLow  / 31.1035 * 10   * usdInr * F);
    const sLtp  = Math.round(spot.xagUsd  / 31.1035 * 1000 * usdInr * F);
    const sHigh = Math.round(spot.xagHigh / 31.1035 * 1000 * usdInr * F);
    const sLow  = Math.round(spot.xagLow  / 31.1035 * 1000 * usdInr * F);
    console.log('[DERIVED] gold=' + gLtp + ' silver=' + sLtp + ' forex=' + usdInr + ' src=' + spot.src);
    return { gLtp, gHigh, gLow, sLtp, sHigh, sLow,
             usdInr, xauUsd: spot.xauUsd, xagUsd: spot.xagUsd, src: spot.src };
  } catch (e) {
    console.log('[DERIVED] failed:', e.message);
    return null;
  }
}

// ─── LAST KNOWN RATES ────────────────────────────────────────────
let lastKnownRates = null;

// ─── /rates — MAIN ENDPOINT ──────────────────────────────────────
app.get('/rates', async (req, res) => {
  const marketOpen = isMCXOpen();

  // ── STEP 1: Try Angel MCX live ONLY if market is open
  if (marketOpen) {
    try {
      const jwt = await login();
      const gC  = getContracts(GOLD_M);
      const sC  = getContracts(SILVER_M);

      const [gTok, gNTok, sTok, sNTok] = await Promise.all([
        findToken(jwt, 'GOLD',   gC[0]),
        findToken(jwt, 'GOLD',   gC[1]),
        findToken(jwt, 'SILVER', sC[0]),
        findToken(jwt, 'SILVER', sC[1]),
      ]);

      if (!gTok?.symboltoken) throw new Error('GOLD token null');
      if (!sTok?.symboltoken) throw new Error('SILVER token null');

      const [gQ, sQ, gNQ, sNQ] = await Promise.all([
        getQuote(jwt, gTok.symboltoken),
        getQuote(jwt, sTok.symboltoken),
        gNTok ? getQuote(jwt, gNTok.symboltoken) : Promise.resolve(null),
        sNTok ? getQuote(jwt, sNTok.symboltoken) : Promise.resolve(null),
      ]);

      if (!gQ || !sQ) throw new Error('Quote fetch failed');
      if (gQ.ltp === 0 || sQ.ltp === 0) throw new Error('LTP=0');

      const gN = gNQ?.ltp > 0 ? gNQ : gQ;
      const sN = sNQ?.ltp > 0 ? sNQ : sQ;

      const payload = {
        success: true, source: 'angel_mcx_live', marketOpen: true,
        contracts: {
          gold:   { current: gC[0], next: gC[1], symbol: gTok.tradingsymbol },
          silver: { current: sC[0], next: sC[1], symbol: sTok.tradingsymbol },
        },
        goldPer10g: gQ.ltp, silverPerKg: sQ.ltp,
        futures: {
          gold:       { ltp:gQ.ltp,  bid:gQ.bid,  ask:gQ.ask,  high:gQ.high,  low:gQ.low,  open:gQ.open  },
          silver:     { ltp:sQ.ltp,  bid:sQ.bid,  ask:sQ.ask,  high:sQ.high,  low:sQ.low,  open:sQ.open  },
          goldNext:   { ltp:gN.ltp,  bid:gN.bid,  ask:gN.ask,  high:gN.high,  low:gN.low   },
          silverNext: { ltp:sN.ltp,  bid:sN.bid,  ask:sN.ask,  high:sN.high,  low:sN.low   },
        },
        timestamp: new Date().toISOString(),
      };
      lastKnownRates = payload;
      return res.json(payload);
    } catch (e) {
      console.log('[RATES] Angel live failed:', e.message);
    }
  }

  // ── STEP 2: Last known real MCX price (closing rates)
  if (lastKnownRates) {
    return res.json({
      ...lastKnownRates,
      source:    'last_known_rates',
      marketOpen,
      note:      marketOpen ? 'Angel unavailable — last known MCX price' : 'MCX closed — last closing price',
      priceAsOf: lastKnownRates.timestamp,
      timestamp: new Date().toISOString(),
    });
  }

  // ── STEP 3: Spot-derived (always works, real international data)
  const d = await getSpotDerived();
  if (d) {
    const gC = getContracts(GOLD_M), sC = getContracts(SILVER_M);
    return res.json({
      success: true, source: 'spot_derived', marketOpen,
      note:       'Live international spot rates (XAU/XAG → INR). MCX live unavailable.',
      spotSource: d.src, usdInr: d.usdInr,
      xauUsd: d.xauUsd, xagUsd: d.xagUsd,
      contracts: { gold: { current:gC[0], next:gC[1] }, silver: { current:sC[0], next:sC[1] } },
      goldPer10g:  d.gLtp,
      silverPerKg: d.sLtp,
      futures: {
        gold:       { ltp:d.gLtp, bid:d.gLtp-10, ask:d.gLtp+10, high:d.gHigh, low:d.gLow, open:d.gLtp },
        silver:     { ltp:d.sLtp, bid:d.sLtp-50, ask:d.sLtp+50, high:d.sHigh, low:d.sLow, open:d.sLtp },
        goldNext:   { ltp:d.gLtp+150, bid:d.gLtp+140, ask:d.gLtp+160, high:d.gHigh+150, low:d.gLow+150 },
        silverNext: { ltp:d.sLtp+500, bid:d.sLtp+490, ask:d.sLtp+510, high:d.sHigh+500, low:d.sLow+500 },
      },
      timestamp: new Date().toISOString(),
    });
  }

  // ── STEP 4: Total failure
  return res.status(500).json({
    success: false, source: 'error',
    error: 'All data sources failed',
    marketOpen, timestamp: new Date().toISOString(),
  });
});

// ─── OTHER ROUTES ────────────────────────────────────────────────
app.get('/', (req, res) => res.json({
  server:          'RR Jewellers Gold Server v15 - PRODUCTION',
  endpoints:       ['/rates','/debug','/cache-status','/login-test','/spot-test','/forex-test','/ping','/updates'],
  cacheBuilt:      cacheBuiltAt ? new Date(cacheBuiltAt).toISOString() : 'not yet',
  mcxOpen:         isMCXOpen(),
  goldContracts:   getContracts(GOLD_M),
  silverContracts: getContracts(SILVER_M),
  lastRateAt:      lastKnownRates?.timestamp || null,
  angelNote:       'Angel MCX live requires static IP whitelist in SmartAPI portal. Current fallback: spot_derived',
  ist: (() => { const i=getIST(); return `${i.year}-${i.month+1}-${i.day} ${i.hour}:${String(i.min).padStart(2,'0')} IST`; })(),
}));

app.get('/ping', (req, res) => res.json({ ok: true, ts: Date.now() }));

app.get('/login-test', async (req, res) => {
  try {
    const jwt = await login();
    res.json({ success: true, preview: jwt.slice(0,30)+'...' });
  } catch (e) {
    res.json({ success: false, error: e.message,
               fix: 'Whitelist Render IP in Angel SmartAPI portal → My Profile → API Settings' });
  }
});

app.get('/spot-test', async (req, res) => {
  try {
    const d = await getSpotDerived();
    res.json(d || { error: 'All spot sources failed' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/forex-test', async (req, res) => {
  const rate = await getForex();
  res.json({ usdInr: rate });
});

app.get('/cache-status', async (req, res) => {
  await ensureCache();
  res.json({
    cacheBuiltAt:    cacheBuiltAt ? new Date(cacheBuiltAt).toISOString() : null,
    goldContracts:   Object.keys(tokenCache.GOLD   ||{}).map(k => ({ label:k, ...tokenCache.GOLD[k] })),
    silverContracts: Object.keys(tokenCache.SILVER ||{}).map(k => ({ label:k, ...tokenCache.SILVER[k] })),
    activeGold:      getContracts(GOLD_M),
    activeSilver:    getContracts(SILVER_M),
  });
});

app.get('/debug', async (req, res) => {
  const spot   = await getSpotDerived();
  const gC     = getContracts(GOLD_M);
  const sC     = getContracts(SILVER_M);
  await ensureCache();
  let angelStatus = 'not_tested';
  try {
    await login();
    angelStatus = 'login_ok';
  } catch (e) {
    angelStatus = 'login_failed: ' + e.message.slice(0, 80);
  }
  res.json({
    angelStatus,
    wantedContracts:  { gold: gC, silver: sC },
    cacheGoldKeys:    Object.keys(tokenCache.GOLD   || {}),
    cacheSilverKeys:  Object.keys(tokenCache.SILVER || {}),
    spotDerived:      spot,
    mcxOpen:          isMCXOpen(),
    lastKnownRatesAt: lastKnownRates?.timestamp || null,
    fix:              'If angelStatus shows login_failed — whitelist Render IP in SmartAPI portal',
  });
});

app.get('/updates', async (req, res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID not set');
    const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r   = await axios.get(url, { timeout: 8000 });
    const json = r.data.replace(/.*?({.*}).*/s, '$1');
    const data = JSON.parse(json);
    const rows = data.table.rows.map(row => ({
      date: row.c[0]?.v||'', title: row.c[1]?.v||'', content: row.c[2]?.v||'', image: row.c[3]?.v||'',
    }));
    res.json({ success: true, updates: rows.filter(r => r.title) });
  } catch (e) {
    res.json({ success: true, updates: [{ date:'Today', title:'Welcome to R.R. Jewellers',
      content:'Live gold & silver rates. Contact us for best prices!', image:'' }] });
  }
});

// ─── SERVER START ─────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log('═══════════════════════════════════════════');
  console.log(' RR Jewellers Gold Server v15 — port ' + PORT);
  console.log('═══════════════════════════════════════════');
  await buildScripCache();
  setInterval(async () => {
    if (Date.now() - cacheBuiltAt > CACHE_TTL) await buildScripCache();
  }, 30 * 60 * 1000);
  setInterval(() => axios.get(SELF_URL + '/ping').catch(() => {}), 4 * 60 * 1000);
});
