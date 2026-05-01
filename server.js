/**
 * RR Jewellers Gold Server v9
 * ─────────────────────────────────────────────
 * Strategy for guaranteed token resolution:
 *   1. On startup (and daily at 8 AM), downloads Angel's official
 *      OpenAPIScripMaster.json and caches MCX GOLD/SILVER tokens.
 *   2. /rates uses cached tokens directly — no searchScrip guessing.
 *   3. If cache miss, falls back to searchScrip broad search.
 * ─────────────────────────────────────────────
 */

'use strict';

const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const app     = express();

app.use(cors());
app.use(express.json());

// ═══════════════════════════════════════
// ANGEL ONE CREDENTIALS
// ═══════════════════════════════════════
const CLIENT_ID   = 'AAAA238852';
const API_KEY     = 'DPAHMIXr';
const TOTP_SECRET = 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN   = '1857';

// ═══════════════════════════════════════
// MCX CONTRACT CYCLES (0-indexed JS months)
// Gold:   JUN AUG OCT DEC FEB APR → 5,7,9,11,1,3
// Silver: MAY JUL SEP DEC MAR     → 4,6,8,11,2
// ═══════════════════════════════════════
const MONTHS   = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
const GOLD_M   = [5,7,9,11,1,3];
const SILVER_M = [4,6,8,11,2];

// Returns next 2 active contract labels e.g. ["JUN26","AUG26"]
function getContracts(validM) {
  const now         = new Date();
  const daysInMonth = new Date(now.getFullYear(), now.getMonth()+1, 0).getDate();
  const nearExpiry  = (daysInMonth - now.getDate()) <= 4;
  let m = now.getMonth(), y = now.getFullYear();
  if (nearExpiry && ++m > 11) { m=0; y++; }
  const out = [];
  for (let i=0; i<24 && out.length<2; i++) {
    if (validM.includes(m)) out.push(MONTHS[m] + y.toString().slice(-2));
    if (++m > 11) { m=0; y++; }
  }
  return out;
}

// ═══════════════════════════════════════
// SCRIP MASTER CACHE
// ═══════════════════════════════════════
// tokenCache[base][contractLabel] = { symboltoken, tradingsymbol }
// e.g. tokenCache['GOLD']['JUN26'] = { symboltoken:'222972', tradingsymbol:'GOLD26JUNFUT' }
let tokenCache = {};
let cacheBuiltAt = 0;
const CACHE_TTL  = 22 * 60 * 60 * 1000; // rebuild every 22 hours

async function buildScripCache() {
  try {
    console.log('[CACHE] Downloading Angel scrip master...');
    const r = await axios.get(
      'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json',
      { timeout: 30000 }
    );

    const instruments = r.data; // array of objects
    if (!Array.isArray(instruments) || instruments.length === 0) throw new Error('Empty scrip master');

    // Filter only MCX futures (expiry = FUTCOM on MCX)
    const mcx = instruments.filter(i => i.exch_seg === 'MCX' && i.instrumenttype === 'FUTCOM');

    const newCache = {};

    for (const inst of mcx) {
      const sym = (inst.symbol || inst.name || '').toUpperCase(); // e.g. "GOLD26JUNFUT"
      const tok = inst.token;
      if (!sym || !tok) continue;

      // Detect base: GOLD, SILVER, GOLDM, SILVERM, etc.
      let base = null;
      for (const b of ['SILVER','GOLD']) { // SILVER first to avoid SILVER matching GOLD
        if (sym.startsWith(b)) { base = b; break; }
      }
      if (!base) continue;

      // Parse month+year from symbol:
      // Formats seen: GOLD26JUNFUT, GOLDJUN26FUT, SILVER26MAYFUT
      // We extract any 3-letter month and 2-digit year from the symbol
      const monthMatch = sym.match(/([A-Z]{3})(\d{2})(?=FUT)|(\d{2})([A-Z]{3})(?=FUT)/);
      if (!monthMatch) continue;

      let mon, yr2;
      if (monthMatch[1] && monthMatch[2]) {
        // format GOLDJUN26FUT
        mon = monthMatch[1]; yr2 = monthMatch[2];
      } else if (monthMatch[3] && monthMatch[4]) {
        // format GOLD26JUNFUT
        yr2 = monthMatch[3]; mon = monthMatch[4];
      }
      if (!mon || !yr2 || !MONTHS.includes(mon)) continue;

      const label = mon + yr2; // "JUN26"
      if (!newCache[base]) newCache[base] = {};
      // Keep first found (usually sorted by expiry)
      if (!newCache[base][label]) {
        newCache[base][label] = { symboltoken: tok, tradingsymbol: sym };
      }
    }

    tokenCache   = newCache;
    cacheBuiltAt = Date.now();

    const gCount = Object.keys(newCache['GOLD']  || {}).length;
    const sCount = Object.keys(newCache['SILVER'] || {}).length;
    console.log('[CACHE] Built OK — GOLD contracts:'+gCount+' SILVER contracts:'+sCount);
    return true;

  } catch(e) {
    console.log('[CACHE] Build failed:', e.message);
    return false;
  }
}

async function ensureCache() {
  if (Date.now() - cacheBuiltAt > CACHE_TTL) {
    await buildScripCache();
  }
}

// Look up token from cache, then fallback to searchScrip
async function findToken(jwt, base, contractLabel) {
  await ensureCache();

  // 1. Cache lookup (most reliable)
  const cached = tokenCache[base]?.[contractLabel];
  if (cached) {
    console.log('[TOKEN] cache hit: '+base+contractLabel+' → '+cached.tradingsymbol+' ('+cached.symboltoken+')');
    return cached;
  }

  // 2. Fallback: searchScrip broad search
  console.log('[TOKEN] cache miss for '+base+contractLabel+', trying searchScrip...');
  const mon = contractLabel.slice(0,3); // "JUN"
  const yr2 = contractLabel.slice(3,5); // "26"

  const queries = [
    base + yr2  + mon + 'FUT',  // GOLD26JUNFUT
    base + mon  + yr2 + 'FUT',  // GOLDJUN26FUT
    base + mon  + yr2,          // GOLDJUN26
  ];

  for (const q of queries) {
    const results = await searchScrip(jwt, q);
    if (results.length > 0) {
      console.log('[TOKEN] searchScrip hit: "'+q+'" → '+results[0].tradingsymbol+' ('+results[0].symboltoken+')');
      return { symboltoken: results[0].symboltoken, tradingsymbol: results[0].tradingsymbol };
    }
  }

  // 3. Last resort: broad base search, match by label
  const results = await searchScrip(jwt, base);
  const needle  = (mon+yr2).toUpperCase();
  const match   = results.find(r =>
    (r.tradingsymbol||'').toUpperCase().includes(needle) ||
    (r.name||'').toUpperCase().includes(needle)
  );
  if (match) {
    console.log('[TOKEN] broad match: '+base+contractLabel+' → '+match.tradingsymbol);
    return { symboltoken: match.symboltoken, tradingsymbol: match.tradingsymbol };
  }

  console.log('[TOKEN] FAILED to find token for '+base+contractLabel);
  return null;
}

// ═══════════════════════════════════════
// TOTP
// ═══════════════════════════════════════
function generateTOTP(secret, offset) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const v = alpha.indexOf(c);
    if (v >= 0) bits += v.toString(2).padStart(5,'0');
  }
  const bytes = [];
  for (let i=0; i+8 <= bits.length; i+=8)
    bytes.push(parseInt(bits.slice(i,i+8),2));
  const key = Buffer.from(bytes);
  const t   = Math.floor(Date.now()/1000/30) + (offset||0);
  const tb  = Buffer.alloc(8);
  tb.writeUInt32BE(Math.floor(t/0x100000000),0);
  tb.writeUInt32BE(t>>>0,4);
  const hmac = crypto.createHmac('sha1',key).update(tb).digest();
  const off  = hmac[hmac.length-1]&0xf;
  const code = ((hmac[off]&0x7f)<<24|(hmac[off+1]&0xff)<<16|
                (hmac[off+2]&0xff)<<8|(hmac[off+3]&0xff))%1000000;
  return code.toString().padStart(6,'0');
}

// ═══════════════════════════════════════
// ANGEL AUTH — JWT cached for 6 hours
// ═══════════════════════════════════════
let JWT=null, JWT_EXP=0;

const HDR = (jwt) => ({
  'Content-Type':     'application/json',
  'Accept':           'application/json',
  'X-UserType':       'USER',
  'X-SourceID':       'WEB',
  'X-ClientLocalIP':  '127.0.0.1',
  'X-ClientPublicIP': '74.220.52.100',
  'X-MACAddress':     'fe:80:00:00:00:00',
  'X-PrivateKey':     API_KEY,
  ...(jwt ? {'Authorization':'Bearer '+jwt} : {})
});

async function login() {
  if (JWT && Date.now() < JWT_EXP) return JWT;
  // Try TOTP window offsets ±4 steps to handle clock drift
  for (const w of [-4,-3,-2,-1,0,1,2,3,4]) {
    const pin = generateTOTP(TOTP_SECRET, w);
    try {
      const r = await axios.post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        {clientcode:CLIENT_ID, password:ANGEL_PIN, totp:pin},
        {headers:HDR(), timeout:10000}
      );
      if (r.data.status && r.data.data?.jwtToken) {
        JWT     = r.data.data.jwtToken;
        JWT_EXP = Date.now() + 6*60*60*1000;
        console.log('[AUTH] OK window='+w);
        return JWT;
      }
    } catch(e) { /* try next window */ }
  }
  throw new Error('Angel login failed — check credentials / IP whitelist');
}

// ═══════════════════════════════════════
// ANGEL API HELPERS
// ═══════════════════════════════════════
async function searchScrip(jwt, q) {
  try {
    const r = await axios.post(
      'https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/searchScrip',
      {exchange:'MCX', searchscrip:q},
      {headers:HDR(jwt), timeout:8000}
    );
    return r.data.data || [];
  } catch(e) {
    console.log('[SEARCH] error for "'+q+'": '+e.message.slice(0,50));
    return [];
  }
}

async function getQuote(jwt, symboltoken) {
  const r = await axios.post(
    'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
    {mode:'FULL', exchangeTokens:{MCX:[String(symboltoken)]}},
    {headers:HDR(jwt), timeout:8000}
  );
  const d = r.data.data?.fetched?.[0] || {};
  return {
    ltp:  Number(d.ltp)  || 0,
    bid:  Number(d.depth?.buy?.[0]?.price)  || Number(d.ltp) || 0,
    ask:  Number(d.depth?.sell?.[0]?.price) || Number(d.ltp) || 0,
    high: Number(d.high) || 0,
    low:  Number(d.low)  || 0,
    open: Number(d.open) || 0,
  };
}

// ═══════════════════════════════════════
// SPOT RATES (Coinbase REMOVED — wrong crypto-linked XAU)
// ═══════════════════════════════════════
async function getSpotRates() {
  // Source 1: metals.live
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', {timeout:6000});
    if (Array.isArray(r.data)) {
      const gold   = r.data.find(x=>x.gold)?.gold;
      const silver = r.data.find(x=>x.silver)?.silver;
      if (gold>2000 && gold<8000 && silver>20 && silver<200) {
        console.log('[SPOT] metals.live: xau='+gold+' xag='+silver);
        return {xauUsd:gold, xagUsd:silver, src:'metals.live'};
      }
    }
  } catch(e) { console.log('[SPOT] metals.live fail'); }

  // Source 2: goldprice.org
  try {
    const r = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
      headers:{'User-Agent':'Mozilla/5.0','Referer':'https://goldprice.org/','Accept':'application/json'},
      timeout:8000
    });
    const gold   = r.data?.items?.[0]?.xauPrice;
    const silver = r.data?.items?.[0]?.xagPrice;
    if (gold>2000 && gold<8000 && silver>20 && silver<200) {
      console.log('[SPOT] goldprice.org: xau='+gold+' xag='+silver);
      return {xauUsd:gold, xagUsd:silver, src:'goldprice.org'};
    }
  } catch(e) { console.log('[SPOT] goldprice.org fail'); }

  // Source 3: fxratesapi (USD base → XAU/XAG = fraction of oz per $1 → invert)
  try {
    const r = await axios.get(
      'https://api.fxratesapi.com/latest?base=USD&currencies=XAU,XAG&format=json',
      {timeout:6000}
    );
    const xauUsd = r.data?.rates?.XAU ? parseFloat((1/r.data.rates.XAU).toFixed(2)) : 0;
    const xagUsd = r.data?.rates?.XAG ? parseFloat((1/r.data.rates.XAG).toFixed(4)) : 0;
    if (xauUsd>2000 && xauUsd<8000 && xagUsd>20 && xagUsd<200) {
      console.log('[SPOT] fxratesapi: xau='+xauUsd+' xag='+xagUsd);
      return {xauUsd, xagUsd, src:'fxratesapi'};
    }
  } catch(e) { console.log('[SPOT] fxratesapi fail'); }

  console.log('[SPOT] All failed — fixed fallback');
  return {xauUsd:3340, xagUsd:33.0, src:'fixed_fallback'};
}

// ═══════════════════════════════════════
// FOREX USD/INR
// ═══════════════════════════════════════
async function getForex() {
  try {
    const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', {timeout:5000});
    const rate = r.data?.rates?.INR;
    if (rate>70 && rate<110) { console.log('[FOREX] frankfurter: '+rate); return rate; }
  } catch(e) {}

  try {
    const r = await axios.get('https://open.er-api.com/v6/latest/USD', {timeout:5000});
    const rate = r.data?.rates?.INR;
    if (rate>70 && rate<110) { console.log('[FOREX] open.er-api: '+rate); return rate; }
  } catch(e) {}

  console.log('[FOREX] all failed — 84.5');
  return 84.5;
}

// ═══════════════════════════════════════
// ROUTES
// ═══════════════════════════════════════
app.get('/', (req,res) => res.json({
  status: 'RR Jewellers Gold Server v9',
  endpoints: ['/rates', '/debug', '/cache-status', '/login-test', '/spot-test', '/updates'],
  cacheBuilt: cacheBuiltAt ? new Date(cacheBuiltAt).toISOString() : 'not yet'
}));

app.get('/login-test', async (req,res) => {
  try {
    const jwt = await login();
    res.json({success:true, preview:jwt.slice(0,20)+'...'});
  } catch(e) { res.json({success:false, error:e.message}); }
});

app.get('/spot-test', async (req,res) => {
  const [spot, usdInr] = await Promise.all([getSpotRates(), getForex()]);
  const g = Math.round((spot.xauUsd/31.1035)*10*usdInr*1.0920);
  const s = Math.round((spot.xagUsd/31.1035)*1000*usdInr*1.0661);
  res.json({spot, usdInr, goldMCX_approx:g, silverMCX_approx:s});
});

// Shows exactly what is in the scrip master cache
app.get('/cache-status', async (req,res) => {
  await ensureCache();
  const gKeys = Object.keys(tokenCache['GOLD']  || {}).sort();
  const sKeys = Object.keys(tokenCache['SILVER'] || {}).sort();
  res.json({
    cacheBuiltAt: cacheBuiltAt ? new Date(cacheBuiltAt).toISOString() : null,
    goldContracts:   gKeys.map(k=>({label:k, ...tokenCache['GOLD'][k]})),
    silverContracts: sKeys.map(k=>({label:k, ...tokenCache['SILVER'][k]})),
  });
});

// Full debug — wanted contracts + resolved tokens + raw search
app.get('/debug', async (req,res) => {
  try {
    const jwt = await login();
    const gC  = getContracts(GOLD_M);
    const sC  = getContracts(SILVER_M);

    const [gTok, sTok] = await Promise.all([
      findToken(jwt, 'GOLD',   gC[0]),
      findToken(jwt, 'SILVER', sC[0]),
    ]);

    // Also run a raw broad search so you can see all available symbols
    const [gRaw, sRaw] = await Promise.all([
      searchScrip(jwt, 'GOLD'),
      searchScrip(jwt, 'SILVER'),
    ]);

    res.json({
      wantedContracts:  {gold:gC, silver:sC},
      goldTokenFound:   gTok,
      silverTokenFound: sTok,
      goldRawResults:   gRaw.map(r=>({sym:r.tradingsymbol, tok:r.symboltoken})),
      silverRawResults: sRaw.map(r=>({sym:r.tradingsymbol, tok:r.symboltoken})),
      cacheGoldKeys:    Object.keys(tokenCache['GOLD']  || {}),
      cacheSilverKeys:  Object.keys(tokenCache['SILVER']|| {}),
    });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Updates from Google Sheets
const SHEET_ID = process.env.SHEET_ID || '';
app.get('/updates', async (req,res) => {
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID env not set');
    const url  = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r    = await axios.get(url, {timeout:8000});
    const json = r.data.replace(/.*?({.*}).*/s,'$1');
    const data = JSON.parse(json);
    const rows = data.table.rows.map(row => ({
      date:    row.c[0]?.v || '',
      title:   row.c[1]?.v || '',
      content: row.c[2]?.v || '',
      image:   row.c[3]?.v || '',
    }));
    res.json({success:true, updates:rows.filter(r=>r.title)});
  } catch(e) {
    res.json({success:true, updates:[
      {date:'Today', title:'Welcome to R.R. Jewellers',
       content:'Live gold & silver rates. Contact us for best prices!', image:''}
    ]});
  }
});

// ═══════════════════════════════════════
// MAIN /rates — 100% Angel MCX live data
// ═══════════════════════════════════════
app.get('/rates', async (req,res) => {
  try {
    const jwt = await login();
    const gC  = getContracts(GOLD_M);
    const sC  = getContracts(SILVER_M);

    // Resolve tokens for current + next contract of each metal
    const [gCurTok, gNxtTok, sCurTok, sNxtTok] = await Promise.all([
      findToken(jwt, 'GOLD',   gC[0]),
      findToken(jwt, 'GOLD',   gC[1]),
      findToken(jwt, 'SILVER', sC[0]),
      findToken(jwt, 'SILVER', sC[1]),
    ]);

    if (!gCurTok?.symboltoken) throw new Error('GOLD '+gC[0]+' token not found — check /debug');
    if (!sCurTok?.symboltoken) throw new Error('SILVER '+sC[0]+' token not found — check /debug');

    console.log('[RATES] GOLD='+gCurTok.symboltoken+'('+gC[0]+') SILVER='+sCurTok.symboltoken+'('+sC[0]+')');

    // Fetch live quotes
    const [gCurr, sCurr, gNextRaw, sNextRaw] = await Promise.all([
      getQuote(jwt, gCurTok.symboltoken),
      getQuote(jwt, sCurTok.symboltoken),
      gNxtTok?.symboltoken ? getQuote(jwt, gNxtTok.symboltoken) : Promise.resolve(null),
      sNxtTok?.symboltoken ? getQuote(jwt, sNxtTok.symboltoken) : Promise.resolve(null),
    ]);

    if (gCurr.ltp === 0) throw new Error('GOLD quote returned LTP=0 — market may be closed');
    if (sCurr.ltp === 0) throw new Error('SILVER quote returned LTP=0 — market may be closed');

    const gNext = gNextRaw || gCurr;
    const sNext = sNextRaw || sCurr;

    res.json({
      success:     true,
      source:      'angel_mcx_live',
      contracts:   {
        gold:   {current:gC[0], next:gC[1], currentSymbol:gCurTok.tradingsymbol},
        silver: {current:sC[0], next:sC[1], currentSymbol:sCurTok.tradingsymbol},
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
          high: Math.round(gNext.high || gNext.ltp*1.003),
          low:  Math.round(gNext.low  || gNext.ltp*0.994),
        },
        silverNext: {
          ltp:  Math.round(sNext.ltp),
          bid:  Math.round(sNext.bid),
          ask:  Math.round(sNext.ask),
          high: Math.round(sNext.high || sNext.ltp*1.012),
          low:  Math.round(sNext.low  || sNext.ltp*0.984),
        },
      },
      timestamp: new Date().toISOString(),
    });

  } catch(err) {
    console.log('[RATES ERROR]', err.message);
    res.status(500).json({
      success:   false,
      source:    'error',
      error:     err.message,
      debug_url: '/debug',
      cache_url: '/cache-status',
      timestamp: new Date().toISOString(),
    });
  }
});

// ═══════════════════════════════════════
// SERVER START
// ═══════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log('RR Jewellers Gold Server v9 — port '+PORT);

  // Build scrip cache immediately on startup
  await buildScripCache();

  // Rebuild cache daily at 8 AM IST (2:30 UTC)
  // Also rebuild every 22 hours as safety net
  setInterval(async () => {
    const now = new Date();
    const utcH = now.getUTCHours();
    const utcM = now.getUTCMinutes();
    // Rebuild at ~2:30 UTC (8 AM IST) or if cache is stale
    if ((utcH === 2 && utcM >= 30) || (Date.now() - cacheBuiltAt > CACHE_TTL)) {
      await buildScripCache();
    }
  }, 30 * 60 * 1000); // check every 30 min

  // Keep Render awake
  const SELF_URL = process.env.SELF_URL || 'https://gold-proxy-server.onrender.com/';
  setInterval(() => {
    require('https').get(SELF_URL, ()=>{ console.log('[PING] awake'); }).on('error',()=>{});
  }, 4*60*1000);
});
