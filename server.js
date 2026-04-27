const express = require('express');
const cors = require('cors');
const axios = require('axios');
const crypto = require('crypto');
const app = express();

app.use(cors());
app.use(express.json());

const CLIENT_ID   = 'AAAA238852';
const API_KEY     = 'DPAHMIXr';
const TOTP_SECRET = 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN   = '1857';

const MONTH_NAMES = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];

// MCX Gold:   JUN, AUG, OCT, DEC, FEB, APR (every 2 months, even months + Jun)
// MCX Silver: MAY, JUL, SEP, DEC, MAR     (every 2 months)
const GOLD_CONTRACT_MONTHS   = [5, 7, 9, 11, 1, 3];  // 0-indexed: Jun=5, Aug=7, Oct=9, Dec=11, Feb=1, Apr=3
const SILVER_CONTRACT_MONTHS = [4, 6, 8, 11, 2];     // May=4, Jul=6, Sep=8, Dec=11, Mar=2

function getNextTwoContracts(validMonths) {
  const now  = new Date();
  let   m    = now.getMonth(); // 0-11
  let   y    = now.getFullYear();
  const result = [];

  for (let i = 0; i < 24 && result.length < 2; i++) {
    if (validMonths.includes(m)) {
      result.push(MONTH_NAMES[m] + y.toString().slice(-2));
    }
    m++;
    if (m > 11) { m = 0; y++; }
  }
  return result;
}

function generateTOTP(secret) {
  const base32Chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const val = base32Chars.indexOf(c);
    if (val >= 0) bits += val.toString(2).padStart(5, '0');
  }
  const bytes = [];
  for (let i = 0; i + 8 <= bits.length; i += 8)
    bytes.push(parseInt(bits.slice(i, i + 8), 2));
  const key = Buffer.from(bytes);
  const time = Math.floor(Date.now() / 1000 / 30);
  const timeBuf = Buffer.alloc(8);
  timeBuf.writeUInt32BE(Math.floor(time / 0x100000000), 0);
  timeBuf.writeUInt32BE(time >>> 0, 4);
  const hmac = crypto.createHmac('sha1', key).update(timeBuf).digest();
  const offset = hmac[hmac.length - 1] & 0xf;
  const code = ((hmac[offset] & 0x7f) << 24 | (hmac[offset+1] & 0xff) << 16 |
                (hmac[offset+2] & 0xff) << 8  | (hmac[offset+3] & 0xff)) % 1000000;
  return code.toString().padStart(6, '0');
}

let authToken  = null;
let authExpiry = 0;

async function getAuthToken() {
  if (authToken && Date.now() < authExpiry) return authToken;
  const totp = generateTOTP(TOTP_SECRET);
  console.log('TOTP:', totp);
  const res = await axios.post(
    'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
    { clientcode: CLIENT_ID, password: ANGEL_PIN, totp },
    { headers: {
        'Content-Type':         'application/json',
        'Accept':               'application/json',
        'X-UserType':           'USER',
        'X-SourceID':           'WEB',
        'X-ClientLocalIP':      '127.0.0.1',
        'X-ClientPublicIP':     '35.160.120.126',
        'X-MACAddress':         '00:00:00:00:00:00',
        'X-PrivateKey':         API_KEY
    }}
  );
  console.log('Login:', JSON.stringify(res.data).slice(0, 300));
  if (res.data.status && res.data.data && res.data.data.jwtToken) {
    authToken  = res.data.data.jwtToken;
    authExpiry = Date.now() + (6 * 60 * 60 * 1000);
    console.log('LOGIN SUCCESS');
    return authToken;
  }
  throw new Error('Login failed: ' + JSON.stringify(res.data));
}

async function searchMCX(token, query) {
  const res = await axios.post(
    'https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/searchScrip',
    { exchange: 'MCX', searchscrip: query },
    { headers: {
        'Authorization':    'Bearer ' + token,
        'Content-Type':     'application/json',
        'Accept':           'application/json',
        'X-UserType':       'USER',
        'X-SourceID':       'WEB',
        'X-ClientLocalIP':  '127.0.0.1',
        'X-ClientPublicIP': '35.160.120.126',
        'X-MACAddress':     '00:00:00:00:00:00',
        'X-PrivateKey':     API_KEY
    }}
  );
  return res.data.data || [];
}

async function getLTP(token, symboltoken) {
  const res = await axios.post(
    'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
    { mode: 'FULL', exchangeTokens: { 'MCX': [symboltoken] } },
    { headers: {
        'Authorization':    'Bearer ' + token,
        'Content-Type':     'application/json',
        'Accept':           'application/json',
        'X-UserType':       'USER',
        'X-SourceID':       'WEB',
        'X-ClientLocalIP':  '127.0.0.1',
        'X-ClientPublicIP': '35.160.120.126',
        'X-MACAddress':     '00:00:00:00:00:00',
        'X-PrivateKey':     API_KEY
    }}
  );
  const d = res.data.data?.fetched?.[0] || {};
  return {
    ltp:  d.ltp  || 0,
    bid:  d.depth?.buy?.[0]?.price  || d.ltp || 0,
    ask:  d.depth?.sell?.[0]?.price || d.ltp || 0,
    high: d.high || 0,
    low:  d.low  || 0
  };
}

app.get('/', (req, res) => {
  res.json({ status: 'Gold Proxy Server v5 - Correct MCX Cycles' });
});

app.get('/debug', async (req, res) => {
  try {
    const token          = await getAuthToken();
    const goldContracts  = getNextTwoContracts(GOLD_CONTRACT_MONTHS);
    const silverContracts= getNextTwoContracts(SILVER_CONTRACT_MONTHS);
    const gR = await searchMCX(token, 'GOLD'   + goldContracts[0]);
    const sR = await searchMCX(token, 'SILVER' + silverContracts[0]);
    res.json({ goldContracts, silverContracts, goldTop3: gR.slice(0,3), silverTop3: sR.slice(0,3) });
  } catch(e) {
    res.json({ error: e.message });
  }
});

app.get('/rates', async (req, res) => {
  try {
    const token = await getAuthToken();

    const gContracts = getNextTwoContracts(GOLD_CONTRACT_MONTHS);
    const sContracts = getNextTwoContracts(SILVER_CONTRACT_MONTHS);
    const [gC, gN, sC, sN] = [gContracts[0], gContracts[1], sContracts[0], sContracts[1]];

    console.log('Gold:', gC, '->', gN, '| Silver:', sC, '->', sN);

    const [gCR, gNR, sCR, sNR] = await Promise.all([
      searchMCX(token, 'GOLD'   + gC),
      searchMCX(token, 'GOLD'   + gN),
      searchMCX(token, 'SILVER' + sC),
      searchMCX(token, 'SILVER' + sN)
    ]);

    const gCT = gCR[0]?.symboltoken;
    const gNT = gNR[0]?.symboltoken;
    const sCT = sCR[0]?.symboltoken;
    const sNT = sNR[0]?.symboltoken;

    if (!gCT || !sCT) throw new Error('Tokens missing: gold=' + gCT + ' silver=' + sCT);

    const tokens   = [gCT, sCT, gNT, sNT].filter(Boolean);
    const ltpArr   = await Promise.all(tokens.map(t => getLTP(token, t)));
    const goldCurr   = ltpArr[0];
    const silverCurr = ltpArr[1];
    const goldNext   = ltpArr[2] || goldCurr;
    const silverNext = ltpArr[3] || silverCurr;

    const fxRes  = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 5000 });
    const usdInr = fxRes.data.rates.INR;
    const xauUsd = parseFloat(((goldCurr.ltp   / 10   / usdInr) * 31.1035).toFixed(2));
    const xagUsd = parseFloat(((silverCurr.ltp / 1000 / usdInr) * 31.1035).toFixed(2));

    res.json({
      success: true,
      source: 'angel_mcx',
      contracts: { gC, gN, sC, sN },
      goldPer10g:  Math.round(goldCurr.ltp),
      silverPerKg: Math.round(silverCurr.ltp),
      futures: {
        gold:       { bid: Math.round(goldCurr.bid),   ask: Math.round(goldCurr.ask),   high: Math.round(goldCurr.high),   low: Math.round(goldCurr.low)   },
        silver:     { bid: Math.round(silverCurr.bid), ask: Math.round(silverCurr.ask), high: Math.round(silverCurr.high), low: Math.round(silverCurr.low) },
        goldNext:   { bid: Math.round(goldNext.bid),   ask: Math.round(goldNext.ask),   high: Math.round(goldNext.high),   low: Math.round(goldNext.low)   },
        silverNext: { bid: Math.round(silverNext.bid), ask: Math.round(silverNext.ask), high: Math.round(silverNext.high), low: Math.round(silverNext.low) }
      },
      xauUsd, xagUsd, usdInr,
      timestamp: new Date().toISOString()
    });

  } catch (angelErr) {
    console.log('Angel error:', angelErr.message);
    try {
      const goldRes = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Referer':    'https://www.goldprice.org/',
          'Origin':     'https://www.goldprice.org'
        }, timeout: 8000
      });
      const fxRes  = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 8000 });
      const xauUsd = goldRes.data.items[0].xauPrice;
      const xagUsd = goldRes.data.items[0].xagPrice;
      const usdInr = fxRes.data.rates.INR;
      const g10    = Math.round((xauUsd/31.1035)*10*usdInr);
      const skg    = Math.round((xagUsd/31.1035)*1000*usdInr);
      const gSp    = g10  * 0.0009;
      const sSp    = skg  * 0.0012;
      res.json({
        success: true,
        source: 'fallback_goldprice',
        goldPer10g: g10, silverPerKg: skg,
        futures: {
          gold:       { bid: Math.round(g10-180-gSp),   ask: Math.round(g10-180+gSp),   high: Math.round(g10*1.003),       low: Math.round(g10*0.994)       },
          silver:     { bid: Math.round(skg-1800-sSp),  ask: Math.round(skg-1800+sSp),  high: Math.round(skg*1.012),       low: Math.round(skg*0.984)       },
          goldNext:   { bid: Math.round(g10+1320-gSp),  ask: Math.round(g10+1320+gSp),  high: Math.round(g10*1.005+1500),  low: Math.round(g10*0.996+1500)  },
          silverNext: { bid: Math.round(skg+1700-sSp),  ask: Math.round(skg+1700+sSp),  high: Math.round(skg*1.013+3500),  low: Math.round(skg*0.984+3500)  }
        },
        xauUsd, xagUsd, usdInr,
        angelError: angelErr.message,
        timestamp: new Date().toISOString()
      });
    } catch(e) {
      res.json({ success: false, error: e.message, angelError: angelErr.message });
    }
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Server v5 running - Smart MCX Contracts'));
