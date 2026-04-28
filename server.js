const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const app     = express();

app.use(cors());
app.use(express.json());

// ═══════════════════════════════════
// ANGEL ONE CREDENTIALS
// ═══════════════════════════════════
const CLIENT_ID   = 'AAAA238852';
const API_KEY     = 'DPAHMIXr';
const TOTP_SECRET = 'XXNWX47RXA5KYW3BB45D4CX474';
const ANGEL_PIN   = '1857';

// ═══════════════════════════════════
// MCX CONTRACT CYCLES
// Gold:   JUN AUG OCT DEC FEB APR
// Silver: MAY JUL SEP DEC MAR
// ═══════════════════════════════════
const MONTHS   = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
const GOLD_M   = [5,7,9,11,1,3];
const SILVER_M = [4,6,8,11,2];

function getContracts(validM) {
  const now = new Date();
  let m = now.getMonth(), y = now.getFullYear();
  const out = [];
  for (let i = 0; i < 24 && out.length < 2; i++) {
    if (validM.includes(m)) out.push(MONTHS[m] + y.toString().slice(-2));
    if (++m > 11) { m = 0; y++; }
  }
  return out;
}

// ═══════════════════════════════════
// TOTP - multi window for time drift
// ═══════════════════════════════════
function generateTOTP(secret, offset) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const v = alpha.indexOf(c);
    if (v >= 0) bits += v.toString(2).padStart(5,'0');
  }
  const bytes = [];
  for (let i = 0; i+8 <= bits.length; i+=8)
    bytes.push(parseInt(bits.slice(i,i+8),2));
  const key = Buffer.from(bytes);
  const t   = Math.floor(Date.now()/1000/30) + (offset||0);
  const tb  = Buffer.alloc(8);
  tb.writeUInt32BE(Math.floor(t/0x100000000),0);
  tb.writeUInt32BE(t>>>0,4);
  const hmac = crypto.createHmac('sha1',key).update(tb).digest();
  const off  = hmac[hmac.length-1]&0xf;
  const code = ((hmac[off]&0x7f)<<24|(hmac[off+1]&0xff)<<16|
                (hmac[off+2]&0xff)<<8|hmac[off+3]&0xff)%1000000;
  return code.toString().padStart(6,'0');
}

// ═══════════════════════════════════
// HEADERS - USE REGISTERED IP
// IP registered in Angel: 74.220.52.100
// ═══════════════════════════════════
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

let JWT=null, JWT_EXP=0;

async function login() {
  if (JWT && Date.now() < JWT_EXP) return JWT;
  for (const w of [-4,-3,-2,-1,0,1,2,3,4]) {
    const pin = generateTOTP(TOTP_SECRET, w);
    console.log(`[AUTH] window=${w} totp=${pin}`);
    try {
      const r = await axios.post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        {clientcode:CLIENT_ID, password:ANGEL_PIN, totp:pin},
        {headers:HDR(), timeout:10000}
      );
      console.log('[AUTH]', JSON.stringify(r.data).slice(0,150));
      if (r.data.status && r.data.data?.jwtToken) {
        JWT=r.data.data.jwtToken;
        JWT_EXP=Date.now()+6*60*60*1000;
        console.log('[AUTH] SUCCESS window='+w);
        return JWT;
      }
    } catch(e) { console.log('[AUTH] window='+w+' err='+e.message); }
  }
  throw new Error('Angel login failed - IP not whitelisted yet');
}

async function searchMCX(jwt, q) {
  const r = await axios.post(
    'https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/searchScrip',
    {exchange:'MCX', searchscrip:q},
    {headers:HDR(jwt), timeout:8000}
  );
  return r.data.data || [];
}

async function getQuote(jwt, token) {
  const r = await axios.post(
    'https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/',
    {mode:'FULL', exchangeTokens:{MCX:[token]}},
    {headers:HDR(jwt), timeout:8000}
  );
  const d = r.data.data?.fetched?.[0] || {};
  return {
    ltp:  d.ltp  || 0,
    bid:  d.depth?.buy?.[0]?.price  || d.ltp || 0,
    ask:  d.depth?.sell?.[0]?.price || d.ltp || 0,
    high: d.high || 0,
    low:  d.low  || 0
  };
}

// ═══════════════════════════════════
// FREE FOREX - no API key needed
// ═══════════════════════════════════
async function getForex() {
  // Try multiple free forex sources
  try {
    const r = await axios.get(
      'https://api.frankfurter.app/latest?from=USD&to=INR',
      {timeout:5000}
    );
    return r.data.rates.INR;
  } catch(e) {
    // Backup forex
    try {
      const r = await axios.get(
        'https://open.er-api.com/v6/latest/USD',
        {timeout:5000}
      );
      return r.data.rates.INR;
    } catch(e2) {
      return 94.25; // last known
    }
  }
}

// ═══════════════════════════════════
// FREE SPOT GOLD/SILVER (no key)
// ═══════════════════════════════════
async function getSpotRates() {
  try {
    const r = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer':    'https://www.goldprice.org/',
        'Origin':     'https://www.goldprice.org'
      },
      timeout: 8000
    });
    return {
      xauUsd: r.data.items[0].xauPrice,
      xagUsd: r.data.items[0].xagPrice
    };
  } catch(e) {
    return { xauUsd: 4720, xagUsd: 76.4 };
  }
}

// ═══════════════════════════════════
// ROUTES
// ═══════════════════════════════════
app.get('/', (req,res) => res.json({
  status: 'RR Jewellers Gold Server v6',
  endpoints: ['/rates', '/login-test', '/debug']
}));

app.get('/login-test', async (req,res) => {
  try {
    const jwt = await login();
    res.json({success:true, preview:jwt.slice(0,20)+'...'});
  } catch(e) {
    res.json({success:false, error:e.message});
  }
});

app.get('/debug', async (req,res) => {
  try {
    const jwt = await login();
    const gC=getContracts(GOLD_M), sC=getContracts(SILVER_M);
    const [gR,sR] = await Promise.all([
      searchMCX(jwt,'GOLD'+gC[0]),
      searchMCX(jwt,'SILVER'+sC[0])
    ]);
    res.json({goldContracts:gC, silverContracts:sC, goldTop3:gR.slice(0,3), silverTop3:sR.slice(0,3)});
  } catch(e) {
    res.json({error:e.message});
  }
});

app.get('/rates', async (req,res) => {
  // Always get forex and spot (free, no limits)
  const [usdInr, spot] = await Promise.all([getForex(), getSpotRates()]);

  try {
    // Try Angel MCX
    const jwt = await login();
    const gC=getContracts(GOLD_M), sC=getContracts(SILVER_M);
    console.log('[RATES] Gold:',gC,'Silver:',sC);

    const [gCR,gNR,sCR,sNR] = await Promise.all([
      searchMCX(jwt,'GOLD'  +gC[0]), searchMCX(jwt,'GOLD'  +gC[1]),
      searchMCX(jwt,'SILVER'+sC[0]), searchMCX(jwt,'SILVER'+sC[1])
    ]);

    const gCT=gCR[0]?.symboltoken, gNT=gNR[0]?.symboltoken;
    const sCT=sCR[0]?.symboltoken, sNT=sNR[0]?.symboltoken;
    if (!gCT||!sCT) throw new Error('Tokens not found');

    const toks=[gCT,sCT,gNT,sNT].filter(Boolean);
    const qtps=await Promise.all(toks.map(t=>getQuote(jwt,t)));

    const gCurr=qtps[0], sCurr=qtps[1];
    const gNext=qtps[2]||gCurr, sNext=qtps[3]||sCurr;

    res.json({
      success:true, source:'angel_mcx',
      contracts:{gold:gC, silver:sC},
      goldPer10g:  Math.round(gCurr.ltp),
      silverPerKg: Math.round(sCurr.ltp),
      futures:{
        gold:      {bid:Math.round(gCurr.bid),ask:Math.round(gCurr.ask),high:Math.round(gCurr.high),low:Math.round(gCurr.low)},
        silver:    {bid:Math.round(sCurr.bid),ask:Math.round(sCurr.ask),high:Math.round(sCurr.high),low:Math.round(sCurr.low)},
        goldNext:  {bid:Math.round(gNext.bid),ask:Math.round(gNext.ask),high:Math.round(gNext.high),low:Math.round(gNext.low)},
        silverNext:{bid:Math.round(sNext.bid),ask:Math.round(sNext.ask),high:Math.round(sNext.high),low:Math.round(sNext.low)}
      },
      xauUsd: spot.xauUsd,
      xagUsd: spot.xagUsd,
      usdInr,
      timestamp: new Date().toISOString()
    });

  } catch(angelErr) {
    // Fallback - still return real spot + forex data
    console.log('[FALLBACK]', angelErr.message);
    const g  = Math.round((spot.xauUsd/31.1035)*10*usdInr);
    const s  = Math.round((spot.xagUsd/31.1035)*1000*usdInr);
    const gS = g*0.0009, sS=s*0.0012;
    res.json({
      success:true, source:'fallback_spot',
      goldPer10g:g, silverPerKg:s,
      futures:{
        gold:      {bid:Math.round(g-180-gS),  ask:Math.round(g-180+gS),  high:Math.round(g*1.003),     low:Math.round(g*0.994)    },
        silver:    {bid:Math.round(s-1800-sS), ask:Math.round(s-1800+sS), high:Math.round(s*1.012),     low:Math.round(s*0.984)    },
        goldNext:  {bid:Math.round(g+1320-gS), ask:Math.round(g+1320+gS), high:Math.round(g*1.005+1500),low:Math.round(g*0.996+1500)},
        silverNext:{bid:Math.round(s+1700-sS), ask:Math.round(s+1700+sS), high:Math.round(s*1.013+3500),low:Math.round(s*0.984+3500)}
      },
      xauUsd: spot.xauUsd,
      xagUsd: spot.xagUsd,
      usdInr,
      angelError: angelErr.message,
      timestamp: new Date().toISOString()
    });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, ()=>{
  console.log('RR Jewellers Server v6 port '+PORT);
  // Self-ping every 4 minutes to prevent Render sleep
  setInterval(function(){
    require('https').get('https://gold-proxy-server.onrender.com/', function(r){
      console.log('[PING] Self-ping OK - server awake');
    }).on('error', function(){});
  }, 4 * 60 * 1000);
});
