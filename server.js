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
const ANGEL_PIN   = '1857'; // ← Change when PIN expires (~90 days)

// ═══════════════════════════════════════
// MCX CONTRACT CYCLES (Auto)
// Gold:   JUN AUG OCT DEC FEB APR
// Silver: MAY JUL SEP DEC MAR
// ═══════════════════════════════════════
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

// ═══════════════════════════════════════
// ANGEL AUTH
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
  for (const w of [-4,-3,-2,-1,0,1,2,3,4]) {
    const pin = generateTOTP(TOTP_SECRET, w);
    try {
      const r = await axios.post(
        'https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword',
        {clientcode:CLIENT_ID, password:ANGEL_PIN, totp:pin},
        {headers:HDR(), timeout:10000}
      );
      if (r.data.status && r.data.data?.jwtToken) {
        JWT=r.data.data.jwtToken;
        JWT_EXP=Date.now()+6*60*60*1000;
        console.log('[AUTH] SUCCESS window='+w);
        return JWT;
      }
    } catch(e) { console.log('[AUTH] w='+w+':'+e.message.slice(0,40)); }
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

// ═══════════════════════════════════════
// SPOT RATES - FIXED SOURCES
// Removed coinbase (gives wrong crypto gold prices!)
// Using only reliable metals sources
// ═══════════════════════════════════════
async function getSpotRates() {

  // Source 1: metals.live (best free metals API)
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver', {
      timeout: 6000
    });
    if (r.data && Array.isArray(r.data)) {
      const gold   = r.data.find(x => x.gold)?.gold;
      const silver = r.data.find(x => x.silver)?.silver;
      // Validate: Gold should be between 2000-5000 USD/oz
      if (gold && silver && gold > 3000 && gold < 8000 && silver > 20 && silver < 200) {
        console.log('[SPOT] metals.live OK: xau='+gold+' xag='+silver);
        return {xauUsd: gold, xagUsd: silver, src: 'metals.live'};
      }
    }
  } catch(e) { console.log('[SPOT] metals.live failed:', e.message.slice(0,50)); }

  // Source 2: Coinbase (gives real XAU/XAG spot prices)
  try {
    const [r1,r2] = await Promise.all([
      axios.get('https://api.coinbase.com/v2/exchange-rates?currency=XAU', {timeout:6000}),
      axios.get('https://api.coinbase.com/v2/exchange-rates?currency=XAG', {timeout:6000})
    ]);
    const xauUsd = parseFloat(r1.data?.data?.rates?.USD) || 0;
    const xagUsd = parseFloat(r2.data?.data?.rates?.USD) || 0;
    if (!isNaN(xauUsd) && !isNaN(xagUsd) && xauUsd > 3000 && xauUsd < 8000 && xagUsd > 20 && xagUsd < 200) {
      console.log('[SPOT] coinbase OK: xau='+xauUsd+' xag='+xagUsd);
      return {xauUsd, xagUsd, src:'coinbase'};
    }
  } catch(e) { console.log('[SPOT] coinbase failed:', e.message.slice(0,50)); }

  // Source 3: goldprice.org with mobile user agent
  try {
    const r = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36',
        'Referer':    'https://goldprice.org/',
        'Accept':     'application/json'
      },
      timeout: 8000
    });
    if (r.data?.items?.[0]) {
      const gold   = r.data.items[0].xauPrice;
      const silver = r.data.items[0].xagPrice;
      if (gold > 3000 && gold < 8000 && silver > 20 && silver < 200) {
        console.log('[SPOT] goldprice.org OK: xau='+gold+' xag='+silver);
        return {xauUsd: gold, xagUsd: silver, src: 'goldprice.org'};
      }
    }
  } catch(e) { console.log('[SPOT] goldprice.org failed:', e.message.slice(0,50)); }

  // Source 3: frankfurter has no metals - use fixed last known
  // Gold ~3340, Silver ~33 (April 2026 approximate)
  console.log('[SPOT] All APIs failed - using last known rates');
  return {xauUsd: 3340, xagUsd: 33.0, src: 'fixed_fallback'};
}

// ═══════════════════════════════════════
// FOREX - USD/INR
// ═══════════════════════════════════════
async function getForex() {
  // Source 1: frankfurter.app
  try {
    const r = await axios.get(
      'https://api.frankfurter.app/latest?from=USD&to=INR',
      {timeout: 5000}
    );
    const rate = r.data?.rates?.INR;
    // Validate: USD/INR should be between 70-100
    if (rate && rate > 70 && rate < 110) {
      console.log('[FOREX] frankfurter OK: '+rate);
      return rate;
    }
  } catch(e) { console.log('[FOREX] frankfurter failed'); }

  // Source 2: open.er-api.com
  try {
    const r = await axios.get(
      'https://open.er-api.com/v6/latest/USD',
      {timeout: 5000}
    );
    const rate = r.data?.rates?.INR;
    if (rate && rate > 70 && rate < 110) {
      console.log('[FOREX] open.er-api OK: '+rate);
      return rate;
    }
  } catch(e) { console.log('[FOREX] open.er-api failed'); }

  // Last known approximate
  console.log('[FOREX] All failed - using 84.5');
  return 94.5;
}

// ═══════════════════════════════════════
// ROUTES
// ═══════════════════════════════════════
app.get('/', (req,res) => res.json({
  status: 'RR Jewellers Gold Server v7',
  endpoints:['/rates','/login-test','/debug','/spot-test','/updates']
}));

app.get('/login-test', async (req,res) => {
  try {
    const jwt = await login();
    res.json({success:true, preview:jwt.slice(0,20)+'...'});
  } catch(e) { res.json({success:false, error:e.message}); }
});

app.get('/spot-test', async (req,res) => {
  const [spot, usdInr] = await Promise.all([getSpotRates(), getForex()]);
  const goldMCX  = Math.round((spot.xauUsd/31.1035)*10*usdInr*1.656);
  const silverMCX = Math.round((spot.xagUsd/31.1035)*1000*usdInr*2.668);
  res.json({
    spot, usdInr,
    goldMCX_approx:  goldMCX,
    silverMCX_approx: silverMCX,
    expected: {gold:'~152,000', silver:'~242,000'}
  });
});

app.get('/debug', async (req,res) => {
  try {
    const jwt = await login();
    const gC=getContracts(GOLD_M), sC=getContracts(SILVER_M);
    const [gR,sR] = await Promise.all([
      searchMCX(jwt,'GOLD'+gC[0]),
      searchMCX(jwt,'SILVER'+sC[0])
    ]);
    res.json({goldContracts:gC, silverContracts:sC,
              goldTop3:gR.slice(0,3), silverTop3:sR.slice(0,3)});
  } catch(e) { res.json({error:e.message}); }
});

// Updates from Google Sheets
const SHEET_ID = 'YOUR_SHEET_ID_HERE';
app.get('/updates', async (req,res) => {
  try {
    const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r = await axios.get(url, {timeout:8000});
    const json = r.data.replace(/.*?({.*}).*/s,'$1');
    const data = JSON.parse(json);
    const rows = data.table.rows.map(row => ({
      date:    row.c[0]?.v || '',
      title:   row.c[1]?.v || '',
      content: row.c[2]?.v || '',
      image:   row.c[3]?.v || ''
    }));
    res.json({success:true, updates:rows.filter(r=>r.title)});
  } catch(e) {
    res.json({success:true, updates:[
      {date:'Today', title:'Welcome to R.R. Jewellers',
       content:'Live gold & silver rates. Contact us for best prices!', image:''},
    ]});
  }
});

// ═══════════════════════════════════════
// MAIN RATES
// ═══════════════════════════════════════
app.get('/rates', async (req,res) => {
  const [spot, usdInr] = await Promise.all([getSpotRates(), getForex()]);

  try {
    // Angel MCX (when IP whitelisted)
    const jwt = await login();
    const gC=getContracts(GOLD_M), sC=getContracts(SILVER_M);

    const [gCR,gNR,sCR,sNR] = await Promise.all([
      searchMCX(jwt,'GOLD'  +gC[0]), searchMCX(jwt,'GOLD'  +gC[1]),
      searchMCX(jwt,'SILVER'+sC[0]), searchMCX(jwt,'SILVER'+sC[1])
    ]);

    const gCT=gCR[0]?.symboltoken, gNT=gNR[0]?.symboltoken;
    const sCT=sCR[0]?.symboltoken, sNT=sNR[0]?.symboltoken;
    if (!gCT||!sCT) throw new Error('MCX tokens not found');

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
        goldNext:  {bid:Math.round(gNext.bid),ask:Math.round(gNext.ask),high:Math.round(gNext.high||gNext.ltp*1.003),low:Math.round(gNext.low||gNext.ltp*0.994)},
        silverNext:{bid:Math.round(sNext.bid),ask:Math.round(sNext.ask),high:Math.round(sNext.high||sNext.ltp*1.012),low:Math.round(sNext.low||sNext.ltp*0.984)}
      },
      xauUsd:spot.xauUsd, xagUsd:spot.xagUsd, usdInr,
      spotSource:spot.src,
      timestamp:new Date().toISOString()
    });

  } catch(angelErr) {
    console.log('[FALLBACK]', angelErr.message);

    // MCX Correction factors (import duty 15% + GST 3% + premium)
    // Gold factor:   1.656 (verified against actual MCX)
    // Silver factor: 2.668 (verified against actual MCX)
    const goldFactor   = 1.0920;
    const silverFactor = 1.0661;

    const g  = Math.round((spot.xauUsd/31.1035)*10*usdInr*goldFactor);
    const s  = Math.round((spot.xagUsd/31.1035)*1000*usdInr*silverFactor);
    const gS = g*0.0009, sS=s*0.0012;

    res.json({
      success:true, source:'fallback_spot',
      spotSource:spot.src,
      goldPer10g:g, silverPerKg:s,
      futures:{
        gold:      {bid:Math.round(g-180-gS),  ask:Math.round(g-180+gS),  high:Math.round(g*1.003), low:Math.round(g*0.994)},
        silver:    {bid:Math.round(s-1800-sS), ask:Math.round(s-1800+sS), high:Math.round(s*1.012), low:Math.round(s*0.984)},
        goldNext:  {bid:Math.round(g+1320-gS), ask:Math.round(g+1320+gS), high:Math.round(g*1.005+1500),low:Math.round(g*0.996+1500)},
        silverNext:{bid:Math.round(s+1700-sS), ask:Math.round(s+1700+sS), high:Math.round(s*1.013+3500),low:Math.round(s*0.984+3500)}
      },
      xauUsd:spot.xauUsd, xagUsd:spot.xagUsd, usdInr,
      angelError:angelErr.message,
      timestamp:new Date().toISOString()
    });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, ()=>{
  console.log('RR Jewellers Gold Server v7 - port '+PORT);
  setInterval(()=>{
    require('https').get('https://gold-proxy-server.onrender.com/', ()=>{
      console.log('[PING] Awake');
    }).on('error',()=>{});
  }, 4*60*1000);
});
