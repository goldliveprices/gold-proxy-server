'use strict';
// ═══════════════════════════════════════════════════════════════════
// RR JEWELLERS GOLD SERVER — Dhan Live Feed Edition v2
// - RenewToken API: auto token refresh, no TOTP needed at runtime
// - Live USD/INR + XAU/USD + XAG/USD cached every 5 min
// - No mock data anywhere
// - Render.com deploy — Data API needs NO static IP
// ═══════════════════════════════════════════════════════════════════

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

// ═══════════════════════════════════════
// ENV VARS — set in Render Dashboard
// ═══════════════════════════════════════
const PORT              = process.env.PORT              || 3000;
const SELF_URL          = process.env.SELF_URL          || '';
const SHEET_ID          = process.env.SHEET_ID          || '';
const DHAN_CLIENT_ID    = process.env.DHAN_CLIENT_ID    || '';
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN || '';

// ═══════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════
const DHAN_FEED_URL = 'wss://api-feed.dhan.co';
const DHAN_API_BASE = 'https://api.dhan.co/v2';
const MONTHS        = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
const GOLD_M        = [0,2,4,6,8,10];
const SILVER_M      = [1,3,5,7,10];

// ═══════════════════════════════════════
// RUNTIME STATE
// ═══════════════════════════════════════
let currentAccessToken = DHAN_ACCESS_TOKEN;
let tokenRenewedAt     = null;

const liveTick = {
  gold:       { ltp:0, bid:0, ask:0, high:0, low:0, open:0, ts:0 },
  silver:     { ltp:0, bid:0, ask:0, high:0, low:0, open:0, ts:0 },
  goldNext:   { ltp:0, bid:0, ask:0, high:0, low:0, open:0, ts:0 },
  silverNext: { ltp:0, bid:0, ask:0, high:0, low:0, open:0, ts:0 },
};

let sessionDayKey = '';
const sessionHL = {
  gold:   { high:0, low:Infinity },
  silver: { high:0, low:Infinity },
};

const WS = {
  ws:               null,
  wsStatus:         'disconnected',
  reconnectCount:   0,
  reconnectTimer:   null,
  lastConnectAt:    null,
  lastDisconnectAt: null,
  lastTickAt:       null,
};

let TOKENS = {
  goldCurrent:   { secId:'436177', symbol:'GOLD-JUN2026-MCX-FUT'   },
  goldNext:      { secId:'436178', symbol:'GOLD-AUG2026-MCX-FUT'   },
  silverCurrent: { secId:'436197', symbol:'SILVER-JUL2026-MCX-FUT' },
  silverNext:    { secId:'436198', symbol:'SILVER-SEP2026-MCX-FUT' },
};

let lastKnownRates = null;

// Forex + spot cache (refreshed every 5 min)
const forexCache = {
  usdInr: 94.5, xauUsd: 3310, xagUsd: 32.8,
  updatedAt: null, src: 'init',
};

// ═══════════════════════════════════════
// IST HELPERS
// ═══════════════════════════════════════
function getIST() {
  const d = new Date(Date.now() + 5.5*60*60*1000);
  return { year:d.getUTCFullYear(), month:d.getUTCMonth(), day:d.getUTCDate(),
           hour:d.getUTCHours(), min:d.getUTCMinutes(), dow:d.getUTCDay() };
}
function istDayKey() { const i=getIST(); return `${i.year}-${i.month+1}-${i.day}`; }

function isMCXOpen() {
  const { dow, hour, min } = getIST();
  if (dow===0) return false;
  const t = hour*60+min;
  if (dow===6) return t>=540 && t<840;
  return t>=540 && t<1435;
}

function resetSessionIfNewDay() {
  const k = istDayKey();
  if (sessionDayKey !== k) {
    sessionDayKey = k;
    sessionHL.gold   = { high:0, low:Infinity };
    sessionHL.silver = { high:0, low:Infinity };
    console.log('[SESSION] Reset:', k);
  }
}

function updateSessionHL(sym, ltp, high, low) {
  resetSessionIfNewDay();
  if (ltp>0)  { if(ltp>sessionHL[sym].high)  sessionHL[sym].high=ltp;  if(ltp<sessionHL[sym].low)  sessionHL[sym].low=ltp;  }
  if (high>0 && high>sessionHL[sym].high) sessionHL[sym].high=high;
  if (low>0  && low<sessionHL[sym].low)   sessionHL[sym].low=low;
}

function tickAgeSeconds() {
  if (!WS.lastTickAt) return Infinity;
  return Math.floor((Date.now()-WS.lastTickAt)/1000);
}

function isDhanLive()  { return WS.wsStatus==='connected' && tickAgeSeconds()<10  && liveTick.gold.ltp>0; }
function isDhanStale() { const a=tickAgeSeconds(); return liveTick.gold.ltp>0 && a>=10 && a<300; }

function getContracts(validM) {
  const ist=getIST(); let m=ist.month, y=ist.year; const out=[];
  for (let i=0; i<24&&out.length<2; i++) {
    if (validM.includes(m)) out.push({ month:MONTHS[m], year:y.toString().slice(-2) });
    if (++m>11) { m=0; y++; }
  }
  return out;
}

function buildContracts() {
  const gC=getContracts(GOLD_M), sC=getContracts(SILVER_M);
  return {
    gold:   { current:(gC[0]?.month||'')+(gC[0]?.year||''), next:(gC[1]?.month||'')+(gC[1]?.year||'') },
    silver: { current:(sC[0]?.month||'')+(sC[0]?.year||''), next:(sC[1]?.month||'')+(sC[1]?.year||'') },
  };
}

// ═══════════════════════════════════════
// FOREX + SPOT REFRESH (every 5 min)
// ═══════════════════════════════════════
async function refreshForexAndSpot() {
  let usdInr=0, xauUsd=0, xagUsd=0, src='';

  // USD/INR — 3 free sources
  try {
    const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000});
    const v = r.data?.rates?.INR;
    if (v>70&&v<110) { usdInr=v; src='frankfurter'; }
  } catch {}

  if (!usdInr) {
    try {
      const r = await axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000});
      const v = r.data?.rates?.INR;
      if (v>70&&v<110) { usdInr=v; src='open.er-api'; }
    } catch {}
  }

  if (!usdInr) {
    try {
      const r = await axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json',{timeout:5000});
      const v = r.data?.usd?.inr;
      if (v>70&&v<110) { usdInr=v; src='fawazahmed0'; }
    } catch {}
  }

  if (!usdInr) { usdInr=forexCache.usdInr; src='cached'; }

  // XAU/USD + XAG/USD — 2 free sources
  try {
    const r = await axios.get('https://api.metals.live/v1/spot/gold,silver',{timeout:6000});
    if (Array.isArray(r.data)) {
      const g=r.data.find(x=>x.gold)?.gold, s=r.data.find(x=>x.silver)?.silver;
      if (g>3000&&g<9000&&s>20&&s<300) { xauUsd=g; xagUsd=s; }
    }
  } catch {}

  if (!xauUsd) {
    try {
      const [gr,sr] = await Promise.all([
        axios.get('https://www.gold-api.com/price/XAU',{timeout:7000}),
        axios.get('https://www.gold-api.com/price/XAG',{timeout:7000}),
      ]);
      const g=gr.data?.price, s=sr.data?.price;
      if (g>3000&&g<9000&&s>20&&s<300) { xauUsd=g; xagUsd=s; }
    } catch {}
  }

  if (!xauUsd) { xauUsd=forexCache.xauUsd; xagUsd=forexCache.xagUsd; }

  forexCache.usdInr=usdInr; forexCache.xauUsd=xauUsd; forexCache.xagUsd=xagUsd;
  forexCache.updatedAt=new Date().toISOString(); forexCache.src=src;
  console.log(`[FOREX] INR=${usdInr} XAU=${xauUsd} XAG=${xagUsd} src=${src}`);
}

function getSpotDerived() {
  const {usdInr,xauUsd,xagUsd,src}=forexCache, F=1.103;
  return {
    gLtp: Math.round(xauUsd/31.1035*10*usdInr*F),
    sLtp: Math.round(xagUsd/31.1035*1000*usdInr*F),
    usdInr, xauUsd, xagUsd, src,
  };
}

// ═══════════════════════════════════════
// TOKEN RENEW — Dhan RenewToken API
// Uses current active token, no TOTP needed
// ═══════════════════════════════════════
async function renewDhanToken() {
  if (!currentAccessToken || !DHAN_CLIENT_ID) {
    console.warn('[TOKEN] Cannot renew — missing token or clientId'); return false;
  }
  try {
    console.log('[TOKEN] Calling RenewToken API...');
    const r = await axios.post(`${DHAN_API_BASE}/RenewToken`, {}, {
      headers: {
        'access-token': currentAccessToken,
        'dhanClientId': DHAN_CLIENT_ID,
        'Content-Type': 'application/json',
      },
      timeout: 12000,
    });
    const newToken = r.data?.accessToken || r.data?.access_token || r.data?.data?.accessToken;
    if (newToken) {
      currentAccessToken = newToken;
      tokenRenewedAt     = new Date().toISOString();
      console.log('[TOKEN] Renewed — valid 24hrs from', tokenRenewedAt);
      // Reconnect WS with new token
      if (WS.ws) { try { WS.ws.terminate(); } catch {} }
      WS.wsStatus = 'disconnected';
      setTimeout(connectDhan, 3000);
      return true;
    }
    console.warn('[TOKEN] No token in response:', JSON.stringify(r.data).slice(0,120));
    return false;
  } catch(e) {
    console.warn('[TOKEN] RenewToken failed:', e.response?.status, e.message.slice(0,80));
    return false;
  }
}

// ═══════════════════════════════════════
// DHAN INSTRUMENT LOOKUP
// ═══════════════════════════════════════
async function fetchDhanInstruments() {
  try {
    const r = await axios.get(`${DHAN_API_BASE}/instrument/MCX_FO`, {
      headers:{'access-token':currentAccessToken,'client-id':DHAN_CLIENT_ID},
      timeout:20000, responseType:'text',
    });
    const lines=r.data.split('\n'), header=lines[0].split(',').map(h=>h.trim().replace(/"/g,''));
    const col={}; header.forEach((h,i)=>col[h]=i);
    const secIdCol  = col['SEM_SMST_SECURITY_ID'] ?? col['SecurityId']    ?? 0;
    const symCol    = col['SEM_TRADING_SYMBOL']   ?? col['tradingsymbol'] ?? 1;
    const nameCol   = col['SEM_INSTRUMENT_NAME']  ?? col['instrumentName']?? 2;
    const expiryCol = col['SM_EXPIRY_DATE']       ?? col['expiryDate']    ?? 5;
    const gold=[],silver=[];
    for (let i=1;i<lines.length;i++) {
      const c=lines[i].split(',').map(x=>x.trim().replace(/"/g,''));
      if (!c[secIdCol]) continue;
      const name=(c[nameCol]||'').toUpperCase(), sym=(c[symCol]||'').toUpperCase();
      const item={secId:c[secIdCol],symbol:sym,expiry:c[expiryCol]||''};
      if (name==='GOLD'   || sym.startsWith('GOLD-'))   gold.push(item);
      if (name==='SILVER' || sym.startsWith('SILVER-')) silver.push(item);
    }
    const now=new Date(), sort=arr=>arr.filter(x=>new Date(x.expiry)>now).sort((a,b)=>new Date(a.expiry)-new Date(b.expiry));
    const gS=sort(gold), sS=sort(silver);
    if (gS.length>=2 && sS.length>=2) {
      TOKENS.goldCurrent=gS[0]; TOKENS.goldNext=gS[1];
      TOKENS.silverCurrent=sS[0]; TOKENS.silverNext=sS[1];
      console.log('[INSTRUMENTS] Gold:',gS[0].symbol,gS[1].symbol);
      console.log('[INSTRUMENTS] Silver:',sS[0].symbol,sS[1].symbol);
    } else { console.warn('[INSTRUMENTS] Not enough contracts — using hardcoded fallback'); }
  } catch(e) { console.warn('[INSTRUMENTS] Failed:',e.message.slice(0,80),'— using fallback'); }
}

// ═══════════════════════════════════════
// BINARY PACKET PARSER (Dhan v2 Little Endian)
// ═══════════════════════════════════════
function parseDhanPacket(buf) {
  try {
    if (!buf || buf.length<10) return null;
    const msgType=buf.readUInt8(0), secId=buf.readUInt32LE(2).toString();
    if (msgType===1||msgType===11) {
      const ltp=Math.round(buf.readFloatLE(6)/100);
      if (ltp<=100||ltp>1000000) return null;
      return {secId,ltp,bid:ltp,ask:ltp,high:0,low:0,open:0,mode:'ltp'};
    }
    if ((msgType===2||msgType===21)&&buf.length>=50) {
      const ltp=Math.round(buf.readFloatLE(6)/100);
      if (ltp<=100||ltp>1000000) return null;
      let open=0,high=0,low=0,bid=ltp,ask=ltp;
      if (buf.length>=58) {
        open=Math.round(buf.readDoubleLE(10)/100)||0;
        high=Math.round(buf.readDoubleLE(18)/100)||0;
        low =Math.round(buf.readDoubleLE(26)/100)||0;
      }
      if (buf.length>=74) {
        const b=Math.round(buf.readDoubleLE(42)/100), a=Math.round(buf.readDoubleLE(50)/100);
        if(b>0)bid=b; if(a>0)ask=a;
      }
      return {secId,ltp,bid,ask,high,low,open,mode:'quote'};
    }
    return null;
  } catch { return null; }
}

// ═══════════════════════════════════════
// WEBSOCKET
// ═══════════════════════════════════════
function connectDhan() {
  if (!DHAN_CLIENT_ID||!currentAccessToken) { console.warn('[WS] Missing credentials'); return; }
  if (WS.wsStatus==='connecting'||WS.wsStatus==='connected') return;
  WS.wsStatus='connecting'; WS.lastConnectAt=new Date().toISOString();
  console.log('[WS] Connecting...');
  const ws=new WebSocket(DHAN_FEED_URL,{handshakeTimeout:12000}); WS.ws=ws;

  ws.on('open',()=>{
    WS.wsStatus='connected'; WS.reconnectCount=0;
    console.log('[WS] Connected — sending auth');
    ws.send(JSON.stringify({LoginReq:{MsgCode:11,ClientId:DHAN_CLIENT_ID,Token:currentAccessToken}}));
    setTimeout(()=>{
      if (ws.readyState!==WebSocket.OPEN) return;
      const instruments=[TOKENS.goldCurrent.secId,TOKENS.goldNext.secId,TOKENS.silverCurrent.secId,TOKENS.silverNext.secId]
        .filter(Boolean).map(id=>({ExchangeSegment:'MCX_FO',SecurityId:id}));
      ws.send(JSON.stringify({RequestCode:21,InstrumentCount:instruments.length,InstrumentList:instruments}));
      console.log('[WS] Subscribed',instruments.length,'instruments');
    },1000);
  });

  ws.on('message',(data)=>{
    if (typeof data==='string') {
      try {
        const msg=JSON.parse(data);
        if (msg?.LoginResp?.Response==='Success') { console.log('[WS] Auth OK'); }
        else if (msg?.LoginResp?.Response) {
          console.warn('[WS] Auth failed:',msg.LoginResp.Response);
          const r=(msg.LoginResp.Response||'').toLowerCase();
          if (r.includes('invalid')||r.includes('expired')||r.includes('unauthori')) {
            console.log('[WS] Token rejected — renewing...');
            renewDhanToken();
          }
        }
      } catch {}
      return;
    }
    const buf=Buffer.isBuffer(data)?data:Buffer.from(data);
    const tick=parseDhanPacket(buf);
    if (!tick||tick.ltp<=0) return;
    WS.lastTickAt=Date.now();
    if      (tick.secId===TOKENS.goldCurrent.secId)   { liveTick.gold      ={...tick,ts:WS.lastTickAt}; updateSessionHL('gold',  tick.ltp,tick.high,tick.low); }
    else if (tick.secId===TOKENS.goldNext.secId)       { liveTick.goldNext  ={...tick,ts:WS.lastTickAt}; }
    else if (tick.secId===TOKENS.silverCurrent.secId)  { liveTick.silver    ={...tick,ts:WS.lastTickAt}; updateSessionHL('silver',tick.ltp,tick.high,tick.low); }
    else if (tick.secId===TOKENS.silverNext.secId)     { liveTick.silverNext={...tick,ts:WS.lastTickAt}; }
  });

  ws.on('close',(code,reason)=>{
    WS.wsStatus='disconnected'; WS.lastDisconnectAt=new Date().toISOString();
    console.log('[WS] Disconnected code:',code,reason?.toString()?.slice(0,60)||'');
    if (code===4001||code===401) { renewDhanToken().then(()=>scheduleReconnect()); }
    else { scheduleReconnect(); }
  });

  ws.on('error',(e)=>console.warn('[WS] Error:',e.message.slice(0,80)));
}

function scheduleReconnect() {
  if (WS.reconnectTimer) return;
  WS.reconnectCount++;
  const delay=Math.min(2000*Math.pow(2,Math.min(WS.reconnectCount,5)),60000);
  console.log(`[WS] Reconnect in ${delay/1000}s (attempt ${WS.reconnectCount})`);
  WS.reconnectTimer=setTimeout(()=>{WS.reconnectTimer=null;connectDhan();},delay);
}

// ═══════════════════════════════════════
// ROUTES
// ═══════════════════════════════════════
app.get('/rates', async (req,res)=>{
  const marketOpen=isMCXOpen(), contracts=buildContracts(), now=new Date().toISOString();
  const {usdInr,xauUsd,xagUsd}=forexCache;

  if (isDhanLive()) {
    const g=liveTick.gold,s=liveTick.silver,gN=liveTick.goldNext,sN=liveTick.silverNext;
    const gH=sessionHL.gold.high||g.high, gL=sessionHL.gold.low===Infinity?g.low:sessionHL.gold.low;
    const sH=sessionHL.silver.high||s.high, sL=sessionHL.silver.low===Infinity?s.low:sessionHL.silver.low;
    const payload={
      success:true, source:'dhan_mcx_live', marketOpen,
      tickAgeMs:Date.now()-WS.lastTickAt, tickAgeSeconds:tickAgeSeconds(), contracts,
      goldPer10g:g.ltp, silverPerKg:s.ltp,
      futures:{
        gold:      {ltp:g.ltp,         bid:g.bid,         ask:g.ask,         high:gH,        low:gL,        open:g.open},
        silver:    {ltp:s.ltp,         bid:s.bid,         ask:s.ask,         high:sH,        low:sL,        open:s.open},
        goldNext:  {ltp:gN.ltp||g.ltp, bid:gN.bid||g.bid, ask:gN.ask||g.ask, high:gN.high||gH, low:gN.low||gL, open:gN.open||g.open},
        silverNext:{ltp:sN.ltp||s.ltp, bid:sN.bid||s.bid, ask:sN.ask||s.ask, high:sN.high||sH, low:sN.low||sL, open:sN.open||s.open},
      },
      usdInr, xauUsd, xagUsd, forexUpdatedAt:forexCache.updatedAt, timestamp:now,
    };
    lastKnownRates={...payload};
    return res.json(payload);
  }

  if (isDhanStale()||lastKnownRates) {
    return res.json({
      ...(lastKnownRates||{}), success:true, source:'last_known_rates', marketOpen,
      tickAgeSeconds:tickAgeSeconds()===Infinity?null:tickAgeSeconds(),
      priceAsOf:WS.lastTickAt?new Date(WS.lastTickAt).toISOString():null,
      usdInr, xauUsd, xagUsd, forexUpdatedAt:forexCache.updatedAt, timestamp:now,
    });
  }

  const d=getSpotDerived();
  return res.json({
    success:true, source:'spot_derived', marketOpen,
    note:'Live MCX unavailable — international spot → INR',
    spotSource:d.src, usdInr:d.usdInr, xauUsd:d.xauUsd, xagUsd:d.xagUsd,
    forexUpdatedAt:forexCache.updatedAt, contracts,
    goldPer10g:d.gLtp, silverPerKg:d.sLtp,
    futures:{
      gold:      {ltp:d.gLtp,bid:d.gLtp,ask:d.gLtp,high:null,low:null,open:null},
      silver:    {ltp:d.sLtp,bid:d.sLtp,ask:d.sLtp,high:null,low:null,open:null},
      goldNext:  {ltp:null,bid:null,ask:null,high:null,low:null,open:null},
      silverNext:{ltp:null,bid:null,ask:null,high:null,low:null,open:null},
    },
    timestamp:now,
  });
});

app.get('/debug',(req,res)=>res.json({
  server:'RR Jewellers Gold Server — Dhan v2', wsStatus:WS.wsStatus,
  lastTickAt:WS.lastTickAt?new Date(WS.lastTickAt).toISOString():null,
  tickAgeSeconds:tickAgeSeconds()===Infinity?null:tickAgeSeconds(),
  reconnectCount:WS.reconnectCount, lastConnectAt:WS.lastConnectAt, lastDisconnectAt:WS.lastDisconnectAt,
  currentSource:isDhanLive()?'dhan_mcx_live':(isDhanStale()||lastKnownRates?'last_known_rates':'spot_derived'),
  marketOpen:isMCXOpen(), tokenRenewedAt, sessionHL, liveTick, tokens:TOKENS,
  forexCache, lastKnownRatesAt:lastKnownRates?.timestamp||null,
  credentials:{clientId:!!DHAN_CLIENT_ID,accessToken:!!currentAccessToken},
}));

app.get('/spot-test',(req,res)=>res.json({...getSpotDerived(),forexCache}));
app.get('/forex-test',(req,res)=>res.json({usdInr:forexCache.usdInr,xauUsd:forexCache.xauUsd,xagUsd:forexCache.xagUsd,updatedAt:forexCache.updatedAt,src:forexCache.src}));
app.get('/cache-status',(req,res)=>res.json({goldContracts:getContracts(GOLD_M).map(c=>c.month+c.year),silverContracts:getContracts(SILVER_M).map(c=>c.month+c.year),tokens:TOKENS,wsStatus:WS.wsStatus,tickAgeSeconds:tickAgeSeconds()===Infinity?null:tickAgeSeconds(),lastKnownRatesAt:lastKnownRates?.timestamp||null,forexUpdatedAt:forexCache.updatedAt}));

app.get('/token-renew', async(req,res)=>{
  const ok=await renewDhanToken();
  res.json({success:ok,tokenRenewedAt,wsStatus:WS.wsStatus});
});

app.get('/ping',(req,res)=>res.json({ok:true,ts:Date.now()}));

app.get('/updates', async(req,res)=>{
  try {
    if (!SHEET_ID) throw new Error('SHEET_ID not set');
    const url=`https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r=await axios.get(url,{timeout:8000});
    const json=r.data.replace(/.*?({.*}).*/s,'$1');
    const data=JSON.parse(json);
    const rows=data.table.rows.map(row=>({date:row.c[0]?.v||'',title:row.c[1]?.v||'',content:row.c[2]?.v||'',image:row.c[3]?.v||''}));
    res.json({success:true,updates:rows.filter(r=>r.title)});
  } catch {
    res.json({success:true,updates:[{date:'Today',title:'Welcome to R.R. Jewellers',content:'Live gold & silver rates. Contact us for best prices!',image:''}]});
  }
});

app.get('/',(req,res)=>res.json({status:'RR Jewellers Gold Server — Dhan v2',wsStatus:WS.wsStatus,endpoints:['/rates','/debug','/spot-test','/forex-test','/cache-status','/ping','/token-renew','/updates']}));

// ═══════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════
app.listen(PORT,'0.0.0.0', async()=>{
  console.log(`[STARTUP] RR Jewellers Gold Server v2 — port ${PORT}`);

  await refreshForexAndSpot();       // 1. Forex immediately
  await fetchDhanInstruments();      // 2. Instrument tokens
  connectDhan();                     // 3. WebSocket

  setInterval(refreshForexAndSpot, 5*60*1000);       // 4. Forex every 5 min
  setInterval(resetSessionIfNewDay,    60*1000);      // 5. Session reset check
  setInterval(()=>axios.get((SELF_URL||`http://localhost:${PORT}`)+'/ping').catch(()=>{}), 4*60*1000); // 6. Keep-alive

  // 7. Auto token renew every 20 hours (token valid 24hr)
  setInterval(async()=>{
    console.log('[TOKEN] Scheduled auto-renew (20hr interval)');
    await renewDhanToken();
  }, 20*60*60*1000);

  // 8. WS health check every 2 min
  setInterval(()=>{
    if (WS.wsStatus==='disconnected'&&!WS.reconnectTimer) {
      console.log('[HEALTH] WS dead — reconnecting'); connectDhan();
    }
  }, 2*60*1000);

  // 9. Instrument refresh every 24hr
  setInterval(fetchDhanInstruments, 24*60*60*1000);
});
