'use strict';
// ═══════════════════════════════════════════════════════════════════
// RR JEWELLERS GOLD SERVER — Dhan Live Feed v4
// FIXES based on official Dhan v2 docs:
// - Exchange segment: MCX_COMM (not MCX_FO)
// - Header: byte[0]=feedCode, byte[1-2]=msgLen, byte[3]=exchSeg, byte[4-7]=secId(int32LE)
// - Ticker(code=2): byte[8-11]=LTP(float32LE), byte[12-15]=LTT(int32LE)
// - Quote(code=4): byte[8-11]=LTP, [12-13]=LTQ, [14-17]=LTT, [18-21]=ATP,
//                  [22-25]=Vol, [26-29]=TotalSell, [30-33]=TotalBuy,
//                  [34-37]=Open, [38-41]=Close, [42-45]=High, [46-49]=Low
// - Full(code=8):  byte[8-11]=LTP, ...[46-49]=Open, [50-53]=Close,
//                  [54-57]=High, [58-61]=Low, [62-161]=MarketDepth(5x20bytes)
// - RequestCode 21 = Subscribe Full Packet
// ═══════════════════════════════════════════════════════════════════

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

// ─── ENV VARS ──────────────────────────────────────────────────────
const PORT              = process.env.PORT              || 3000;
const SELF_URL          = process.env.SELF_URL          || '';
const SHEET_ID          = process.env.SHEET_ID          || '';
const DHAN_CLIENT_ID    = process.env.DHAN_CLIENT_ID    || '';
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN || '';

const DHAN_API_BASE = 'https://api.dhan.co/v2';
const MONTHS        = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
const GOLD_M        = [0,2,4,6,8,10];
const SILVER_M      = [1,3,5,7,10];

// ─── Dhan Feed Response Codes (from docs Annexure) ─────────────────
const FEED_CODE = {
  INDEX:       1,
  TICKER:      2,  // LTP + LTT only
  QUOTE:       4,  // Full OHLC without depth
  OI:          5,
  PREV_CLOSE:  6,
  MARKET_STATUS:7,
  FULL:        8,  // OHLC + Market Depth
  DISCONNECT:  50,
};

// ─── Dhan Exchange Segment (from docs Annexure) ─────────────────────
// MCX_COMM = 5  ← THIS IS THE CORRECT ONE FOR MCX COMMODITY
const EXCH_SEG = { MCX_COMM: 'MCX_COMM' };

// ─── RUNTIME STATE ─────────────────────────────────────────────────
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
  ws:null, wsStatus:'disconnected', reconnectCount:0,
  reconnectTimer:null, lastConnectAt:null, lastDisconnectAt:null, lastTickAt:null,
  lastRawBufHex: '',
};

// Hardcoded fallback — auto-updated from API
let TOKENS = {
  goldCurrent:   { secId:'436177', symbol:'GOLD-JUN2026-MCX-FUT'   },
  goldNext:      { secId:'436178', symbol:'GOLD-AUG2026-MCX-FUT'   },
  silverCurrent: { secId:'436197', symbol:'SILVER-JUL2026-MCX-FUT' },
  silverNext:    { secId:'436198', symbol:'SILVER-SEP2026-MCX-FUT' },
};

let lastKnownRates = null;
const forexCache = { usdInr:94.5, xauUsd:3310, xagUsd:32.8, updatedAt:null, src:'init' };

// ─── IST HELPERS ───────────────────────────────────────────────────
function getIST() {
  const d = new Date(Date.now() + 5.5*60*60*1000);
  return { year:d.getUTCFullYear(), month:d.getUTCMonth(), day:d.getUTCDate(),
           hour:d.getUTCHours(), min:d.getUTCMinutes(), dow:d.getUTCDay() };
}
function istDayKey() { const i=getIST(); return `${i.year}-${i.month+1}-${i.day}`; }
function isMCXOpen() {
  const {dow,hour,min}=getIST(); if(dow===0) return false;
  const t=hour*60+min;
  return dow===6 ? (t>=540&&t<840) : (t>=540&&t<1435);
}
function resetSessionIfNewDay() {
  const k=istDayKey();
  if(sessionDayKey!==k){
    sessionDayKey=k;
    sessionHL.gold={high:0,low:Infinity};
    sessionHL.silver={high:0,low:Infinity};
    console.log('[SESSION] Reset:',k);
  }
}
function updateSessionHL(sym,ltp,high,low) {
  resetSessionIfNewDay();
  if(ltp>0) { if(ltp>sessionHL[sym].high)sessionHL[sym].high=ltp; if(ltp<sessionHL[sym].low)sessionHL[sym].low=ltp; }
  if(high>0&&high>sessionHL[sym].high) sessionHL[sym].high=high;
  if(low>0&&low<sessionHL[sym].low)   sessionHL[sym].low=low;
}
function tickAgeSeconds() {
  if(!WS.lastTickAt) return Infinity;
  return Math.floor((Date.now()-WS.lastTickAt)/1000);
}
function isDhanLive()  { return WS.wsStatus==='connected'&&tickAgeSeconds()<10&&liveTick.gold.ltp>0; }
function isDhanStale() { const a=tickAgeSeconds(); return liveTick.gold.ltp>0&&a>=10&&a<300; }
function getContracts(validM) {
  const ist=getIST(); let m=ist.month,y=ist.year; const out=[];
  for(let i=0;i<24&&out.length<2;i++){
    if(validM.includes(m)) out.push({month:MONTHS[m],year:y.toString().slice(-2)});
    if(++m>11){m=0;y++;}
  }
  return out;
}
function buildContracts() {
  const gC=getContracts(GOLD_M),sC=getContracts(SILVER_M);
  return {
    gold:  {current:(gC[0]?.month||'')+(gC[0]?.year||''),next:(gC[1]?.month||'')+(gC[1]?.year||'')},
    silver:{current:(sC[0]?.month||'')+(sC[0]?.year||''),next:(sC[1]?.month||'')+(sC[1]?.year||'')},
  };
}

// ─── FOREX REFRESH ─────────────────────────────────────────────────
async function refreshForexAndSpot() {
  let usdInr=0,xauUsd=0,xagUsd=0,src='';
  const fxTry=[
    ['frankfurter', ()=>axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000}).then(r=>r.data?.rates?.INR)],
    ['open.er-api', ()=>axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000}).then(r=>r.data?.rates?.INR)],
    ['fawazahmed0', ()=>axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json',{timeout:5000}).then(r=>r.data?.usd?.inr)],
  ];
  for(const [name,fn] of fxTry){ if(usdInr) break; try{const v=await fn(); if(v>70&&v<110){usdInr=v;src=name;}}catch{} }
  if(!usdInr){usdInr=forexCache.usdInr;src='cached';}
  try{
    const r=await axios.get('https://api.metals.live/v1/spot/gold,silver',{timeout:6000});
    if(Array.isArray(r.data)){const g=r.data.find(x=>x.gold)?.gold,s=r.data.find(x=>x.silver)?.silver;if(g>3000&&g<9000&&s>20&&s<300){xauUsd=g;xagUsd=s;}}
  }catch{}
  if(!xauUsd){try{const [gr,sr]=await Promise.all([axios.get('https://www.gold-api.com/price/XAU',{timeout:7000}),axios.get('https://www.gold-api.com/price/XAG',{timeout:7000})]);const g=gr.data?.price,s=sr.data?.price;if(g>3000&&g<9000&&s>20&&s<300){xauUsd=g;xagUsd=s;}}catch{}}
  if(!xauUsd){xauUsd=forexCache.xauUsd;xagUsd=forexCache.xagUsd;}
  forexCache.usdInr=usdInr;forexCache.xauUsd=xauUsd;forexCache.xagUsd=xagUsd;
  forexCache.updatedAt=new Date().toISOString();forexCache.src=src;
  console.log(`[FOREX] INR=${usdInr} XAU=${xauUsd} XAG=${xagUsd} src=${src}`);
}
function getSpotDerived(){
  const{usdInr,xauUsd,xagUsd,src}=forexCache,F=1.103;
  return{gLtp:Math.round(xauUsd/31.1035*10*usdInr*F),sLtp:Math.round(xagUsd/31.1035*1000*usdInr*F),usdInr,xauUsd,xagUsd,src};
}

// ─── TOKEN RENEW ───────────────────────────────────────────────────
async function renewDhanToken(){
  if(!currentAccessToken||!DHAN_CLIENT_ID){console.warn('[TOKEN] Cannot renew');return false;}
  try{
    const r=await axios.post(`${DHAN_API_BASE}/RenewToken`,{},{
      headers:{'access-token':currentAccessToken,'dhanClientId':DHAN_CLIENT_ID,'Content-Type':'application/json'},
      timeout:12000,
    });
    const t=r.data?.accessToken||r.data?.access_token||r.data?.data?.accessToken;
    if(t){currentAccessToken=t;tokenRenewedAt=new Date().toISOString();console.log('[TOKEN] Renewed');if(WS.ws){try{WS.ws.terminate();}catch{}}WS.wsStatus='disconnected';setTimeout(connectDhan,3000);return true;}
    console.warn('[TOKEN] No token:',JSON.stringify(r.data).slice(0,100));return false;
  }catch(e){console.warn('[TOKEN] Fail:',e.response?.status,e.message.slice(0,80));return false;}
}

// ─── INSTRUMENT LOOKUP ─────────────────────────────────────────────
async function fetchDhanInstruments(){
  try{
    // MCX instruments are in MCX_FO segment in Dhan's instrument list
    const r=await axios.get(`${DHAN_API_BASE}/instrument/MCX_FO`,{
      headers:{'access-token':currentAccessToken,'client-id':DHAN_CLIENT_ID},
      timeout:20000,responseType:'text',
    });
    const lines=r.data.split('\n'),header=lines[0].split(',').map(h=>h.trim().replace(/"/g,''));
    const col={};header.forEach((h,i)=>col[h]=i);
    const secIdCol=col['SEM_SMST_SECURITY_ID']??col['SecurityId']??0;
    const symCol=col['SEM_TRADING_SYMBOL']??col['tradingsymbol']??1;
    const nameCol=col['SEM_INSTRUMENT_NAME']??col['instrumentName']??2;
    const expiryCol=col['SM_EXPIRY_DATE']??col['expiryDate']??5;
    const gold=[],silver=[];
    for(let i=1;i<lines.length;i++){
      const c=lines[i].split(',').map(x=>x.trim().replace(/"/g,''));
      if(!c[secIdCol]) continue;
      const name=(c[nameCol]||'').toUpperCase(),sym=(c[symCol]||'').toUpperCase();
      const item={secId:c[secIdCol],symbol:sym,expiry:c[expiryCol]||''};
      // GOLD futures only (not GOLDM mini, GOLDPETAL, GOLDGUINEA etc)
      if(name==='GOLD'&&sym.includes('FUT')) gold.push(item);
      // SILVER futures only (not SILVERM mini, SILVERMIC etc)
      if(name==='SILVER'&&sym.includes('FUT')) silver.push(item);
    }
    const now=new Date(),sort=arr=>arr.filter(x=>new Date(x.expiry)>now).sort((a,b)=>new Date(a.expiry)-new Date(b.expiry));
    const gS=sort(gold),sS=sort(silver);
    if(gS.length>=2&&sS.length>=2){
      TOKENS.goldCurrent=gS[0];TOKENS.goldNext=gS[1];
      TOKENS.silverCurrent=sS[0];TOKENS.silverNext=sS[1];
      console.log('[INSTRUMENTS] Gold:',gS[0].secId,gS[0].symbol,'|',gS[1].secId,gS[1].symbol);
      console.log('[INSTRUMENTS] Silver:',sS[0].secId,sS[0].symbol,'|',sS[1].secId,sS[1].symbol);
    }else{console.warn('[INSTRUMENTS] Not enough contracts — using hardcoded fallback');}
  }catch(e){console.warn('[INSTRUMENTS] Failed:',e.message.slice(0,80),'— using fallback');}
}

// ─── BINARY PACKET PARSER ──────────────────────────────────────────
// Exact offsets from Dhan v2 official docs:
//
// HEADER (8 bytes, 0-indexed):
//   byte[0]   = Feed Response Code (uint8)
//   byte[1-2] = Message Length (int16 LE)
//   byte[3]   = Exchange Segment (uint8)
//   byte[4-7] = Security ID (int32 LE)
//
// TICKER packet (feedCode=2), total ~16 bytes:
//   byte[8-11]  = LTP (float32 LE)
//   byte[12-15] = LTT (int32 LE)
//
// QUOTE packet (feedCode=4), total ~50 bytes:
//   byte[8-11]  = LTP (float32 LE)
//   byte[12-13] = LTQ (int16 LE)
//   byte[14-17] = LTT (int32 LE)
//   byte[18-21] = ATP (float32 LE)
//   byte[22-25] = Volume (int32 LE)
//   byte[26-29] = Total Sell Qty (int32 LE)
//   byte[30-33] = Total Buy Qty (int32 LE)
//   byte[34-37] = Day Open (float32 LE)
//   byte[38-41] = Day Close (float32 LE)
//   byte[42-45] = Day High (float32 LE)
//   byte[46-49] = Day Low (float32 LE)
//
// FULL packet (feedCode=8), total ~162 bytes:
//   byte[8-11]  = LTP (float32 LE)
//   byte[12-13] = LTQ (int16 LE)
//   byte[14-17] = LTT (int32 LE)
//   byte[18-21] = ATP (float32 LE)
//   byte[22-25] = Volume (int32 LE)
//   byte[26-29] = Total Sell Qty (int32 LE)
//   byte[30-33] = Total Buy Qty (int32 LE)
//   byte[34-37] = OI (int32 LE)
//   byte[38-41] = OI High (int32 LE)
//   byte[42-45] = OI Low (int32 LE)
//   byte[46-49] = Day Open (float32 LE)
//   byte[50-53] = Day Close (float32 LE)
//   byte[54-57] = Day High (float32 LE)
//   byte[58-61] = Day Low (float32 LE)
//   byte[62-161]= Market Depth (5 x 20 bytes)
//     Each depth entry: [BidQty(4),AskQty(4),BidOrders(2),AskOrders(2),BidPrice(4),AskPrice(4)]
function parseDhanPacket(buf) {
  try {
    if(!buf||buf.length<8) return null;
    const feedCode = buf.readUInt8(0);
    const secId    = buf.readInt32LE(4).toString();  // bytes 4-7

    // Ticker packet
    if(feedCode===FEED_CODE.TICKER && buf.length>=16){
      const ltp = buf.readFloatLE(8) / 100;
      if(ltp<100||ltp>9999999) return null;
      return {secId, ltp:Math.round(ltp), bid:Math.round(ltp), ask:Math.round(ltp), high:0, low:0, open:0, mode:'ticker'};
    }

    // Quote packet
    if(feedCode===FEED_CODE.QUOTE && buf.length>=50){
      const ltp  = buf.readFloatLE(8)  / 100;
      if(ltp<100||ltp>9999999) return null;
      const open = buf.readFloatLE(34) / 100;
      const high = buf.readFloatLE(42) / 100;
      const low  = buf.readFloatLE(46) / 100;
      return {secId, ltp:Math.round(ltp), bid:Math.round(ltp), ask:Math.round(ltp),
              high:Math.round(high)||0, low:Math.round(low)||0, open:Math.round(open)||0, mode:'quote'};
    }

    // Full packet — has market depth so we can get bid/ask
    if(feedCode===FEED_CODE.FULL && buf.length>=80){
      const ltp  = buf.readFloatLE(8)  / 100;
      if(ltp<100||ltp>9999999) return null;
      const open = buf.readFloatLE(46) / 100;
      const high = buf.readFloatLE(54) / 100;
      const low  = buf.readFloatLE(58) / 100;
      // Market depth starts at byte 62
      // Each entry: BidQty(4)+AskQty(4)+BidOrders(2)+AskOrders(2)+BidPrice(4)+AskPrice(4) = 20 bytes
      let bid=Math.round(ltp), ask=Math.round(ltp);
      if(buf.length>=82){
        const b = buf.readFloatLE(62+12) / 100;  // first BidPrice at offset 62+12=74
        const a = buf.readFloatLE(62+16) / 100;  // first AskPrice at offset 62+16=78
        if(b>100) bid=Math.round(b);
        if(a>100) ask=Math.round(a);
      }
      return {secId, ltp:Math.round(ltp), bid, ask,
              high:Math.round(high)||0, low:Math.round(low)||0, open:Math.round(open)||0, mode:'full'};
    }

    // Prev Close packet — log but skip
    if(feedCode===FEED_CODE.PREV_CLOSE){return null;}

    // Disconnect packet
    if(feedCode===FEED_CODE.DISCONNECT && buf.length>=10){
      const code=buf.readInt16LE(8);
      console.warn('[WS] Server disconnect code:',code);
      return null;
    }

    return null;
  } catch(e) { return null; }
}

// ─── WEBSOCKET ─────────────────────────────────────────────────────
function getDhanWsUrl(){
  return `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(currentAccessToken)}&clientId=${encodeURIComponent(DHAN_CLIENT_ID)}&authType=2`;
}

function connectDhan(){
  if(!DHAN_CLIENT_ID||!currentAccessToken){console.warn('[WS] Missing creds');return;}
  if(WS.wsStatus==='connecting'||WS.wsStatus==='connected') return;
  WS.wsStatus='connecting'; WS.lastConnectAt=new Date().toISOString();
  console.log('[WS] Connecting...');
  const ws=new WebSocket(getDhanWsUrl(),{handshakeTimeout:15000});
  WS.ws=ws;

  ws.on('open',()=>{
    WS.wsStatus='connected'; WS.reconnectCount=0;
    console.log('[WS] Connected! Subscribing...');
    // Subscribe Quote mode (RequestCode=17) for OHLC without depth
    // Using MCX_COMM as exchange segment per Dhan annexure
    const instruments=[
      {ExchangeSegment:EXCH_SEG.MCX_COMM, SecurityId:TOKENS.goldCurrent.secId},
      {ExchangeSegment:EXCH_SEG.MCX_COMM, SecurityId:TOKENS.goldNext.secId},
      {ExchangeSegment:EXCH_SEG.MCX_COMM, SecurityId:TOKENS.silverCurrent.secId},
      {ExchangeSegment:EXCH_SEG.MCX_COMM, SecurityId:TOKENS.silverNext.secId},
    ].filter(x=>x.SecurityId);

    // Subscribe Quote (17) first
    ws.send(JSON.stringify({RequestCode:17, InstrumentCount:instruments.length, InstrumentList:instruments}));
    console.log('[WS] Subscribed Quote mode for',instruments.length,'instruments');

    // Also subscribe Full (21) for depth/bid-ask
    setTimeout(()=>{
      if(ws.readyState!==WebSocket.OPEN) return;
      ws.send(JSON.stringify({RequestCode:21, InstrumentCount:instruments.length, InstrumentList:instruments}));
      console.log('[WS] Subscribed Full mode for',instruments.length,'instruments');
    },500);
  });

  ws.on('message',(data)=>{
    if(typeof data==='string'){
      try{const m=JSON.parse(data); console.log('[WS] Text:',JSON.stringify(m).slice(0,150));}catch{}
      return;
    }
    const buf=Buffer.isBuffer(data)?data:Buffer.from(data);
    // Log first packet for debugging
    if(!WS.lastRawBufHex){
      WS.lastRawBufHex=buf.slice(0,Math.min(buf.length,32)).toString('hex');
      console.log('[WS] First packet hex:',WS.lastRawBufHex,'len:',buf.length,'feedCode:',buf[0]);
    }
    const tick=parseDhanPacket(buf);
    if(!tick||tick.ltp<=0) return;
    WS.lastTickAt=Date.now();
    const sid=tick.secId;
    if(sid===TOKENS.goldCurrent.secId)  {liveTick.gold      ={...tick,ts:WS.lastTickAt};updateSessionHL('gold',  tick.ltp,tick.high,tick.low);}
    else if(sid===TOKENS.goldNext.secId)      {liveTick.goldNext  ={...tick,ts:WS.lastTickAt};}
    else if(sid===TOKENS.silverCurrent.secId) {liveTick.silver    ={...tick,ts:WS.lastTickAt};updateSessionHL('silver',tick.ltp,tick.high,tick.low);}
    else if(sid===TOKENS.silverNext.secId)    {liveTick.silverNext={...tick,ts:WS.lastTickAt};}
    else {
      // Unknown secId — log once for debugging
      console.log('[WS] Unknown secId:',sid,'ltp:',tick.ltp,'mode:',tick.mode);
    }
  });

  ws.on('close',(code,reason)=>{
    WS.wsStatus='disconnected'; WS.lastDisconnectAt=new Date().toISOString();
    console.log('[WS] Closed code:',code,reason?.toString()?.slice(0,80)||'');
    if(code===807||code===809||code===808){
      console.log('[WS] Token expired — renewing');
      renewDhanToken().then(()=>scheduleReconnect());
    } else { scheduleReconnect(); }
  });

  ws.on('error',(e)=>console.warn('[WS] Error:',e.message.slice(0,100)));
}

function scheduleReconnect(){
  if(WS.reconnectTimer) return;
  WS.reconnectCount++;
  const d=Math.min(2000*Math.pow(2,Math.min(WS.reconnectCount,5)),60000);
  console.log(`[WS] Reconnect in ${d/1000}s (attempt ${WS.reconnectCount})`);
  WS.reconnectTimer=setTimeout(()=>{WS.reconnectTimer=null;connectDhan();},d);
}

// ─── ROUTES ────────────────────────────────────────────────────────
app.get('/rates',async(req,res)=>{
  const marketOpen=isMCXOpen(),contracts=buildContracts(),now=new Date().toISOString();
  const{usdInr,xauUsd,xagUsd}=forexCache;
  if(isDhanLive()){
    const g=liveTick.gold,s=liveTick.silver,gN=liveTick.goldNext,sN=liveTick.silverNext;
    const gH=sessionHL.gold.high||g.high,gL=sessionHL.gold.low===Infinity?g.low:sessionHL.gold.low;
    const sH=sessionHL.silver.high||s.high,sL=sessionHL.silver.low===Infinity?s.low:sessionHL.silver.low;
    const p={success:true,source:'dhan_mcx_live',marketOpen,tickAgeMs:Date.now()-WS.lastTickAt,tickAgeSeconds:tickAgeSeconds(),contracts,goldPer10g:g.ltp,silverPerKg:s.ltp,futures:{gold:{ltp:g.ltp,bid:g.bid,ask:g.ask,high:gH,low:gL,open:g.open},silver:{ltp:s.ltp,bid:s.bid,ask:s.ask,high:sH,low:sL,open:s.open},goldNext:{ltp:gN.ltp||g.ltp,bid:gN.bid||g.bid,ask:gN.ask||g.ask,high:gN.high||gH,low:gN.low||gL,open:gN.open||g.open},silverNext:{ltp:sN.ltp||s.ltp,bid:sN.bid||s.bid,ask:sN.ask||s.ask,high:sN.high||sH,low:sN.low||sL,open:sN.open||s.open}},usdInr,xauUsd,xagUsd,forexUpdatedAt:forexCache.updatedAt,timestamp:now};
    lastKnownRates={...p};return res.json(p);
  }
  if(isDhanStale()||lastKnownRates){
    return res.json({...(lastKnownRates||{}),success:true,source:'last_known_rates',marketOpen,tickAgeSeconds:tickAgeSeconds()===Infinity?null:tickAgeSeconds(),priceAsOf:WS.lastTickAt?new Date(WS.lastTickAt).toISOString():null,usdInr,xauUsd,xagUsd,forexUpdatedAt:forexCache.updatedAt,timestamp:now});
  }
  const d=getSpotDerived();
  return res.json({success:true,source:'spot_derived',marketOpen,note:'Live MCX unavailable — spot→INR',spotSource:d.src,usdInr:d.usdInr,xauUsd:d.xauUsd,xagUsd:d.xagUsd,forexUpdatedAt:forexCache.updatedAt,contracts,goldPer10g:d.gLtp,silverPerKg:d.sLtp,futures:{gold:{ltp:d.gLtp,bid:d.gLtp,ask:d.gLtp,high:null,low:null,open:null},silver:{ltp:d.sLtp,bid:d.sLtp,ask:d.sLtp,high:null,low:null,open:null},goldNext:{ltp:null,bid:null,ask:null,high:null,low:null,open:null},silverNext:{ltp:null,bid:null,ask:null,high:null,low:null,open:null}},timestamp:now});
});

app.get('/debug',(req,res)=>res.json({
  server:'RR Jewellers — Dhan v4',wsStatus:WS.wsStatus,
  lastTickAt:WS.lastTickAt?new Date(WS.lastTickAt).toISOString():null,
  tickAgeSeconds:tickAgeSeconds()===Infinity?null:tickAgeSeconds(),
  reconnectCount:WS.reconnectCount,lastConnectAt:WS.lastConnectAt,lastDisconnectAt:WS.lastDisconnectAt,
  currentSource:isDhanLive()?'dhan_mcx_live':(isDhanStale()||lastKnownRates?'last_known_rates':'spot_derived'),
  marketOpen:isMCXOpen(),tokenRenewedAt,sessionHL,liveTick,tokens:TOKENS,forexCache,
  lastRawBufHex:WS.lastRawBufHex,
  lastKnownRatesAt:lastKnownRates?.timestamp||null,
  credentials:{clientId:!!DHAN_CLIENT_ID,accessToken:!!currentAccessToken},
}));

app.get('/spot-test',(req,res)=>res.json({...getSpotDerived(),forexCache}));
app.get('/forex-test',(req,res)=>res.json(forexCache));
app.get('/cache-status',(req,res)=>res.json({goldContracts:getContracts(GOLD_M).map(c=>c.month+c.year),silverContracts:getContracts(SILVER_M).map(c=>c.month+c.year),tokens:TOKENS,wsStatus:WS.wsStatus,tickAgeSeconds:tickAgeSeconds()===Infinity?null:tickAgeSeconds(),lastRawBufHex:WS.lastRawBufHex}));
app.get('/token-renew',async(req,res)=>{const ok=await renewDhanToken();res.json({success:ok,tokenRenewedAt,wsStatus:WS.wsStatus});});
app.get('/ping',(req,res)=>res.json({ok:true,ts:Date.now()}));
app.get('/updates',async(req,res)=>{
  try{if(!SHEET_ID)throw new Error('no SHEET_ID');const url=`https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;const r=await axios.get(url,{timeout:8000});const json=r.data.replace(/.*?({.*}).*/s,'$1');const data=JSON.parse(json);const rows=data.table.rows.map(row=>({date:row.c[0]?.v||'',title:row.c[1]?.v||'',content:row.c[2]?.v||'',image:row.c[3]?.v||''}));res.json({success:true,updates:rows.filter(r=>r.title)});}
  catch{res.json({success:true,updates:[{date:'Today',title:'Welcome to R.R. Jewellers',content:'Live gold & silver rates.',image:''}]});}
});
app.get('/',(req,res)=>res.json({status:'RR Jewellers — Dhan v4',wsStatus:WS.wsStatus,endpoints:['/rates','/debug','/spot-test','/forex-test','/cache-status','/ping','/token-renew','/updates']}));

// ─── STARTUP ───────────────────────────────────────────────────────
app.listen(PORT,'0.0.0.0',async()=>{
  console.log(`[STARTUP] RR Jewellers Gold Server v4 — port ${PORT}`);
  await refreshForexAndSpot();
  await fetchDhanInstruments();
  connectDhan();
  setInterval(refreshForexAndSpot,5*60*1000);
  setInterval(resetSessionIfNewDay,60*1000);
  setInterval(()=>axios.get((SELF_URL||`http://localhost:${PORT}`)+'/ping').catch(()=>{}),4*60*1000);
  setInterval(async()=>{console.log('[TOKEN] Auto-renew 20hr');await renewDhanToken();},20*60*60*1000);
  setInterval(()=>{if(WS.wsStatus==='disconnected'&&!WS.reconnectTimer){console.log('[HEALTH] WS dead — reconnect');connectDhan();}},2*60*1000);
  setInterval(fetchDhanInstruments,24*60*60*1000);
});
