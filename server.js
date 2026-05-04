'use strict';
const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

const PORT              = process.env.PORT              || 3000;
const SELF_URL          = process.env.SELF_URL          || '';
const DHAN_CLIENT_ID    = process.env.DHAN_CLIENT_ID    || '';
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN || '';

const INSTRUMENTS = [
  { secId: '436177', segment: 'MCX_COMM', key: 'gold',       name: 'GOLD-JUN2026' },
  { secId: '436178', segment: 'MCX_COMM', key: 'goldNext',   name: 'GOLD-AUG2026' },
  { secId: '436197', segment: 'MCX_COMM', key: 'silver',     name: 'SILVER-JUL2026' },
  { secId: '436198', segment: 'MCX_COMM', key: 'silverNext', name: 'SILVER-SEP2026' },
];
const TOKEN_MAP = {};
INSTRUMENTS.forEach(i => { TOKEN_MAP[i.secId] = i; });

const emptyTick = () => ({ ltp:0,bid:0,ask:0,high:0,low:0,open:0,prevClose:0,ts:0 });
const liveTick  = { gold:emptyTick(),goldNext:emptyTick(),silver:emptyTick(),silverNext:emptyTick() };
const sessionHL = { gold:{high:0,low:Infinity}, silver:{high:0,low:Infinity} };

const WS = {
  ws:null, status:'disconnected',
  reconnectCount:0, reconnectTimer:null,
  lastConnectAt:null, lastDisconnectAt:null,
  lastTickAt:null, lastDisconnectCode:null,
  lastRawBufHex:'', lastTextMsg:'',
  packetsReceived:0,
};
let lastKnownRates = null;

const forexCache = { usdInr:94.5,xauUsd:3310,xagUsd:32.8,updatedAt:null,src:'init' };

async function refreshForex() {
  const sources = [
    ['frankfurter', async()=>(await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000})).data.rates.INR],
    ['open.er-api', async()=>(await axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000})).data.rates.INR],
  ];
  let usdInr=0,src='';
  for(const [name,fn] of sources){
    if(usdInr) break;
    try { const v=await fn(); if(v>70&&v<110){usdInr=v;src=name;} } catch(e){console.warn('[FOREX]',name,e.message);}
  }
  let xauUsd=0,xagUsd=0;
  try {
    const r=await axios.get('https://api.metals.live/v1/spot/gold,silver',{timeout:6000});
    if(Array.isArray(r.data)){
      const g=r.data.find(x=>x.gold),s=r.data.find(x=>x.silver);
      if(g&&g.gold>3000)xauUsd=g.gold; if(s&&s.silver>20)xagUsd=s.silver;
    }
  } catch(e){console.warn('[SPOT]',e.message);}
  if(!xauUsd){ try {
    const [g,s]=await Promise.all([axios.get('https://www.gold-api.com/price/XAU',{timeout:7000}),axios.get('https://www.gold-api.com/price/XAG',{timeout:7000})]);
    if(g.data.price>3000)xauUsd=g.data.price; if(s.data.price>20)xagUsd=s.data.price;
  } catch(e){console.warn('[SPOT2]',e.message);} }
  forexCache.usdInr=usdInr||forexCache.usdInr;
  forexCache.xauUsd=xauUsd||forexCache.xauUsd;
  forexCache.xagUsd=xagUsd||forexCache.xagUsd;
  forexCache.updatedAt=new Date().toISOString();
  forexCache.src=src;
  console.log('[FOREX] usdInr=%s xauUsd=%s src=%s',forexCache.usdInr,forexCache.xauUsd,src);
}

function spotDerived(){
  const {usdInr,xauUsd,xagUsd}=forexCache,F=1.103;
  return {
    goldPer10g: Math.round((xauUsd/31.1035)*10*usdInr*F),
    silverPerKg:Math.round((xagUsd/31.1035)*1000*usdInr*F),
  };
}

function isMCXOpen(){
  const d=new Date(Date.now()+5.5*3600000),dow=d.getUTCDay(),t=d.getUTCHours()*60+d.getUTCMinutes();
  if(dow===0)return false;
  return dow===6?(t>=540&&t<840):(t>=540&&t<1435);
}
function tickAge(){ return WS.lastTickAt?Math.floor((Date.now()-WS.lastTickAt)/1000):Infinity; }
function isDhanLive(){ return WS.status==='connected'&&tickAge()<15&&liveTick.gold.ltp>0; }
function updateHL(sym,ltp,high,low){
  if(ltp>0){if(ltp>sessionHL[sym].high)sessionHL[sym].high=ltp;if(ltp<sessionHL[sym].low)sessionHL[sym].low=ltp;}
  if(high>0&&high>sessionHL[sym].high)sessionHL[sym].high=high;
  if(low>0&&low<sessionHL[sym].low)sessionHL[sym].low=low;
}

function parseBuf(buf){
  try{
    if(!buf||buf.length<8)return null;
    const fc=buf.readUInt8(0);
    const secId=buf.readInt32LE(4).toString();
    if(fc===50){WS.lastDisconnectCode=buf.length>=10?buf.readInt16LE(8):0;console.warn('[WS] Disconnect code=',WS.lastDisconnectCode);return null;}
    if(fc===6&&buf.length>=16){return{type:'prevClose',secId,prevClose:buf.readInt32LE(8)};}
    if(fc===2&&buf.length>=16){const ltp=buf.readFloatLE(8);if(!Number.isFinite(ltp)||ltp<=0)return null;return{type:'ticker',secId,ltp:Math.round(ltp)};}
    if(fc===4&&buf.length>=50){const ltp=buf.readFloatLE(8);if(!Number.isFinite(ltp)||ltp<=0)return null;return{type:'quote',secId,ltp:Math.round(ltp),bid:Math.round(ltp),ask:Math.round(ltp),open:Math.round(buf.readFloatLE(34)),high:Math.round(buf.readFloatLE(42)),low:Math.round(buf.readFloatLE(46))};}
    if(WS.packetsReceived<=15)console.log('[PARSE] fc=%d len=%d hex=%s',fc,buf.length,buf.slice(0,16).toString('hex'));
    return null;
  }catch(e){console.warn('[PARSE]',e.message);return null;}
}

// ─── TRY BOTH URLs ────────────────────────────────────────────────
// Strategy: Try V2 URL first (with query params), if fails try V1 URL
const WS_URLS = [
  // URL 1: V2 with authType=2
  ()=>`wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(DHAN_ACCESS_TOKEN)}&clientId=${encodeURIComponent(DHAN_CLIENT_ID)}&authType=2`,
  // URL 2: V2 with authType=1  
  ()=>`wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(DHAN_ACCESS_TOKEN)}&clientId=${encodeURIComponent(DHAN_CLIENT_ID)}&authType=1`,
  // URL 3: plain V1
  ()=>`wss://api-feed.dhan.co`,
];
let urlIndex = 0;

function getCurrentUrl(){ return WS_URLS[urlIndex % WS_URLS.length](); }

function subscribe(ws){
  const instruments = INSTRUMENTS.map(i=>({ExchangeSegment:i.segment,SecurityId:i.secId}));
  [[15,0],[17,500],[21,1000]].forEach(([code,delay])=>{
    setTimeout(()=>{
      if(ws.readyState!==WebSocket.OPEN)return;
      ws.send(JSON.stringify({RequestCode:code,InstrumentCount:instruments.length,InstrumentList:instruments}));
      console.log('[WS] Sent RequestCode',code);
    },delay);
  });
}

function connectDhan(){
  if(!DHAN_CLIENT_ID||!DHAN_ACCESS_TOKEN){console.warn('[WS] No credentials');return;}
  if(WS.status==='connecting'||WS.status==='connected')return;
  WS.status='connecting';
  WS.lastConnectAt=new Date().toISOString();
  WS.lastDisconnectCode=null;WS.packetsReceived=0;WS.lastRawBufHex='';

  const url=getCurrentUrl();
  console.log('[WS] Connecting... urlIndex=%d',urlIndex);

  const ws=new WebSocket(url,{handshakeTimeout:15000});
  WS.ws=ws;
  let fastDisconnect=false;

  ws.on('open',()=>{
    WS.status='connected';WS.reconnectCount=0;
    console.log('[WS] Connected urlIndex=%d',urlIndex);
    subscribe(ws);
    // If urlIndex>=3 (V1 binary), send auth packet
    if(urlIndex>=2){
      const buf=Buffer.alloc(585,0);
      buf.writeUInt8(11,0);buf.writeInt16LE(585,1);
      Buffer.from(DHAN_CLIENT_ID,'utf8').copy(buf,3,0,30);
      Buffer.from(DHAN_ACCESS_TOKEN,'utf8').copy(buf,83,0,500);
      buf.write('2P',583,'utf8');
      ws.send(buf);
      console.log('[WS] Sent V1 binary auth');
    }
  });

  ws.on('message',(data)=>{
    if(typeof data==='string'){WS.lastTextMsg=data.slice(0,500);console.log('[WS] Text:',WS.lastTextMsg);return;}
    const buf=Buffer.isBuffer(data)?data:Buffer.from(data);
    WS.packetsReceived++;
    if(WS.packetsReceived<=10){
      const hex=buf.slice(0,Math.min(buf.length,32)).toString('hex');
      WS.lastRawBufHex=hex;
      console.log('[WS] Pkt#%d fc=%d len=%d hex=%s',WS.packetsReceived,buf.readUInt8(0),buf.length,hex);
    }
    const tick=parseBuf(buf);
    if(!tick)return;
    const token=TOKEN_MAP[tick.secId];
    if(!token){console.log('[WS] Unknown secId=%s',tick.secId);return;}
    const key=token.key;
    WS.lastTickAt=Date.now();
    if(tick.type==='prevClose'){liveTick[key].prevClose=tick.prevClose;console.log('[WS] PrevClose %s=%d',key,tick.prevClose);return;}
    liveTick[key]={...liveTick[key],ltp:tick.ltp||liveTick[key].ltp,bid:tick.bid||liveTick[key].bid,ask:tick.ask||liveTick[key].ask,open:tick.open||liveTick[key].open,high:tick.high||liveTick[key].high,low:(tick.low&&tick.low>0)?tick.low:liveTick[key].low,ts:WS.lastTickAt};
    if(key==='gold'||key==='silver')updateHL(key,tick.ltp,tick.high,tick.low);
  });

  ws.on('close',(code,reason)=>{
    WS.status='disconnected';WS.lastDisconnectAt=new Date().toISOString();
    const elapsed=Date.now()-new Date(WS.lastConnectAt).getTime();
    console.warn('[WS] Closed code=%s elapsed=%dms urlIndex=%d',code,elapsed,urlIndex);
    // If disconnected within 500ms, try next URL strategy
    if(elapsed<500&&WS.packetsReceived===0){
      urlIndex++;
      console.log('[WS] Fast disconnect — trying urlIndex=%d',urlIndex);
    }
    scheduleReconnect();
  });

  ws.on('error',(err)=>{ console.warn('[WS] Error:',err.message); });
}

function scheduleReconnect(){
  if(WS.reconnectTimer)return;
  WS.reconnectCount++;
  const delay=Math.min(3000*Math.pow(2,Math.min(WS.reconnectCount,4)),30000);
  console.log('[WS] Reconnect in',delay/1000,'s urlIndex=',urlIndex);
  WS.reconnectTimer=setTimeout(()=>{WS.reconnectTimer=null;connectDhan();},delay);
}

app.get('/rates',(req,res)=>{
  const marketOpen=isMCXOpen(),now=new Date().toISOString();
  if(isDhanLive()){
    const g=liveTick.gold,s=liveTick.silver,gN=liveTick.goldNext,sN=liveTick.silverNext;
    const payload={success:true,source:'dhan_mcx_live',marketOpen,tickAgeMs:Date.now()-WS.lastTickAt,goldPer10g:g.ltp,silverPerKg:s.ltp,futures:{gold:{ltp:g.ltp,bid:g.bid,ask:g.ask,high:sessionHL.gold.high||g.high,low:sessionHL.gold.low===Infinity?g.low:sessionHL.gold.low,open:g.open,prevClose:g.prevClose},silver:{ltp:s.ltp,bid:s.bid,ask:s.ask,high:sessionHL.silver.high||s.high,low:sessionHL.silver.low===Infinity?s.low:sessionHL.silver.low,open:s.open,prevClose:s.prevClose},goldNext:{ltp:gN.ltp||g.ltp,bid:gN.bid||g.bid,ask:gN.ask||g.ask},silverNext:{ltp:sN.ltp||s.ltp,bid:sN.bid||s.bid,ask:sN.ask||s.ask}},usdInr:forexCache.usdInr,xauUsd:forexCache.xauUsd,forexUpdatedAt:forexCache.updatedAt,timestamp:now};
    lastKnownRates={...payload};return res.json(payload);
  }
  if(lastKnownRates)return res.json({...lastKnownRates,source:'last_known_rates',tickAgeSeconds:tickAge()===Infinity?null:tickAge(),timestamp:now});
  const {goldPer10g,silverPerKg}=spotDerived();
  return res.json({success:true,source:'spot_derived',marketOpen,goldPer10g,silverPerKg,usdInr:forexCache.usdInr,xauUsd:forexCache.xauUsd,forexUpdatedAt:forexCache.updatedAt,timestamp:now});
});

app.get('/debug',(req,res)=>{
  res.json({
    server:'RR Jewellers Multi-URL v6',
    wsStatus:WS.status,
    currentUrlIndex:urlIndex,
    lastTickAt:WS.lastTickAt?new Date(WS.lastTickAt).toISOString():null,
    tickAgeSeconds:tickAge()===Infinity?null:tickAge(),
    packetsReceived:WS.packetsReceived,
    lastRawBufHex:WS.lastRawBufHex,
    lastTextMsg:WS.lastTextMsg,
    lastDisconnectCode:WS.lastDisconnectCode,
    reconnectCount:WS.reconnectCount,
    lastConnectAt:WS.lastConnectAt,
    lastDisconnectAt:WS.lastDisconnectAt,
    marketOpen:isMCXOpen(),
    liveTick,sessionHL,forexCache,
    credentials:{clientId:!!DHAN_CLIENT_ID,clientIdLen:DHAN_CLIENT_ID.length,accessToken:!!DHAN_ACCESS_TOKEN,accessTokenLen:DHAN_ACCESS_TOKEN.length},
  });
});
app.get('/ping',(req,res)=>res.json({ok:true,ts:Date.now()}));
app.get('/',(req,res)=>res.json({status:'RR Jewellers Multi-URL v6',endpoints:['/rates','/debug','/ping']}));

app.listen(PORT,'0.0.0.0',async()=>{
  console.log('[STARTUP] v6 port=%s tokenLen=%s',PORT,DHAN_ACCESS_TOKEN.length);
  try{await refreshForex();}catch(e){console.warn('[STARTUP]',e.message);}
  connectDhan();
  setInterval(refreshForex,5*60*1000);
  setInterval(()=>{const url=SELF_URL||`http://localhost:${PORT}`;axios.get(url+'/ping').catch(()=>{});},4*60*1000);
  setInterval(()=>{if(WS.status==='disconnected'&&!WS.reconnectTimer)connectDhan();},60*1000);
});
