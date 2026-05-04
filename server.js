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

// ─── BINARY AUTH PACKET (v1 docs exact) ──────────────────────────
// Total = 83 (header) + 500 (token) + 2 (auth type) = 585 bytes
// Header: byte[0]=FeedReqCode, byte[1-2]=MsgLen, byte[3-32]=ClientID(30), byte[33-82]=zeros(50)
// Token:  byte[83-582] = Access Token (500 bytes)
// Auth:   byte[583-584] = "2P"
function buildAuthPacket() {
  const buf = Buffer.alloc(585, 0);
  buf.writeUInt8(11, 0);
  buf.writeInt16LE(585, 1);
  const cid = Buffer.from(DHAN_CLIENT_ID, 'utf8');
  cid.copy(buf, 3, 0, Math.min(cid.length, 30));
  const tok = Buffer.from(DHAN_ACCESS_TOKEN, 'utf8');
  tok.copy(buf, 83, 0, Math.min(tok.length, 500));
  buf.write('2P', 583, 'utf8');
  return buf;
}

// ─── BINARY SUBSCRIBE PACKET (v1 docs exact) ─────────────────────
// bytes 0-82  = Header (83 bytes)
// bytes 83-86 = Instrument Count (int32 LE, 4 bytes)
// bytes 87+   = Instruments (21 bytes each)
//   byte[0]   = Exchange Segment (int8)
//   byte[1-20] = Security ID string (20 bytes, zero-padded)
const EXCH_ENUM = { MCX_COMM: 7, NSE_EQ: 1, NSE_FNO: 2, BSE_EQ: 3 };

function buildSubscribePacket(requestCode, instruments) {
  const HEADER_SIZE = 83;
  const COUNT_SIZE  = 4;
  const INSTR_SIZE  = 21;
  const total = HEADER_SIZE + COUNT_SIZE + instruments.length * INSTR_SIZE;
  const buf   = Buffer.alloc(total, 0);

  // Header
  buf.writeUInt8(requestCode, 0);
  buf.writeInt16LE(total, 1);
  const cid = Buffer.from(DHAN_CLIENT_ID, 'utf8');
  cid.copy(buf, 3, 0, Math.min(cid.length, 30));

  // Count at offset 83
  buf.writeInt32LE(instruments.length, HEADER_SIZE);

  // Instruments starting at offset 87
  instruments.forEach((instr, i) => {
    const base = HEADER_SIZE + COUNT_SIZE + i * INSTR_SIZE;
    buf.writeUInt8(EXCH_ENUM[instr.segment] || 7, base);
    const sec = Buffer.from(String(instr.secId), 'utf8');
    sec.copy(buf, base + 1, 0, Math.min(sec.length, 20));
  });

  return buf;
}

// Request codes for subscribe (v1 docs annexure)
const REQ = { TICKER: 21, QUOTE: 22, MKTDEPTH: 23 };

// ─── INSTRUMENTS ──────────────────────────────────────────────────
const INSTRUMENTS = [
  { secId: '436177', segment: 'MCX_COMM', key: 'gold',       name: 'GOLD-JUN2026' },
  { secId: '436178', segment: 'MCX_COMM', key: 'goldNext',   name: 'GOLD-AUG2026' },
  { secId: '436197', segment: 'MCX_COMM', key: 'silver',     name: 'SILVER-JUL2026' },
  { secId: '436198', segment: 'MCX_COMM', key: 'silverNext', name: 'SILVER-SEP2026' },
];
const TOKEN_MAP = {};
INSTRUMENTS.forEach(i => { TOKEN_MAP[i.secId] = i; });

// ─── LIVE STATE ───────────────────────────────────────────────────
const emptyTick = () => ({ ltp:0, bid:0, ask:0, high:0, low:0, open:0, prevClose:0, ts:0 });
const liveTick  = { gold:emptyTick(), goldNext:emptyTick(), silver:emptyTick(), silverNext:emptyTick() };
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

// ─── FOREX ────────────────────────────────────────────────────────
const forexCache = { usdInr:94.5, xauUsd:3310, xagUsd:32.8, updatedAt:null, src:'init' };

async function refreshForex() {
  const fxSrc = [
    ['frankfurter', async () => (await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000})).data.rates.INR],
    ['open.er-api', async () => (await axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000})).data.rates.INR],
  ];
  let usdInr=0, src='';
  for (const [name,fn] of fxSrc) {
    if(usdInr) break;
    try { const v=await fn(); if(v>70&&v<110){usdInr=v;src=name;} } catch(e){console.warn('[FOREX]',name,e.message);}
  }
  let xauUsd=0, xagUsd=0;
  try {
    const r=await axios.get('https://api.metals.live/v1/spot/gold,silver',{timeout:6000});
    if(Array.isArray(r.data)){
      const g=r.data.find(x=>x.gold), s=r.data.find(x=>x.silver);
      if(g&&g.gold>3000) xauUsd=g.gold;
      if(s&&s.silver>20) xagUsd=s.silver;
    }
  } catch(e){console.warn('[SPOT]',e.message);}
  if(!xauUsd){
    try {
      const [g,s]=await Promise.all([
        axios.get('https://www.gold-api.com/price/XAU',{timeout:7000}),
        axios.get('https://www.gold-api.com/price/XAG',{timeout:7000}),
      ]);
      if(g.data.price>3000) xauUsd=g.data.price;
      if(s.data.price>20)   xagUsd=s.data.price;
    } catch(e){console.warn('[SPOT2]',e.message);}
  }
  forexCache.usdInr    = usdInr    || forexCache.usdInr;
  forexCache.xauUsd    = xauUsd    || forexCache.xauUsd;
  forexCache.xagUsd    = xagUsd    || forexCache.xagUsd;
  forexCache.updatedAt = new Date().toISOString();
  forexCache.src       = src;
  console.log('[FOREX] usdInr=%s xauUsd=%s src=%s', forexCache.usdInr, forexCache.xauUsd, src);
}

function spotDerived() {
  const {usdInr,xauUsd,xagUsd}=forexCache, F=1.103;
  return {
    goldPer10g:  Math.round((xauUsd/31.1035)*10*usdInr*F),
    silverPerKg: Math.round((xagUsd/31.1035)*1000*usdInr*F),
  };
}

// ─── HELPERS ──────────────────────────────────────────────────────
function isMCXOpen() {
  const d=new Date(Date.now()+5.5*3600000), dow=d.getUTCDay(), t=d.getUTCHours()*60+d.getUTCMinutes();
  if(dow===0) return false;
  return dow===6 ? (t>=540&&t<840) : (t>=540&&t<1435);
}
function tickAge(){ return WS.lastTickAt ? Math.floor((Date.now()-WS.lastTickAt)/1000) : Infinity; }
function isDhanLive(){ return WS.status==='connected'&&tickAge()<15&&liveTick.gold.ltp>0; }
function updateHL(sym,ltp,high,low){
  if(ltp>0){if(ltp>sessionHL[sym].high)sessionHL[sym].high=ltp;if(ltp<sessionHL[sym].low)sessionHL[sym].low=ltp;}
  if(high>0&&high>sessionHL[sym].high)sessionHL[sym].high=high;
  if(low>0&&low<sessionHL[sym].low)sessionHL[sym].low=low;
}

// ─── BINARY PARSER (docs 1-indexed → 0-indexed: subtract 1) ──────
// Ticker   LTP: docs byte 9-12  → offset 8
// PrevClose:    docs byte 9-12  → offset 8 (int32)
// Quote    LTP: docs byte 9-12  → offset 8
// Quote   Open: docs byte 35-38 → offset 34
// Quote   High: docs byte 43-46 → offset 42
// Quote    Low: docs byte 47-50 → offset 46
function parseBuf(buf) {
  try {
    if(!buf||buf.length<8) return null;
    const fc    = buf.readUInt8(0);
    const secId = buf.readInt32LE(4).toString();

    if(fc===50){
      WS.lastDisconnectCode = buf.length>=10 ? buf.readInt16LE(8) : 0;
      console.warn('[WS] Disconnect code=',WS.lastDisconnectCode);
      return null;
    }
    if(fc===6&&buf.length>=16){
      return { type:'prevClose', secId, prevClose: buf.readInt32LE(8) };
    }
    if(fc===2&&buf.length>=16){
      const ltp=buf.readFloatLE(8);
      if(!Number.isFinite(ltp)||ltp<=0) return null;
      return { type:'ticker', secId, ltp:Math.round(ltp), ltt:buf.readUInt32LE(12) };
    }
    if(fc===4&&buf.length>=50){
      const ltp=buf.readFloatLE(8);
      if(!Number.isFinite(ltp)||ltp<=0) return null;
      return {
        type:'quote', secId,
        ltp:Math.round(ltp), bid:Math.round(ltp), ask:Math.round(ltp),
        open:Math.round(buf.readFloatLE(34)),
        high:Math.round(buf.readFloatLE(42)),
        low: Math.round(buf.readFloatLE(46)),
        ltt: buf.readUInt32LE(14),
      };
    }
    if(fc===3&&buf.length>=112){
      const ltp=buf.readFloatLE(8);
      if(!Number.isFinite(ltp)||ltp<=0) return null;
      const bid=buf.readFloatLE(12+12), ask=buf.readFloatLE(12+16);
      return {
        type:'depth', secId,
        ltp:Math.round(ltp),
        bid:bid>0?Math.round(bid):Math.round(ltp),
        ask:ask>0?Math.round(ask):Math.round(ltp),
      };
    }
    if(WS.packetsReceived<=10){
      console.log('[PARSE] unknown fc=%d len=%d hex=%s', fc, buf.length,
        buf.slice(0,Math.min(buf.length,16)).toString('hex'));
    }
    return null;
  } catch(e){ console.warn('[PARSE]',e.message); return null; }
}

// ─── WEBSOCKET ────────────────────────────────────────────────────
function connectDhan() {
  if(!DHAN_CLIENT_ID||!DHAN_ACCESS_TOKEN){ console.warn('[WS] No credentials'); return; }
  if(WS.status==='connecting'||WS.status==='connected') return;

  WS.status='connecting';
  WS.lastConnectAt=new Date().toISOString();
  WS.lastDisconnectCode=null; WS.packetsReceived=0; WS.lastRawBufHex='';
  console.log('[WS] Connecting V1 (token len=%d)', DHAN_ACCESS_TOKEN.length);

  const ws=new WebSocket('wss://api-feed.dhan.co', { handshakeTimeout:15000 });
  WS.ws=ws;

  ws.on('open',()=>{
    WS.status='connected'; WS.reconnectCount=0;
    console.log('[WS] Open — sending auth');
    const auth=buildAuthPacket();
    console.log('[WS] Auth packet built: %d bytes, tokenLen=%d', auth.length, DHAN_ACCESS_TOKEN.length);
    ws.send(auth);

    // Subscribe after 1.5s to let auth complete
    setTimeout(()=>{
      if(ws.readyState!==WebSocket.OPEN) return;
      const tkr=buildSubscribePacket(REQ.TICKER, INSTRUMENTS);
      ws.send(tkr);
      console.log('[WS] Subscribe TICKER (%d bytes)', tkr.length);
    }, 1500);

    setTimeout(()=>{
      if(ws.readyState!==WebSocket.OPEN) return;
      const q=buildSubscribePacket(REQ.QUOTE, INSTRUMENTS);
      ws.send(q);
      console.log('[WS] Subscribe QUOTE (%d bytes)', q.length);
    }, 2500);
  });

  ws.on('message',(data)=>{
    if(typeof data==='string'){
      WS.lastTextMsg=data.slice(0,500);
      console.log('[WS] Text:',WS.lastTextMsg); return;
    }
    const buf=Buffer.isBuffer(data)?data:Buffer.from(data);
    WS.packetsReceived++;
    if(WS.packetsReceived<=10){
      const hex=buf.slice(0,Math.min(buf.length,32)).toString('hex');
      WS.lastRawBufHex=hex;
      console.log('[WS] Pkt#%d fc=%d len=%d hex=%s', WS.packetsReceived, buf.readUInt8(0), buf.length, hex);
    }
    const tick=parseBuf(buf);
    if(!tick) return;
    const token=TOKEN_MAP[tick.secId];
    if(!token){ console.log('[WS] Unknown secId=%s',tick.secId); return; }
    const key=token.key;
    WS.lastTickAt=Date.now();
    if(tick.type==='prevClose'){
      liveTick[key].prevClose=tick.prevClose;
      console.log('[WS] PrevClose %s=%d',key,tick.prevClose); return;
    }
    liveTick[key]={
      ...liveTick[key],
      ltp: tick.ltp||liveTick[key].ltp,
      bid: tick.bid||liveTick[key].bid,
      ask: tick.ask||liveTick[key].ask,
      open:tick.open||liveTick[key].open,
      high:tick.high||liveTick[key].high,
      low: (tick.low&&tick.low>0)?tick.low:liveTick[key].low,
      ts:  WS.lastTickAt,
    };
    if(key==='gold'||key==='silver') updateHL(key,tick.ltp,tick.high,tick.low);
  });

  ws.on('close',(code,reason)=>{
    WS.status='disconnected'; WS.lastDisconnectAt=new Date().toISOString();
    console.warn('[WS] Closed code=%s reason=%s',code,reason&&reason.toString?reason.toString():'');
    scheduleReconnect();
  });

  ws.on('error',(err)=>{ console.warn('[WS] Error:',err.message); });
}

function scheduleReconnect(){
  if(WS.reconnectTimer) return;
  WS.reconnectCount++;
  const delay=Math.min(2000*Math.pow(2,Math.min(WS.reconnectCount,5)),60000);
  console.log('[WS] Reconnect in',delay/1000,'s');
  WS.reconnectTimer=setTimeout(()=>{ WS.reconnectTimer=null; connectDhan(); }, delay);
}

// ─── ROUTES ───────────────────────────────────────────────────────
app.get('/rates',(req,res)=>{
  const marketOpen=isMCXOpen(), now=new Date().toISOString();
  if(isDhanLive()){
    const g=liveTick.gold, s=liveTick.silver, gN=liveTick.goldNext, sN=liveTick.silverNext;
    const payload={
      success:true, source:'dhan_mcx_live', marketOpen,
      tickAgeMs:Date.now()-WS.lastTickAt,
      goldPer10g:g.ltp, silverPerKg:s.ltp,
      futures:{
        gold:{ltp:g.ltp,bid:g.bid,ask:g.ask,high:sessionHL.gold.high||g.high,low:sessionHL.gold.low===Infinity?g.low:sessionHL.gold.low,open:g.open,prevClose:g.prevClose},
        silver:{ltp:s.ltp,bid:s.bid,ask:s.ask,high:sessionHL.silver.high||s.high,low:sessionHL.silver.low===Infinity?s.low:sessionHL.silver.low,open:s.open,prevClose:s.prevClose},
        goldNext:{ltp:gN.ltp||g.ltp,bid:gN.bid||g.bid,ask:gN.ask||g.ask},
        silverNext:{ltp:sN.ltp||s.ltp,bid:sN.bid||s.bid,ask:sN.ask||s.ask},
      },
      usdInr:forexCache.usdInr, xauUsd:forexCache.xauUsd,
      forexUpdatedAt:forexCache.updatedAt, timestamp:now,
    };
    lastKnownRates={...payload};
    return res.json(payload);
  }
  if(lastKnownRates) return res.json({...lastKnownRates,source:'last_known_rates',tickAgeSeconds:tickAge()===Infinity?null:tickAge(),timestamp:now});
  const {goldPer10g,silverPerKg}=spotDerived();
  return res.json({success:true,source:'spot_derived',marketOpen,goldPer10g,silverPerKg,usdInr:forexCache.usdInr,xauUsd:forexCache.xauUsd,forexUpdatedAt:forexCache.updatedAt,timestamp:now});
});

app.get('/debug',(req,res)=>{
  res.json({
    server:'RR Jewellers V1-Auth v5',
    protocol:'DhanHQ V1 binary — exact byte offsets',
    wsStatus:WS.status,
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
    liveTick, sessionHL, forexCache,
    credentials:{
      clientId:!!DHAN_CLIENT_ID, clientIdLen:DHAN_CLIENT_ID.length,
      accessToken:!!DHAN_ACCESS_TOKEN, accessTokenLen:DHAN_ACCESS_TOKEN.length,
    },
  });
});
app.get('/ping',(req,res)=>res.json({ok:true,ts:Date.now()}));
app.get('/',(req,res)=>res.json({status:'RR Jewellers V1-Auth v5',endpoints:['/rates','/debug','/ping']}));

// ─── STARTUP ──────────────────────────────────────────────────────
app.listen(PORT,'0.0.0.0',async()=>{
  console.log('[STARTUP] RR Jewellers V1-Auth v5 port=%s tokenLen=%s clientIdLen=%s',
    PORT, DHAN_ACCESS_TOKEN.length, DHAN_CLIENT_ID.length);
  try { await refreshForex(); } catch(e){ console.warn('[STARTUP]',e.message); }
  connectDhan();
  setInterval(refreshForex, 5*60*1000);
  setInterval(()=>{ const url=SELF_URL||`http://localhost:${PORT}`; axios.get(url+'/ping').catch(()=>{}); }, 4*60*1000);
  setInterval(()=>{ if(WS.status==='disconnected'&&!WS.reconnectTimer) connectDhan(); }, 60*1000);
});
