'use strict';
// RR Jewellers v10 — Production Final
// MCX: Dhan WebSocket (live ticks ~95ms)
// SPOT: Twelve Data WebSocket (XAU/USD, XAG/USD ~170ms)
// USD/INR: Free REST APIs cached 5min
// Margin: GOLD_MARGIN_PCT + SILVER_MARGIN_PCT env vars

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

// ─── ENV VARS ──────────────────────────────────────────
const PORT               = process.env.PORT               || 3000;
const SELF_URL           = process.env.SELF_URL           || '';
const SHEET_ID           = process.env.SHEET_ID           || '';
const DHAN_CLIENT_ID     = process.env.DHAN_CLIENT_ID     || '';
const DHAN_ACCESS_TOKEN  = process.env.DHAN_ACCESS_TOKEN  || '';
const TWELVE_DATA_KEY    = process.env.TWELVE_DATA_KEY    || ''; // twelvedata.com free key
const DHAN_BASE          = 'https://api.dhan.co/v2';

// ─── MCX CONTRACTS ─────────────────────────────────────
const GOLD_CONTRACTS = [
  { secId:'459277', display:'GOLD JUN26',  expiry:'2026-06-05' },
  { secId:'466583', display:'GOLD AUG26',  expiry:'2026-08-05' },
  { secId:'483079', display:'GOLD OCT26',  expiry:'2026-10-05' },
  { secId:'495213', display:'GOLD DEC26',  expiry:'2026-12-04' },
  { secId:'559933', display:'GOLD FEB27',  expiry:'2027-02-05' },
];
const SILVER_CONTRACTS = [
  { secId:'464150', display:'SILVER JUL26', expiry:'2026-07-03' },
  { secId:'471725', display:'SILVER SEP26', expiry:'2026-09-04' },
  { secId:'495214', display:'SILVER DEC26', expiry:'2026-12-04' },
  { secId:'564619', display:'SILVER MAR27', expiry:'2027-03-05' },
];

function pickCurrentAndNext(contracts) {
  var now = new Date();
  var sorted = contracts
    .map(function(c){ return Object.assign({},c,{ed:new Date(c.expiry)}); })
    .filter(function(c){ return !isNaN(c.ed); })
    .sort(function(a,b){ return a.ed-b.ed; });
  var up = sorted.filter(function(c){ return c.ed>=now; });
  if (up.length>=2) return {current:up[0],next:up[1]};
  if (up.length===1) return {current:sorted[sorted.length-2]||up[0],next:up[0]};
  var last=sorted.slice(-2);
  return {current:last[0]||sorted[0],next:last[1]||sorted[0]};
}
function getAC(){ return {gold:pickCurrentAndNext(GOLD_CONTRACTS),silver:pickCurrentAndNext(SILVER_CONTRACTS)}; }

// ─── MCX RATE CACHE ────────────────────────────────────
var RC = {
  goldLtp:0,goldOpen:0,goldHigh:0,goldLow:0,goldPrevClose:0,goldBid:0,goldAsk:0,
  goldNextLtp:0,goldNextBid:0,goldNextAsk:0,goldNextHigh:0,goldNextLow:0,
  silverLtp:0,silverOpen:0,silverHigh:0,silverLow:0,silverPrevClose:0,silverBid:0,silverAsk:0,
  silverNextLtp:0,silverNextBid:0,silverNextAsk:0,silverNextHigh:0,silverNextLow:0,
  source:'init',updatedAt:null,
};

function applyTick(key,tick){
  if(!tick||tick.ltp<=0) return;
  var l=tick.ltp;
  if(key==='gold'){
    RC.goldLtp=l;
    if(tick.high>0) RC.goldHigh=tick.high;
    if(tick.low>0)  RC.goldLow=tick.low;
    if(tick.open>0) RC.goldOpen=tick.open;
    if(tick.prevClose>0) RC.goldPrevClose=tick.prevClose;
    RC.goldBid=tick.bid>0?tick.bid:l-30;
    RC.goldAsk=tick.ask>0?tick.ask:l+30;
  } else if(key==='goldNext'){
    RC.goldNextLtp=l;
    RC.goldNextBid=tick.bid>0?tick.bid:l-50;
    RC.goldNextAsk=tick.ask>0?tick.ask:l+50;
    if(tick.high>0) RC.goldNextHigh=tick.high;
    if(tick.low>0)  RC.goldNextLow=tick.low;
  } else if(key==='silver'){
    RC.silverLtp=l;
    if(tick.high>0) RC.silverHigh=tick.high;
    if(tick.low>0)  RC.silverLow=tick.low;
    if(tick.open>0) RC.silverOpen=tick.open;
    if(tick.prevClose>0) RC.silverPrevClose=tick.prevClose;
    RC.silverBid=tick.bid>0?tick.bid:l-100;
    RC.silverAsk=tick.ask>0?tick.ask:l+100;
  } else if(key==='silverNext'){
    RC.silverNextLtp=l;
    RC.silverNextBid=tick.bid>0?tick.bid:l-200;
    RC.silverNextAsk=tick.ask>0?tick.ask:l+200;
    if(tick.high>0) RC.silverNextHigh=tick.high;
    if(tick.low>0)  RC.silverNextLow=tick.low;
  }
  RC.updatedAt=new Date().toISOString();
}

// ─── FOREX CACHE ───────────────────────────────────────
// XAU/USD + XAG/USD: Twelve Data WebSocket (live ~170ms)
// USD/INR: Free REST APIs cached 5min
var FX = {
  usdInr:94.5, xauUsd:0, xagUsd:0,
  xauBid:0, xauAsk:0, xauHigh:0, xauLow:0,
  xagBid:0, xagAsk:0, xagHigh:0, xagLow:0,
  usdInrHigh:0, usdInrLow:0,
  updatedAt:null, src:'init',
  xauUpdatedAt:null, xagUpdatedAt:null,
};

// ─── MCX OPEN CHECK ────────────────────────────────────
function isMCXOpen(){
  var d=new Date(Date.now()+5.5*3600000);
  var dow=d.getUTCDay(), t=d.getUTCHours()*60+d.getUTCMinutes();
  if(dow===0) return false;
  return dow===6?(t>=540&&t<840):(t>=540&&t<1435);
}

// ─── USD/INR REFRESH (every 5 min) ─────────────────────
async function refreshUsdInr(){
  var fxList=[
    ['frankfurter', function(){ return axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000}).then(function(r){return r.data.rates.INR;}); }],
    ['open.er-api',  function(){ return axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000}).then(function(r){return r.data.rates.INR;}); }],
    ['fawazahmed0',  function(){ return axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json',{timeout:5000}).then(function(r){return r.data.usd.inr;}); }],
  ];
  var v=0, src='';
  for(var i=0;i<fxList.length&&!v;i++){
    try{ var r=await fxList[i][1](); if(r>70&&r<110){v=r;src=fxList[i][0];} }catch(e){}
  }
  if(!v){v=FX.usdInr;src='cached';}
  FX.usdInr=Math.round(v*100)/100;
  FX.updatedAt=new Date().toISOString();
  FX.src=src;
  console.log('[FOREX] usdInr=%s src=%s',FX.usdInr,FX.src);
}

// ─── TWELVE DATA WEBSOCKET (XAU/USD + XAG/USD) ─────────
var TDws = {
  ws:null, status:'disconnected',
  reconnectTimer:null, reconnectCount:0,
  lastConnectAt:null, packetsReceived:0,
};

function connectTwelveData(){
  if(!TWELVE_DATA_KEY){
    console.warn('[TD] No TWELVE_DATA_KEY — using REST fallback for spot');
    refreshSpotREST();
    return;
  }
  if(TDws.status==='connecting'||TDws.status==='connected') return;
  TDws.status='connecting';
  TDws.lastConnectAt=new Date().toISOString();
  console.log('[TD] Connecting to Twelve Data WebSocket...');

  var ws=new WebSocket('wss://ws.twelvedata.com/v1/quotes/price?apikey='+TWELVE_DATA_KEY,{handshakeTimeout:15000});
  TDws.ws=ws;

  ws.on('open',function(){
    TDws.status='connected'; TDws.reconnectCount=0;
    console.log('[TD] Connected');
    ws.send(JSON.stringify({
      action: 'subscribe',
      params: { symbols: 'XAU/USD,XAG/USD' }
    }));
  });

  ws.on('message',function(data){
    try{
      var msg=JSON.parse(data);
      if(msg.event==='subscribe-status') { console.log('[TD]',msg.status,msg.message||''); return; }
      if(msg.event==='heartbeat') return;
      if(msg.event==='price'&&msg.symbol&&msg.price){
        TDws.packetsReceived++;
        var p=parseFloat(msg.price);
        var b=parseFloat(msg.bid||msg.price);
        var a=parseFloat(msg.ask||msg.price);
        if(msg.symbol==='XAU/USD'&&p>3000&&p<9000){
          FX.xauUsd=Math.round(p*100)/100;
          FX.xauBid=Math.round(b*100)/100;
          FX.xauAsk=Math.round(a*100)/100;
          if(!FX.xauHigh||p>FX.xauHigh) FX.xauHigh=Math.round(p*100)/100;
          if(!FX.xauLow||p<FX.xauLow)   FX.xauLow=Math.round(p*100)/100;
          FX.xauUpdatedAt=new Date().toISOString();
        }
        if(msg.symbol==='XAG/USD'&&p>20&&p<300){
          FX.xagUsd=Math.round(p*1000)/1000;
          FX.xagBid=Math.round(b*1000)/1000;
          FX.xagAsk=Math.round(a*1000)/1000;
          if(!FX.xagHigh||p>FX.xagHigh) FX.xagHigh=Math.round(p*1000)/1000;
          if(!FX.xagLow||p<FX.xagLow)   FX.xagLow=Math.round(p*1000)/1000;
          FX.xagUpdatedAt=new Date().toISOString();
        }
      }
    }catch(e){}
  });

  ws.on('close',function(code){
    TDws.status='disconnected';
    console.warn('[TD] Closed code=%s',code);
    TDws.reconnectCount++;
    var d=Math.min(3000*Math.pow(2,Math.min(TDws.reconnectCount,4)),30000);
    TDws.reconnectTimer=setTimeout(function(){TDws.reconnectTimer=null;connectTwelveData();},d);
  });

  ws.on('error',function(e){ console.warn('[TD] Error:',e.message); });
}

// REST fallback when no Twelve Data key
async function refreshSpotREST(){
  try{
    var r=await axios.get('https://api.metals.live/v1/spot/gold,silver',{timeout:6000});
    if(Array.isArray(r.data)){
      var g=r.data.find(function(x){return x.gold;}),s=r.data.find(function(x){return x.silver;});
      if(g&&g.gold>3000){FX.xauUsd=Math.round(g.gold*100)/100;}
      if(s&&s.silver>20){FX.xagUsd=Math.round(s.silver*1000)/1000;}
      FX.xauUpdatedAt=new Date().toISOString();
      FX.xagUpdatedAt=new Date().toISOString();
      return;
    }
  }catch(e){}
  try{
    var res=await Promise.all([
      axios.get('https://www.gold-api.com/price/XAU',{timeout:7000}),
      axios.get('https://www.gold-api.com/price/XAG',{timeout:7000}),
    ]);
    if(res[0].data.price>3000) FX.xauUsd=Math.round(res[0].data.price*100)/100;
    if(res[1].data.price>20)   FX.xagUsd=Math.round(res[1].data.price*1000)/1000;
    FX.xauUpdatedAt=new Date().toISOString();
    FX.xagUpdatedAt=new Date().toISOString();
  }catch(e){}
}

function spotDerived(){
  var F=1.103;
  return {
    goldPer10g:  Math.round((FX.xauUsd/31.1035)*10*FX.usdInr*F),
    silverPerKg: Math.round((FX.xagUsd/31.1035)*1000*FX.usdInr*F),
  };
}

// ─── TOKEN AUTO-RENEW ──────────────────────────────────
var currentToken=DHAN_ACCESS_TOKEN, tokenRenewedAt=null;

async function renewToken(){
  if(!currentToken||!DHAN_CLIENT_ID) return false;
  try{
    var r=await axios.post(DHAN_BASE+'/RenewToken',{},{
      headers:{'access-token':currentToken,'dhanClientId':DHAN_CLIENT_ID,'Content-Type':'application/json'},
      timeout:12000,
    });
    var t=r.data?.accessToken||r.data?.access_token||r.data?.data?.accessToken;
    if(t){
      currentToken=t; tokenRenewedAt=new Date().toISOString();
      console.log('[TOKEN] Renewed at',tokenRenewedAt);
      if(WS.ws){try{WS.ws.terminate();}catch(e){}}
      WS.status='disconnected';
      setTimeout(connectDhan,3000);
      return true;
    }
    return false;
  }catch(e){console.warn('[TOKEN] Renew failed:',e.message.slice(0,60));return false;}
}

// ─── DHAN WEBSOCKET ────────────────────────────────────
var WS={
  ws:null,status:'disconnected',reconnectTimer:null,reconnectCount:0,
  lastConnectAt:null,lastDisconnectAt:null,lastDisconnectCode:null,
  lastTickAt:null,packetsReceived:0,lastRawHex:'',lastTextMsg:'',
};
var TOKEN_MAP={};

function buildTokenMap(){
  var ac=getAC(); TOKEN_MAP={};
  TOKEN_MAP[ac.gold.current.secId]='gold';
  TOKEN_MAP[ac.gold.next.secId]='goldNext';
  TOKEN_MAP[ac.silver.current.secId]='silver';
  TOKEN_MAP[ac.silver.next.secId]='silverNext';
  console.log('[TOKENMAP]',JSON.stringify(TOKEN_MAP));
}

function subscribeWS(ws){
  var ac=getAC();
  var instruments=[
    {ExchangeSegment:'MCX_COMM',SecurityId:ac.gold.current.secId},
    {ExchangeSegment:'MCX_COMM',SecurityId:ac.gold.next.secId},
    {ExchangeSegment:'MCX_COMM',SecurityId:ac.silver.current.secId},
    {ExchangeSegment:'MCX_COMM',SecurityId:ac.silver.next.secId},
  ];
  [[15,0],[17,600],[21,1200]].forEach(function(p){
    setTimeout(function(){
      if(ws.readyState!==WebSocket.OPEN) return;
      ws.send(JSON.stringify({RequestCode:p[0],InstrumentCount:instruments.length,InstrumentList:instruments}));
      console.log('[WS] Subscribe code=%d',p[0]);
    },p[1]);
  });
}

function parseBuf(buf){
  try{
    if(!buf||buf.length<8) return null;
    var fc=buf.readUInt8(0), secId=buf.readInt32LE(4).toString();
    if(fc===50){WS.lastDisconnectCode=buf.length>=10?buf.readInt16LE(8):0;return null;}
    if(fc===6&&buf.length>=16){var pc=buf.readFloatLE(8);return isFinite(pc)&&pc>0?{type:'prevClose',secId,prevClose:Math.round(pc)}:null;}
    if(fc===2&&buf.length>=16){var l2=buf.readFloatLE(8);return !isFinite(l2)||l2<=100?null:{type:'ticker',secId,ltp:Math.round(l2)};}
    if(fc===4&&buf.length>=50){
      var l4=buf.readFloatLE(8); if(!isFinite(l4)||l4<=100) return null;
      return{type:'quote',secId,ltp:Math.round(l4),open:Math.round(buf.readFloatLE(34))||0,high:Math.round(buf.readFloatLE(42))||0,low:Math.round(buf.readFloatLE(46))||0};
    }
    if(fc===8&&buf.length>=62){
      var l8=buf.readFloatLE(8); if(!isFinite(l8)||l8<=100) return null;
      var o8=buf.length>49?Math.round(buf.readFloatLE(46)):0;
      var h8=buf.length>57?Math.round(buf.readFloatLE(54)):0;
      var lw8=buf.length>61?Math.round(buf.readFloatLE(58)):0;
      var b8=Math.round(l8),a8=Math.round(l8);
      if(buf.length>=82){var bf=buf.readFloatLE(74),af=buf.readFloatLE(78);if(isFinite(bf)&&bf>100)b8=Math.round(bf);if(isFinite(af)&&af>100)a8=Math.round(af);}
      return{type:'full',secId,ltp:Math.round(l8),bid:b8,ask:a8,open:o8,high:h8,low:lw8};
    }
    return null;
  }catch(e){return null;}
}

function scheduleReconnect(){
  if(WS.reconnectTimer) return;
  WS.reconnectCount++;
  var d=Math.min(3000*Math.pow(2,Math.min(WS.reconnectCount,4)),30000);
  WS.reconnectTimer=setTimeout(function(){WS.reconnectTimer=null;connectDhan();},d);
  console.log('[WS] Reconnect in %ds',d/1000);
}

function connectDhan(){
  if(!DHAN_CLIENT_ID||!currentToken){console.warn('[WS] No credentials');return;}
  if(WS.status==='connecting'||WS.status==='connected') return;
  WS.status='connecting'; WS.lastConnectAt=new Date().toISOString();
  WS.packetsReceived=0; WS.lastRawHex='';
  buildTokenMap();
  var wsUrl='wss://api-feed.dhan.co?version=2&token='+encodeURIComponent(currentToken)+'&clientId='+encodeURIComponent(DHAN_CLIENT_ID)+'&authType=2';
  var ws=new WebSocket(wsUrl,{handshakeTimeout:15000}); WS.ws=ws;

  ws.on('open',function(){WS.status='connected';WS.reconnectCount=0;console.log('[WS] Connected');subscribeWS(ws);});

  ws.on('message',function(data){
    if(typeof data==='string'){WS.lastTextMsg=data.slice(0,200);return;}
    var buf=Buffer.isBuffer(data)?data:Buffer.from(data);
    WS.packetsReceived++;
    if(WS.packetsReceived<=3) WS.lastRawHex=buf.slice(0,32).toString('hex');
    var tick=parseBuf(buf); if(!tick) return;
    WS.lastTickAt=Date.now();
    if(tick.type==='prevClose'){
      if(TOKEN_MAP[tick.secId]==='gold')   RC.goldPrevClose=tick.prevClose;
      if(TOKEN_MAP[tick.secId]==='silver') RC.silverPrevClose=tick.prevClose;
      return;
    }
    var key=TOKEN_MAP[tick.secId]; if(!key) return;
    applyTick(key,tick); RC.source='dhan_ws_live';
  });

  ws.on('close',function(code){
    WS.status='disconnected'; WS.lastDisconnectAt=new Date().toISOString();
    console.warn('[WS] Closed code=%d packets=%d',code,WS.packetsReceived);
    scheduleReconnect();
  });
  ws.on('error',function(e){console.warn('[WS] Error:',e.message);});
}

// ─── OHLC REST BACKUP ──────────────────────────────────
var lastOhlcError=null,ohlcCallCount=0;

function pollOhlc(){
  if(!DHAN_CLIENT_ID||!currentToken) return;
  var ac=getAC();
  var secIds=[parseInt(ac.gold.current.secId,10),parseInt(ac.gold.next.secId,10),parseInt(ac.silver.current.secId,10),parseInt(ac.silver.next.secId,10)];
  axios.post(DHAN_BASE+'/marketfeed/ohlc',{MCX_COMM:secIds},{
    headers:{'Accept':'application/json','Content-Type':'application/json','access-token':currentToken,'client-id':DHAN_CLIENT_ID},
    timeout:5000,
  }).then(function(resp){
    var seg=resp.data&&resp.data.data&&resp.data.data['MCX_COMM'];
    if(!seg){lastOhlcError='No MCX_COMM';return;}
    ohlcCallCount++; lastOhlcError=null;
    var wsLive=WS.status==='connected'&&WS.lastTickAt&&Date.now()-WS.lastTickAt<5000;
    if(wsLive) return;
    function applyRow(secId,key){
      var row=seg[String(secId)];if(!row)return;
      var ltp=row.last_price||0,ohlc=row.ohlc||{};
      if(ltp>0){applyTick(key,{ltp:Math.round(ltp),open:ohlc.open?Math.round(ohlc.open):0,high:ohlc.high?Math.round(ohlc.high):0,low:ohlc.low?Math.round(ohlc.low):0});RC.source='dhan_ohlc_rest';}
    }
    applyRow(ac.gold.current.secId,'gold');applyRow(ac.gold.next.secId,'goldNext');
    applyRow(ac.silver.current.secId,'silver');applyRow(ac.silver.next.secId,'silverNext');
  }).catch(function(e){lastOhlcError=e.message;});
}

// ─── ROUTES ────────────────────────────────────────────
app.get('/rates',function(req,res){
  var ac=getAC(),spot=spotDerived(),now=new Date().toISOString();
  var hasLive=RC.goldLtp>0;
  res.json({
    success:true, source:hasLive?RC.source:'spot_derived', marketOpen:isMCXOpen(),
    goldPer10g:  hasLive?RC.goldLtp:spot.goldPer10g,
    silverPerKg: hasLive?RC.silverLtp:spot.silverPerKg,
    futures:{
      gold:     {ltp:RC.goldLtp,    bid:RC.goldBid,    ask:RC.goldAsk,    high:RC.goldHigh,    low:RC.goldLow,    open:RC.goldOpen,    prevClose:RC.goldPrevClose,   contract:ac.gold.current.display,  expiry:ac.gold.current.expiry},
      goldNext: {ltp:RC.goldNextLtp,bid:RC.goldNextBid,ask:RC.goldNextAsk,high:RC.goldNextHigh,low:RC.goldNextLow,contract:ac.gold.next.display,expiry:ac.gold.next.expiry},
      silver:   {ltp:RC.silverLtp,  bid:RC.silverBid,  ask:RC.silverAsk,  high:RC.silverHigh,  low:RC.silverLow,  open:RC.silverOpen,  prevClose:RC.silverPrevClose, contract:ac.silver.current.display,expiry:ac.silver.current.expiry},
      silverNext:{ltp:RC.silverNextLtp,bid:RC.silverNextBid,ask:RC.silverNextAsk,high:RC.silverNextHigh,low:RC.silverNextLow,contract:ac.silver.next.display,expiry:ac.silver.next.expiry},
    },
    spot:{
      xauUsd:FX.xauUsd, xauBid:FX.xauBid, xauAsk:FX.xauAsk, xauHigh:FX.xauHigh, xauLow:FX.xauLow,
      xagUsd:FX.xagUsd, xagBid:FX.xagBid, xagAsk:FX.xagAsk, xagHigh:FX.xagHigh, xagLow:FX.xagLow,
      usdInr:FX.usdInr, usdInrHigh:FX.usdInrHigh, usdInrLow:FX.usdInrLow,
    },
    // Legacy fields for HTML compatibility
    xauUsd:FX.xauUsd, xagUsd:FX.xagUsd, usdInr:FX.usdInr,
    spotDerived:spot,
    wsStatus:WS.status, wsTickAgeMs:WS.lastTickAt?Date.now()-WS.lastTickAt:null,
    tdStatus:TDws.status,
    updatedAt:RC.updatedAt, forexUpdatedAt:FX.updatedAt, timestamp:now,
  });
});

app.get('/debug',function(req,res){
  res.json({
    server:'RR Jewellers v10',
    dhan:{wsStatus:WS.status,packets:WS.packetsReceived,tickAgeMs:WS.lastTickAt?Date.now()-WS.lastTickAt:null,reconnects:WS.reconnectCount,lastConnect:WS.lastConnectAt},
    twelveData:{wsStatus:TDws.status,packets:TDws.packetsReceived,hasKey:!!TWELVE_DATA_KEY,xauUpdatedAt:FX.xauUpdatedAt,xagUpdatedAt:FX.xagUpdatedAt},
    ohlc:{calls:ohlcCallCount,lastError:lastOhlcError},
    rateCache:RC, forexCache:FX,
    activeContracts:getAC(), tokenMap:TOKEN_MAP,
    marketOpen:isMCXOpen(), tokenRenewedAt,
    env:{DHAN_CLIENT_ID:!!DHAN_CLIENT_ID,tokenLen:currentToken.length,TWELVE_DATA_KEY:!!TWELVE_DATA_KEY},
  });
});

app.get('/token-renew',async function(req,res){
  var ok=await renewToken();
  res.json({success:ok,tokenRenewedAt,wsStatus:WS.status});
});
app.get('/spot-test',function(req,res){res.json({spot:spotDerived(),forex:FX,tdStatus:TDws.status});});
app.get('/ping',function(req,res){res.json({ok:true,ts:Date.now()});});

app.get('/updates',async function(req,res){
  try{
    if(!SHEET_ID) throw new Error('no SHEET_ID');
    var url='https://docs.google.com/spreadsheets/d/'+SHEET_ID+'/gviz/tq?tqx=out:json&sheet=Updates';
    var r=await axios.get(url,{timeout:8000});
    var json=r.data.replace(/.*?({.*}).*/s,'$1');
    var data=JSON.parse(json);
    var rows=data.table.rows.map(function(row){return{date:row.c[0]?.v||'',title:row.c[1]?.v||'',content:row.c[2]?.v||'',image:row.c[3]?.v||''};});
    res.json({success:true,updates:rows.filter(function(r){return r.title;})});
  }catch(e){res.json({success:true,updates:[{date:'Today',title:'Welcome to R.R. Jewellers',content:'Live gold & silver rates.',image:''}]});}
});

app.get('/',function(req,res){res.json({status:'RR Jewellers v10',dhanWS:WS.status,tdWS:TDws.status,endpoints:['/rates','/debug','/ping','/token-renew','/spot-test','/updates']});});

// ─── STARTUP ───────────────────────────────────────────
app.listen(PORT,'0.0.0.0',async function(){
  console.log('[STARTUP] RR Jewellers v10 port=%s',PORT);

  var renewed=await renewToken();
  if(!renewed) currentToken=DHAN_ACCESS_TOKEN;

  await refreshUsdInr();
  connectDhan();
  connectTwelveData();

  setInterval(function(){if(isMCXOpen()) pollOhlc();},1000);
  setInterval(refreshUsdInr,5*60*1000);
  // Spot REST fallback refresh every 30s (when no Twelve Data key)
  if(!TWELVE_DATA_KEY) setInterval(refreshSpotREST,30*1000);
  setInterval(function(){if(WS.status==='disconnected'&&!WS.reconnectTimer)connectDhan();},30*1000);
  setInterval(function(){axios.get((SELF_URL||'http://localhost:'+PORT)+'/ping').catch(function(){});},4*60*1000);
  setInterval(function(){buildTokenMap();},24*60*60*1000);
  setInterval(async function(){console.log('[TOKEN] 20hr renew');await renewToken();},20*60*60*1000);
});
