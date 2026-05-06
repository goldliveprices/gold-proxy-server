'use strict';
// RR Jewellers v12.1 — Corrected data sources
// XAU/USD : Twelve Data WebSocket (live ticks) ← keep
// XAG/USD : gold-api.com (primary, no key) + metals.live fallback, REST every 2min
// USD/INR : frankfurter.dev + open.er-api.com + fawazahmed0, REST every 5min
// MCX     : Dhan WS, RC15 Ticker + RC17 Quote
// NOTE    : Dhan discontinued Currency segment Jul 2024 (RBI circular) — no USDINR from Dhan

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');
const http      = require('http');

const app    = express();
const server = http.createServer(app);
app.use(cors());
app.use(express.json());

// ENV
const PORT              = process.env.PORT              || 3000;
const SELF_URL          = process.env.SELF_URL          || '';
const SHEET_ID          = process.env.SHEET_ID          || '';
const DHAN_CLIENT_ID    = process.env.DHAN_CLIENT_ID    || '';
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN || '';
const TWELVE_DATA_KEY   = process.env.TWELVE_DATA_KEY   || '';
const GOLD_MARGIN_PCT   = parseFloat(process.env.GOLD_MARGIN_PCT   || '0');
const SILVER_MARGIN_PCT = parseFloat(process.env.SILVER_MARGIN_PCT || '0');
const DHAN_BASE         = 'https://api.dhan.co/v2';

// MCX Contracts
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
// USDINR via REST only (Dhan discontinued Currency segment Jul 2024 - RBI circular)

function pickCurrentAndNext(c){
  var now=new Date();
  var sorted=c.map(function(x){return Object.assign({},x,{ed:new Date(x.expiry)});})
    .filter(function(x){return !isNaN(x.ed);}).sort(function(a,b){return a.ed-b.ed;});
  var up=sorted.filter(function(x){return x.ed>=now;});
  if(up.length>=2) return {current:up[0],next:up[1]};
  if(up.length===1) return {current:sorted[sorted.length-2]||up[0],next:up[0]};
  var last=sorted.slice(-2);
  return {current:last[0]||sorted[0],next:last[1]||sorted[0]};
}
function getAC(){return {gold:pickCurrentAndNext(GOLD_CONTRACTS),silver:pickCurrentAndNext(SILVER_CONTRACTS)};}

// Rate cache
var RC={
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

// Forex cache
var FX={
  usdInr:84.5,usdInrHigh:0,usdInrLow:Infinity,usdInrBid:0,usdInrAsk:0,
  xauUsd:0,xauBid:0,xauAsk:0,xauHigh:0,xauLow:0,
  xagUsd:0,xagBid:0,xagAsk:0,xagHigh:0,xagLow:0,
  updatedAt:null,src:'init',xauUpdatedAt:null,xagUpdatedAt:null,
};

function isMCXOpen(){
  var d=new Date(Date.now()+5.5*3600000);
  var dow=d.getUTCDay(),t=d.getUTCHours()*60+d.getUTCMinutes();
  if(dow===0) return false;
  return dow===6?(t>=540&&t<840):(t>=540&&t<1435);
}

function msUntilIST(hh,mm){
  var nowMs=Date.now();
  var ist=new Date(nowMs+5.5*3600000);
  var y=ist.getUTCFullYear(),mo=ist.getUTCMonth(),d=ist.getUTCDate();
  var utcMin=hh*60+mm-330;
  var utcH=Math.floor(utcMin/60),utcM=utcMin%60;
  if(utcH<0) utcH+=24;
  var target=new Date(Date.UTC(y,mo,d,utcH,utcM,0,0));
  if(target.getTime()<=nowMs) target.setUTCDate(target.getUTCDate()+1);
  return target.getTime()-nowMs;
}

// ── HTML WebSocket push server (/feed) ──
var feedWSS=new WebSocket.Server({server:server,path:'/feed'});
var feedClients=new Set();

feedWSS.on('connection',function(ws){
  feedClients.add(ws);
  ws.isAlive=true;
  ws.send(JSON.stringify(buildPayload())); // instant snapshot
  ws.on('pong',function(){ws.isAlive=true;});
  ws.on('close',function(){feedClients.delete(ws);});
  ws.on('error',function(){feedClients.delete(ws);});
});

setInterval(function(){
  feedClients.forEach(function(ws){
    if(!ws.isAlive){ws.terminate();feedClients.delete(ws);return;}
    ws.isAlive=false; ws.ping();
  });
},25000);

function broadcast(){
  if(feedClients.size===0) return;
  var msg=JSON.stringify(buildPayload());
  feedClients.forEach(function(ws){
    if(ws.readyState===WebSocket.OPEN) ws.send(msg);
  });
}

function buildPayload(){
  var ac=getAC();
  var gSell=RC.goldLtp>0?Math.round(RC.goldLtp*(1+GOLD_MARGIN_PCT/100)):null;
  var sSell=RC.silverLtp>0?Math.round(RC.silverLtp*(1+SILVER_MARGIN_PCT/100)):null;
  return {
    ts:Date.now(),src:RC.source,mktOpen:isMCXOpen(),
    goldSell:gSell,silverSell:sSell,
    f:{
      g:{ltp:RC.goldLtp||null,bid:RC.goldBid||null,ask:RC.goldAsk||null,
         high:RC.goldHigh||null,low:RC.goldLow||null,open:RC.goldOpen||null,
         pc:RC.goldPrevClose||null,con:ac.gold.current.display,exp:ac.gold.current.expiry},
      gN:{ltp:RC.goldNextLtp||null,bid:RC.goldNextBid||null,ask:RC.goldNextAsk||null,
          high:RC.goldNextHigh||null,low:RC.goldNextLow||null,
          con:ac.gold.next.display,exp:ac.gold.next.expiry},
      s:{ltp:RC.silverLtp||null,bid:RC.silverBid||null,ask:RC.silverAsk||null,
         high:RC.silverHigh||null,low:RC.silverLow||null,open:RC.silverOpen||null,
         pc:RC.silverPrevClose||null,con:ac.silver.current.display,exp:ac.silver.current.expiry},
      sN:{ltp:RC.silverNextLtp||null,bid:RC.silverNextBid||null,ask:RC.silverNextAsk||null,
          high:RC.silverNextHigh||null,low:RC.silverNextLow||null,
          con:ac.silver.next.display,exp:ac.silver.next.expiry},
    },
    sp:{
      xauUsd:FX.xauUsd||null,xauBid:FX.xauBid||null,xauAsk:FX.xauAsk||null,
      xauHigh:FX.xauHigh||null,xauLow:FX.xauLow||null,
      xagUsd:FX.xagUsd||null,xagBid:FX.xagBid||null,xagAsk:FX.xagAsk||null,
      xagHigh:FX.xagHigh||null,xagLow:FX.xagLow||null,
      usdInr:FX.usdInr||null,usdInrBid:FX.usdInrBid||null,usdInrAsk:FX.usdInrAsk||null,
      usdInrHigh:FX.usdInrHigh||null,usdInrLow:FX.usdInrLow===Infinity?null:FX.usdInrLow,
    },
    margin:{g:GOLD_MARGIN_PCT,s:SILVER_MARGIN_PCT},
  };
}

// USD/INR REST fallback every 5min
async function refreshUsdInr(){
  var apis=[
    ['frankfurter',function(){return axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000}).then(function(r){return r.data.rates.INR;});}],
    ['open.er-api',function(){return axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000}).then(function(r){return r.data.rates.INR;});}],
    ['fawazahmed0',function(){return axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json',{timeout:5000}).then(function(r){return r.data.usd.inr;});}],
  ];
  var v=0,src='';
  for(var i=0;i<apis.length&&!v;i++){
    try{var r=await apis[i][1]();if(r>70&&r<115){v=r;src=apis[i][0];}}catch(e){}
  }
  if(!v){v=FX.usdInr;src='cached';}
  FX.usdInr=Math.round(v*100)/100;
  if(FX.usdInr>0){
    if(!FX.usdInrHigh||FX.usdInr>FX.usdInrHigh) FX.usdInrHigh=FX.usdInr;
    if(FX.usdInrLow===Infinity||FX.usdInr<FX.usdInrLow) FX.usdInrLow=FX.usdInr;
  }
  FX.updatedAt=new Date().toISOString();FX.src=src;
  console.log('[FOREX] usdInr=%s src=%s',FX.usdInr,src);
}

// XAG/USD — gold-api.com (primary, free, no key, has bid/ask/H/L)
// Fallback 1: Twelve Data REST
// Fallback 2: metals.live
async function pollXagUsd(){
  // Primary: gold-api.com — free, no key, gives bid/ask/open/high/low
  try{
    var r=await axios.get('https://www.gold-api.com/price/XAG',{timeout:6000});
    var d=r.data;
    if(d&&d.price>20&&d.price<300){
      FX.xagUsd=Math.round(d.price*1000)/1000;
      if(d.bid>0)  FX.xagBid=Math.round(d.bid*1000)/1000;
      if(d.ask>0)  FX.xagAsk=Math.round(d.ask*1000)/1000;
      if(d.high_price>0){if(!FX.xagHigh||d.high_price>FX.xagHigh) FX.xagHigh=Math.round(d.high_price*1000)/1000;}
      if(d.low_price>0) {if(!FX.xagLow||d.low_price<FX.xagLow)   FX.xagLow=Math.round(d.low_price*1000)/1000;}
      FX.xagUpdatedAt=new Date().toISOString();
      console.log('[XAG] gold-api.com XAG=%s bid=%s ask=%s H=%s L=%s',FX.xagUsd,FX.xagBid,FX.xagAsk,FX.xagHigh,FX.xagLow);
      broadcast();
      return;
    }
  }catch(e){console.warn('[XAG] gold-api.com fail:',e.message.slice(0,50));}

  // Fallback 1: Twelve Data REST
  if(TWELVE_DATA_KEY){
    try{
      var r2=await axios.get('https://api.twelvedata.com/price',{params:{symbol:'XAG/USD',apikey:TWELVE_DATA_KEY},timeout:8000});
      var p=parseFloat(r2.data&&r2.data.price);
      if(p>20&&p<300){
        FX.xagUsd=Math.round(p*1000)/1000;
        FX.xagBid=Math.round((p-0.02)*1000)/1000;
        FX.xagAsk=Math.round((p+0.02)*1000)/1000;
        if(!FX.xagHigh||p>FX.xagHigh) FX.xagHigh=Math.round(p*1000)/1000;
        if(!FX.xagLow||p<FX.xagLow)   FX.xagLow=Math.round(p*1000)/1000;
        FX.xagUpdatedAt=new Date().toISOString();
        console.log('[XAG] TD fallback XAG=%s',FX.xagUsd);
        broadcast();
        return;
      }
    }catch(e){console.warn('[XAG] TD fail:',e.message.slice(0,50));}
  }

  // Fallback 2: metals.live
  try{
    var r3=await axios.get('https://api.metals.live/v1/spot/silver',{timeout:6000});
    if(Array.isArray(r3.data)){
      var s=r3.data.find(function(x){return x.silver;});
      if(s&&s.silver>20&&s.silver<300){
        FX.xagUsd=Math.round(s.silver*1000)/1000;
        FX.xagBid=Math.round((s.silver-0.02)*1000)/1000;
        FX.xagAsk=Math.round((s.silver+0.02)*1000)/1000;
        FX.xagUpdatedAt=new Date().toISOString();
        console.log('[XAG] metals.live fallback XAG=%s',FX.xagUsd);
        broadcast();
      }
    }
  }catch(e){console.warn('[XAG] metals.live fail:',e.message.slice(0,50));}
}

// USD/INR H/L from TD /quote every 15min
async function pollUsdInrQuoteTD(){
  if(!TWELVE_DATA_KEY) return;
  try{
    var r=await axios.get('https://api.twelvedata.com/quote',{params:{symbol:'USD/INR',apikey:TWELVE_DATA_KEY},timeout:8000});
    var q=r.data;
    if(q&&q.high&&q.low){
      var hi=parseFloat(q.high),lo=parseFloat(q.low);
      if(hi>70&&hi<115) FX.usdInrHigh=Math.round(hi*100)/100;
      if(lo>70&&lo<115) FX.usdInrLow=Math.round(lo*100)/100;
      if(q.bid) FX.usdInrBid=Math.round(parseFloat(q.bid)*100)/100;
      if(q.ask) FX.usdInrAsk=Math.round(parseFloat(q.ask)*100)/100;
    }
  }catch(e){}
}

// Twelve Data WS — XAU/USD live
var TDws={ws:null,status:'disconnected',reconnectTimer:null,reconnectCount:0,lastConnectAt:null,packetsReceived:0,pingTimer:null};
function connectTwelveData(){
  if(!TWELVE_DATA_KEY){refreshSpotREST();return;}
  if(TDws.status==='connecting'||TDws.status==='connected') return;
  TDws.status='connecting';TDws.lastConnectAt=new Date().toISOString();
  var ws=new WebSocket('wss://ws.twelvedata.com/v1/quotes/price?apikey='+TWELVE_DATA_KEY,{handshakeTimeout:15000});
  TDws.ws=ws;
  ws.on('open',function(){
    TDws.status='connected';TDws.reconnectCount=0;
    ws.send(JSON.stringify({action:'subscribe',params:{symbols:'XAU/USD'}}));
    if(TDws.pingTimer) clearInterval(TDws.pingTimer);
    TDws.pingTimer=setInterval(function(){if(ws.readyState===WebSocket.OPEN) ws.ping();},20000);
  });
  ws.on('message',function(data){
    try{
      var msg=JSON.parse(data);
      if(msg.event==='heartbeat') return;
      if(msg.event==='price'&&msg.symbol==='XAU/USD'&&msg.price){
        var p=parseFloat(msg.price);
        if(p>3000&&p<9000){
          var b=parseFloat(msg.bid||msg.price),a=parseFloat(msg.ask||msg.price);
          FX.xauUsd=Math.round(p*100)/100;
          FX.xauBid=Math.round(b*100)/100;
          FX.xauAsk=Math.round(a*100)/100;
          if(!FX.xauHigh||p>FX.xauHigh) FX.xauHigh=Math.round(p*100)/100;
          if(!FX.xauLow||p<FX.xauLow)   FX.xauLow=Math.round(p*100)/100;
          FX.xauUpdatedAt=new Date().toISOString();
          TDws.packetsReceived++;
          broadcast();
        }
      }
    }catch(e){}
  });
  ws.on('close',function(code){
    TDws.status='disconnected';
    if(TDws.pingTimer){clearInterval(TDws.pingTimer);TDws.pingTimer=null;}
    TDws.reconnectCount++;
    var d=Math.min(3000*Math.pow(2,Math.min(TDws.reconnectCount,4)),30000);
    TDws.reconnectTimer=setTimeout(function(){TDws.reconnectTimer=null;connectTwelveData();},d);
  });
  ws.on('error',function(e){console.warn('[TD] err:',e.message);});
}

async function refreshSpotREST(){
  try{
    var r=await axios.get('https://api.metals.live/v1/spot/gold,silver',{timeout:6000});
    if(Array.isArray(r.data)){
      var g=r.data.find(function(x){return x.gold;}),s=r.data.find(function(x){return x.silver;});
      if(g&&g.gold>3000){FX.xauUsd=Math.round(g.gold*100)/100;FX.xauUpdatedAt=new Date().toISOString();}
      if(s&&s.silver>20){FX.xagUsd=Math.round(s.silver*1000)/1000;FX.xagUpdatedAt=new Date().toISOString();}
      return;
    }
  }catch(e){}
  try{
    var res=await Promise.all([axios.get('https://www.gold-api.com/price/XAU',{timeout:7000}),axios.get('https://www.gold-api.com/price/XAG',{timeout:7000})]);
    if(res[0].data.price>3000){FX.xauUsd=Math.round(res[0].data.price*100)/100;FX.xauUpdatedAt=new Date().toISOString();}
    if(res[1].data.price>20){FX.xagUsd=Math.round(res[1].data.price*1000)/1000;FX.xagUpdatedAt=new Date().toISOString();}
  }catch(e){}
}

function spotDerived(){
  return {
    goldPer10g:FX.xauUsd>0&&FX.usdInr>0?Math.round((FX.xauUsd/31.1035)*10*FX.usdInr*1.0920):0,
    silverPerKg:FX.xagUsd>0&&FX.usdInr>0?Math.round((FX.xagUsd/31.1035)*1000*FX.usdInr*1.0661):0,
  };
}

// Token renew
var currentToken=DHAN_ACCESS_TOKEN,tokenRenewedAt=null,renewRetryTimer=null,renewAttempts=0;
async function renewToken(){
  if(!currentToken||!DHAN_CLIENT_ID) return false;
  try{
    var r=await axios.post(DHAN_BASE+'/RenewToken',{},{
      headers:{'access-token':currentToken,'dhanClientId':DHAN_CLIENT_ID,'Content-Type':'application/json'},
      timeout:12000,
    });
    var t=r.data?.accessToken||r.data?.access_token||r.data?.data?.accessToken;
    if(t){
      currentToken=t;tokenRenewedAt=new Date().toISOString();renewAttempts=0;
      if(renewRetryTimer){clearTimeout(renewRetryTimer);renewRetryTimer=null;}
      console.log('[TOKEN] Renewed len=%d',t.length);
      if(WS.ws){try{WS.ws.terminate();}catch(e){}}
      WS.status='disconnected';
      setTimeout(connectDhan,2000);
      return true;
    }
    return false;
  }catch(e){console.warn('[TOKEN] fail:',e.message.slice(0,80));return false;}
}
async function renewWithRetry(){
  var ok=await renewToken();
  if(!ok){
    renewAttempts++;
    var d=Math.min(renewAttempts*2*60*1000,5*60*1000);
    console.warn('[TOKEN] Retry #%d in %ds',renewAttempts,d/1000);
    renewRetryTimer=setTimeout(renewWithRetry,d);
  }
}
function scheduleDailyRenew(){
  var ms=msUntilIST(8,30);
  console.log('[TOKEN] Daily renew in %dm',(ms/60000).toFixed(0));
  setTimeout(function(){renewWithRetry();scheduleDailyRenew();},ms);
}
function scheduleSpotHLReset(){
  var ms=msUntilIST(9,0);
  setTimeout(function(){
    FX.xauHigh=0;FX.xauLow=0;FX.xagHigh=0;FX.xagLow=0;FX.usdInrHigh=0;FX.usdInrLow=Infinity;
    console.log('[HL-RESET] Spot H/L reset 9AM IST');
    scheduleSpotHLReset();
  },ms);
}

// Dhan WS
var WS={ws:null,status:'disconnected',reconnectTimer:null,reconnectCount:0,
  lastConnectAt:null,lastDisconnectAt:null,lastTickAt:null,packetsReceived:0,
  lastRawHex:'',lastTextMsg:'',pingTimer:null};
var TOKEN_MAP={};

function buildTokenMap(){
  var ac=getAC();TOKEN_MAP={};
  TOKEN_MAP[ac.gold.current.secId]='gold';
  TOKEN_MAP[ac.gold.next.secId]='goldNext';
  TOKEN_MAP[ac.silver.current.secId]='silver';
  TOKEN_MAP[ac.silver.next.secId]='silverNext';
  console.log('[TOKENMAP]',JSON.stringify(TOKEN_MAP));
}

function subscribeWS(ws){
  var ac=getAC();
  var mcx=[
    {ExchangeSegment:'MCX_COMM',SecurityId:ac.gold.current.secId},
    {ExchangeSegment:'MCX_COMM',SecurityId:ac.gold.next.secId},
    {ExchangeSegment:'MCX_COMM',SecurityId:ac.silver.current.secId},
    {ExchangeSegment:'MCX_COMM',SecurityId:ac.silver.next.secId},
  ];
  var send=function(obj,delay){
    setTimeout(function(){if(ws.readyState===WebSocket.OPEN) ws.send(JSON.stringify(obj));},delay);
  };
  // RC15=Ticker (fastest LTP), RC17=Quote (OHLC+OHLC)
  send({RequestCode:15,InstrumentCount:mcx.length,InstrumentList:mcx},0);
  send({RequestCode:17,InstrumentCount:mcx.length,InstrumentList:mcx},200);
  console.log('[WS] Subscribed %d MCX instruments',mcx.length);
}

function parseBuf(buf){
  try{
    if(!buf||buf.length<8) return null;
    var fc=buf.readUInt8(0),secId=buf.readInt32LE(4).toString();
    if(fc===50) return null;
    if(fc===6&&buf.length>=16){var pc=buf.readFloatLE(8);return isFinite(pc)&&pc>0?{type:'prevClose',secId,prevClose:Math.round(pc)}:null;}
    if(fc===2&&buf.length>=16){var l2=buf.readFloatLE(8);return !isFinite(l2)||l2<=10?null:{type:'ticker',secId,ltp:Math.round(l2*100)/100};}
    if(fc===4&&buf.length>=50){
      var l4=buf.readFloatLE(8);if(!isFinite(l4)||l4<=10) return null;
      return{type:'quote',secId,ltp:Math.round(l4*100)/100,
        open:Math.round(buf.readFloatLE(34)*100)/100||0,
        high:Math.round(buf.readFloatLE(42)*100)/100||0,
        low:Math.round(buf.readFloatLE(46)*100)/100||0};
    }
    if(fc===8&&buf.length>=62){
      var l8=buf.readFloatLE(8);if(!isFinite(l8)||l8<=10) return null;
      var o8=buf.length>49?Math.round(buf.readFloatLE(46)*100)/100:0;
      var h8=buf.length>57?Math.round(buf.readFloatLE(54)*100)/100:0;
      var lw8=buf.length>61?Math.round(buf.readFloatLE(58)*100)/100:0;
      var b8=Math.round(l8*100)/100,a8=Math.round(l8*100)/100;
      if(buf.length>=82){var bf=buf.readFloatLE(74),af=buf.readFloatLE(78);if(isFinite(bf)&&bf>10)b8=Math.round(bf*100)/100;if(isFinite(af)&&af>10)a8=Math.round(af*100)/100;}
      return{type:'full',secId,ltp:Math.round(l8*100)/100,bid:b8,ask:a8,open:o8,high:h8,low:lw8};
    }
    return null;
  }catch(e){return null;}
}

function scheduleReconnect(){
  if(WS.reconnectTimer) return;
  WS.reconnectCount++;
  if(WS.reconnectCount>=5&&WS.reconnectCount%5===0){console.warn('[WS] %d reconnects — renew token',WS.reconnectCount);renewWithRetry();}
  var d=Math.min(2000*Math.pow(2,Math.min(WS.reconnectCount-1,4)),20000);
  WS.reconnectTimer=setTimeout(function(){WS.reconnectTimer=null;connectDhan();},d);
  console.log('[WS] Reconnect #%d in %ds',WS.reconnectCount,d/1000);
}

function connectDhan(){
  if(!DHAN_CLIENT_ID||!currentToken){console.warn('[WS] No creds');return;}
  if(WS.status==='connecting'||WS.status==='connected') return;
  WS.status='connecting';WS.lastConnectAt=new Date().toISOString();
  WS.packetsReceived=0;WS.lastRawHex='';
  buildTokenMap();
  var wsUrl='wss://api-feed.dhan.co?version=2&token='+encodeURIComponent(currentToken)+'&clientId='+encodeURIComponent(DHAN_CLIENT_ID)+'&authType=2';
  var ws=new WebSocket(wsUrl,{handshakeTimeout:15000});WS.ws=ws;

  ws.on('open',function(){
    WS.status='connected';WS.reconnectCount=0;console.log('[WS] Connected');
    subscribeWS(ws);
    if(WS.pingTimer) clearInterval(WS.pingTimer);
    WS.pingTimer=setInterval(function(){if(ws.readyState===WebSocket.OPEN) ws.ping();},20000);
  });

  ws.on('message',function(data){
    if(typeof data==='string'){WS.lastTextMsg=data.slice(0,200);return;}
    var buf=Buffer.isBuffer(data)?data:Buffer.from(data);
    WS.packetsReceived++;
    if(WS.packetsReceived<=3) WS.lastRawHex=buf.slice(0,32).toString('hex');
    var tick=parseBuf(buf);if(!tick) return;
    WS.lastTickAt=Date.now();
    var key=TOKEN_MAP[tick.secId];if(!key) return;

    if(tick.type==='prevClose'){
      if(key==='gold')   RC.goldPrevClose=tick.prevClose;
      if(key==='silver') RC.silverPrevClose=tick.prevClose;
      return;
    }
    applyTick(key,tick);
    RC.source='dhan_ws_live';
    broadcast(); // push immediately to all HTML clients
  });

  ws.on('pong',function(){WS.lastTickAt=Date.now();});
  ws.on('close',function(code){
    WS.status='disconnected';WS.lastDisconnectAt=new Date().toISOString();
    if(WS.pingTimer){clearInterval(WS.pingTimer);WS.pingTimer=null;}
    console.warn('[WS] Closed code=%d pkt=%d',code,WS.packetsReceived);
    scheduleReconnect();
  });
  ws.on('error',function(e){console.warn('[WS] err:',e.message);});
}

// OHLC REST backup
var lastOhlcError=null,ohlcCallCount=0,ohlcBackoffUntil=0;
function pollOhlc(){
  if(!DHAN_CLIENT_ID||!currentToken) return;
  if(Date.now()<ohlcBackoffUntil) return;
  if(WS.status==='connected'&&WS.lastTickAt&&Date.now()-WS.lastTickAt<3000) return;
  var ac=getAC();
  var secIds=[parseInt(ac.gold.current.secId,10),parseInt(ac.gold.next.secId,10),parseInt(ac.silver.current.secId,10),parseInt(ac.silver.next.secId,10)];
  axios.post(DHAN_BASE+'/marketfeed/ohlc',{MCX_COMM:secIds},{
    headers:{'Accept':'application/json','Content-Type':'application/json','access-token':currentToken,'client-id':DHAN_CLIENT_ID},
    timeout:5000,
  }).then(function(resp){
    var seg=resp.data&&resp.data.data&&resp.data.data['MCX_COMM'];
    if(!seg){lastOhlcError='No MCX_COMM';return;}
    ohlcCallCount++;lastOhlcError=null;
    function applyRow(secId,key){
      var row=seg[String(secId)];if(!row)return;
      var ltp=row.last_price||0,ohlc=row.ohlc||{};
      if(ltp>0){applyTick(key,{ltp:Math.round(ltp),open:ohlc.open?Math.round(ohlc.open):0,high:ohlc.high?Math.round(ohlc.high):0,low:ohlc.low?Math.round(ohlc.low):0});RC.source='dhan_ohlc_rest';}
    }
    applyRow(ac.gold.current.secId,'gold');applyRow(ac.gold.next.secId,'goldNext');
    applyRow(ac.silver.current.secId,'silver');applyRow(ac.silver.next.secId,'silverNext');
    broadcast();
  }).catch(function(e){
    lastOhlcError=e.message;
    if(e.response&&e.response.status===429){ohlcBackoffUntil=Date.now()+60000;console.warn('[OHLC] 429 backoff 60s');}
  });
}

// Routes
app.get('/rates',function(req,res){res.json(buildPayload());});
app.get('/debug',function(req,res){
  res.json({server:'RR Jewellers v12',htmlClients:feedClients.size,
    dhan:{wsStatus:WS.status,packets:WS.packetsReceived,tickAgeMs:WS.lastTickAt?Date.now()-WS.lastTickAt:null,reconnects:WS.reconnectCount,lastConnect:WS.lastConnectAt},
    twelveData:{wsStatus:TDws.status,packets:TDws.packetsReceived,hasKey:!!TWELVE_DATA_KEY,xauUpdatedAt:FX.xauUpdatedAt,xagUpdatedAt:FX.xagUpdatedAt},
    ohlc:{calls:ohlcCallCount,lastError:lastOhlcError,backoffUntil:ohlcBackoffUntil>Date.now()?new Date(ohlcBackoffUntil).toISOString():null},
    rateCache:RC,forexCache:FX,activeContracts:getAC(),tokenMap:TOKEN_MAP,
    marketOpen:isMCXOpen(),tokenRenewedAt,renewAttempts,
    env:{DHAN_CLIENT_ID:!!DHAN_CLIENT_ID,tokenLen:currentToken.length,TWELVE_DATA_KEY:!!TWELVE_DATA_KEY},
  });
});
app.get('/token-renew',async function(req,res){var ok=await renewToken();res.json({success:ok,tokenRenewedAt,wsStatus:WS.status,tokenLen:currentToken.length});});
app.get('/ping',function(req,res){res.json({ok:true,ts:Date.now(),wsStatus:WS.status,htmlClients:feedClients.size,tokenRenewedAt});});
app.get('/spot-test',function(req,res){res.json({spot:spotDerived(),forex:FX,tdStatus:TDws.status});});
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
app.get('/',function(req,res){res.json({status:'RR Jewellers v12',dhanWS:WS.status,tdWS:TDws.status,htmlClients:feedClients.size,tokenRenewedAt,endpoints:['/rates','/debug','/ping','/token-renew','/spot-test','/updates','/feed (WS push)']});});

// Startup
server.listen(PORT,'0.0.0.0',async function(){
  console.log('[STARTUP] RR Jewellers v12 port=%s',PORT);
  await renewWithRetry();
  await refreshUsdInr();
  connectDhan();
  connectTwelveData();

  // XAG/USD: gold-api.com primary, every 2min
  pollXagUsd();
  setInterval(pollXagUsd, 2*60*1000);

  // USD/INR H/L from TD quote every 15min
  if(TWELVE_DATA_KEY){
    pollUsdInrQuoteTD();
    setInterval(pollUsdInrQuoteTD,15*60*1000);
  }

  // REST spot fallback (no TD key)
  if(!TWELVE_DATA_KEY){ refreshSpotREST();setInterval(refreshSpotREST,30*1000); }
  setInterval(function(){if(isMCXOpen()) pollOhlc();},5000);
  setInterval(refreshUsdInr,5*60*1000);
  setInterval(function(){if(WS.status==='disconnected'&&!WS.reconnectTimer) connectDhan();},30*1000);
  setInterval(function(){buildTokenMap();},24*60*60*1000);
  setInterval(function(){axios.get((SELF_URL||'http://localhost:'+PORT)+'/ping').catch(function(){});},4*60*1000);
  scheduleDailyRenew();
  scheduleSpotHLReset();
  console.log('[STARTUP] v12 ready — WebSocket push on /feed');
});
