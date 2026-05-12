'use strict';
// ═══════════════════════════════════════════════════════════════════
// RR Jewellers v16 — Dual Isolated Feed Architecture
//
// /feed/mcx → Dhan MCX binary ticks ONLY (20ms) — broadcastMCX()
// /feed/fx  → FCS Spot ticks ONLY (200ms)       — broadcastFX()
// Zero coupling. Each fires independently at its own speed.
//
// FIXES v16:
//   - NO shared broadcast() — Dhan and FCS never block each other
//   - TOTP: checks t-1, t, t+1 (clock drift fix)
//   - Startup: valid token → skip renewWithRetry
//   - Env polling 60s (Render dashboard token detection)
//   - OHLC 401 → auto renewWithRetry
//   - FCS msg: high/low extracted from p.h/p.l
//
// npm: express, ws, axios (NO speakeasy)
// ═══════════════════════════════════════════════════════════════════

const express   = require('express');
const http      = require('http');
const axios     = require('axios');
const WebSocket = require('ws');
const crypto    = require('crypto');

const app    = express();
const server = http.createServer(app);
app.use(express.json());
app.use((req, res, next) => { res.setHeader('Access-Control-Allow-Origin', '*'); next(); });

const PORT      = process.env.PORT      || 3000;
const SELF_URL  = process.env.SELF_URL  || '';
const SHEET_ID  = process.env.SHEET_ID  || '';
const DHAN_BASE = 'https://api.dhan.co/v2';
const goldPct   = () => parseFloat(process.env.GOLD_MARGIN_PCT   || '0');
const silverPct = () => parseFloat(process.env.SILVER_MARGIN_PCT || '0');

// ═══════════════════════════════════════════════════════════════════
// CACHE ENGINE
// ═══════════════════════════════════════════════════════════════════
const S = {
  goldLtp:0,goldBid:0,goldAsk:0,goldHigh:0,goldLow:0,goldOpen:0,goldPrevClose:0,
  goldNextLtp:0,goldNextBid:0,goldNextAsk:0,goldNextHigh:0,goldNextLow:0,
  silverLtp:0,silverBid:0,silverAsk:0,silverHigh:0,silverLow:0,silverOpen:0,silverPrevClose:0,
  silverNextLtp:0,silverNextBid:0,silverNextAsk:0,silverNextHigh:0,silverNextLow:0,
  xauUsd:0,xauBid:0,xauAsk:0,xauHigh:0,xauLow:0,
  xagUsd:0,xagBid:0,xagAsk:0,xagHigh:0,xagLow:0,
  usdInr:0,usdInrBid:0,usdInrAsk:0,usdInrHigh:0,usdInrLow:Infinity,
  mcxSrc:'init',fxSrc:'init',mcxAt:0,fxAt:0,
};

const MCX_MAP = {
  gold:       ['goldLtp','goldBid','goldAsk','goldHigh','goldLow','goldOpen','goldPrevClose'],
  goldNext:   ['goldNextLtp','goldNextBid','goldNextAsk','goldNextHigh','goldNextLow'],
  silver:     ['silverLtp','silverBid','silverAsk','silverHigh','silverLow','silverOpen','silverPrevClose'],
  silverNext: ['silverNextLtp','silverNextBid','silverNextAsk','silverNextHigh','silverNextLow'],
};

function writeMCX(key, tick) {
  const f = MCX_MAP[key];
  if (!f || !tick.ltp || tick.ltp <= 0) return false;
  const prev = S[f[0]];
  S[f[0]] = tick.ltp;
  if (f[1] && tick.bid  > 0) S[f[1]] = tick.bid;
  if (f[2] && tick.ask  > 0) S[f[2]] = tick.ask;
  if (f[3] && tick.high > 0) S[f[3]] = tick.high;
  if (f[4] && tick.low  > 0) S[f[4]] = tick.low;
  if (f[5] && tick.open > 0) S[f[5]] = tick.open;
  if (f[6] && tick.prevClose > 0) S[f[6]] = tick.prevClose;
  const changed = tick.ltp !== prev || prev === 0;
  if (changed) { S.mcxSrc = tick.src || 'dhan_ws'; S.mcxAt = Date.now(); }
  return changed;
}

function writeFX(sym, d) {
  const now = Date.now(); let changed = false;
  if (sym === 'XAU' && d.price > 3000 && d.price < 9000) {
    if (d.price !== S.xauUsd) { S.xauUsd = r2(d.price); changed = true; }
    if (d.bid  > 0) S.xauBid  = r2(d.bid);
    if (d.ask  > 0) S.xauAsk  = r2(d.ask);
    if (d.high > 0) S.xauHigh = r2(d.high);
    if (d.low  > 0) S.xauLow  = r2(d.low);
    S.fxAt = now;
  } else if (sym === 'XAG' && d.price > 20 && d.price < 300) {
    if (d.price !== S.xagUsd) { S.xagUsd = r3(d.price); changed = true; }
    if (d.bid  > 0) S.xagBid  = r3(d.bid);
    if (d.ask  > 0) S.xagAsk  = r3(d.ask);
    if (d.high > 0) S.xagHigh = r3(d.high);
    if (d.low  > 0) S.xagLow  = r3(d.low);
    S.fxAt = now;
  } else if (sym === 'INR' && d.price > 70 && d.price < 115) {
    if (d.price !== S.usdInr) { S.usdInr = r2(d.price); changed = true; }
    if (d.bid  > 0) S.usdInrBid  = r2(d.bid);
    if (d.ask  > 0) S.usdInrAsk  = r2(d.ask);
    if (d.high > 0) S.usdInrHigh = r2(d.high);
    if (d.low  > 0 && d.low < S.usdInrLow) S.usdInrLow = r2(d.low);
    S.fxAt = now;
  }
  if (changed) S.fxSrc = d.src || 'api';
  return changed;
}

function resetHL() {
  S.xauHigh=0;S.xauLow=0;S.xagHigh=0;S.xagLow=0;
  S.usdInrHigh=0;S.usdInrLow=Infinity;
  S.goldHigh=0;S.goldLow=0;S.silverHigh=0;S.silverLow=0;
  S.goldNextHigh=0;S.goldNextLow=0;S.silverNextHigh=0;S.silverNextLow=0;
  console.log('[CACHE] Daily H/L reset 9AM IST');
}

const r2  = v => Math.round(v*100)/100;
const r3  = v => Math.round(v*1000)/1000;
const nn  = v => (v&&v>0)?v:null;
const nni = v => (v===Infinity||!v||v<=0)?null:v;

// ═══════════════════════════════════════════════════════════════════
// TICK ENGINE
// ═══════════════════════════════════════════════════════════════════
const GOLD_C = [
  {secId:'459277',display:'GOLD JUN26', expiry:'2026-06-05'},
  {secId:'466583',display:'GOLD AUG26', expiry:'2026-08-05'},
  {secId:'483079',display:'GOLD OCT26', expiry:'2026-10-05'},
  {secId:'495213',display:'GOLD DEC26', expiry:'2026-12-04'},
  {secId:'559933',display:'GOLD FEB27', expiry:'2027-02-05'},
];
const SILV_C = [
  {secId:'464150',display:'SILVER JUL26',expiry:'2026-07-03'},
  {secId:'471725',display:'SILVER SEP26',expiry:'2026-09-04'},
  {secId:'495214',display:'SILVER DEC26',expiry:'2026-12-04'},
  {secId:'564619',display:'SILVER MAR27',expiry:'2027-03-05'},
];
let AC={}, TOKEN_MAP={};

function pickCN(list) {
  const now=new Date();
  const s=list.map(c=>({...c,ed:new Date(c.expiry)})).filter(c=>!isNaN(c.ed)).sort((a,b)=>a.ed-b.ed);
  const up=s.filter(c=>c.ed>=now);
  if(up.length>=2) return {cur:up[0],nxt:up[1]};
  const last=s.slice(-2);
  return {cur:last[0]||s[0],nxt:last[1]||s[0]};
}
function refreshAC() {
  const g=pickCN(GOLD_C),s=pickCN(SILV_C);
  AC={gold:g.cur,goldNext:g.nxt,silver:s.cur,silverNext:s.nxt};
  TOKEN_MAP={[g.cur.secId]:'gold',[g.nxt.secId]:'goldNext',[s.cur.secId]:'silver',[s.nxt.secId]:'silverNext'};
}
refreshAC();
setInterval(refreshAC,6*3600*1000);

function isMCXOpen() {
  const d=new Date(Date.now()+5.5*3600000),dow=d.getUTCDay(),t=d.getUTCHours()*60+d.getUTCMinutes();
  if(dow===0) return false;
  return dow===6?(t>=540&&t<840):(t>=540&&t<1435);
}

// MCX-only payload — sent on /feed/mcx
function buildMCXPayload() {
  const gm=goldPct(),sm=silverPct();
  const gSell=S.goldLtp>0?Math.round(S.goldLtp*(1+gm/100)):null;
  const sSell=S.silverLtp>0?Math.round(S.silverLtp*(1+sm/100)):null;
  return {
    t:'mcx', ts:Date.now(), src:S.mcxSrc, mktOpen:isMCXOpen(),
    goldSell:gSell, silverSell:sSell,
    f:{
      g: {ltp:nn(S.goldLtp),    bid:nn(S.goldBid),    ask:nn(S.goldAsk),    high:nn(S.goldHigh),    low:nn(S.goldLow),    open:nn(S.goldOpen),   pc:nn(S.goldPrevClose),  con:AC.gold?.display,    exp:AC.gold?.expiry},
      gN:{ltp:nn(S.goldNextLtp),bid:nn(S.goldNextBid),ask:nn(S.goldNextAsk),high:nn(S.goldNextHigh),low:nn(S.goldNextLow),                                                con:AC.goldNext?.display,exp:AC.goldNext?.expiry},
      s: {ltp:nn(S.silverLtp),  bid:nn(S.silverBid),  ask:nn(S.silverAsk),  high:nn(S.silverHigh),  low:nn(S.silverLow),  open:nn(S.silverOpen), pc:nn(S.silverPrevClose),con:AC.silver?.display,  exp:AC.silver?.expiry},
      sN:{ltp:nn(S.silverNextLtp),bid:nn(S.silverNextBid),ask:nn(S.silverNextAsk),high:nn(S.silverNextHigh),low:nn(S.silverNextLow),                                     con:AC.silverNext?.display,exp:AC.silverNext?.expiry},
    },
    margin:{g:gm,s:sm},
    // legacy
    success:true,source:S.mcxSrc,goldPer10g:gSell,silverPerKg:sSell,
  };
}

// FX-only payload — sent on /feed/fx
function buildFXPayload() {
  return {
    t:'fx', ts:Date.now(), src:S.fxSrc,
    sp:{
      xauUsd:nn(S.xauUsd),xauBid:nn(S.xauBid),xauAsk:nn(S.xauAsk),xauHigh:nn(S.xauHigh),xauLow:nn(S.xauLow),
      xagUsd:nn(S.xagUsd),xagBid:nn(S.xagBid),xagAsk:nn(S.xagAsk),xagHigh:nn(S.xagHigh),xagLow:nn(S.xagLow),
      usdInr:nn(S.usdInr),usdInrBid:nn(S.usdInrBid),usdInrAsk:nn(S.usdInrAsk),usdInrHigh:nn(S.usdInrHigh),usdInrLow:nni(S.usdInrLow),
    },
  };
}

// Combined REST snapshot
function buildFullPayload() {
  const m=buildMCXPayload(), f=buildFXPayload();
  return {...m,...f,t:'full',sp:f.sp,spot:f.sp,
    futures:{
      gold:      {ltp:nn(S.goldLtp),      bid:nn(S.goldBid),      ask:nn(S.goldAsk),      high:nn(S.goldHigh),    low:nn(S.goldLow),   open:nn(S.goldOpen)},
      silver:    {ltp:nn(S.silverLtp),    bid:nn(S.silverBid),    ask:nn(S.silverAsk),    high:nn(S.silverHigh),  low:nn(S.silverLow), open:nn(S.silverOpen)},
      goldNext:  {ltp:nn(S.goldNextLtp),  bid:nn(S.goldNextBid),  ask:nn(S.goldNextAsk),  high:nn(S.goldNextHigh),low:nn(S.goldNextLow)},
      silverNext:{ltp:nn(S.silverNextLtp),bid:nn(S.silverNextBid),ask:nn(S.silverNextAsk),high:nn(S.silverNextHigh),low:nn(S.silverNextLow)},
    },
    xauUsd:nn(S.xauUsd),xagUsd:nn(S.xagUsd),usdInr:nn(S.usdInr),
  };
}

// ═══════════════════════════════════════════════════════════════════
// DUAL FEED SERVERS — /feed/mcx and /feed/fx
// ═══════════════════════════════════════════════════════════════════
const mcxWSS = new WebSocket.Server({noServer:true,perMessageDeflate:false});
const fxWSS  = new WebSocket.Server({noServer:true,perMessageDeflate:false});
const mcxClients=new Set(), fxClients=new Set();
let lastMCX=null, lastFX=null, mcxBC=0, fxBC=0;

server.on('upgrade',(req,socket,head)=>{
  const url=req.url||'', ip=req.headers['x-forwarded-for']||req.socket?.remoteAddress||'?';
  console.log('[WS] upgrade url=%s ip=%s',url,ip);
  if(url==='/feed/mcx'||url.startsWith('/feed/mcx?')) {
    mcxWSS.handleUpgrade(req,socket,head,ws=>mcxWSS.emit('connection',ws,req));
  } else if(url==='/feed/fx'||url.startsWith('/feed/fx?')) {
    fxWSS.handleUpgrade(req,socket,head,ws=>fxWSS.emit('connection',ws,req));
  } else if(url==='/feed'||url.startsWith('/feed?')||url==='/'||url.startsWith('/?')) {
    // Legacy path → MCX feed (HTML v13 compat)
    mcxWSS.handleUpgrade(req,socket,head,ws=>mcxWSS.emit('connection',ws,req));
  } else {
    console.warn('[WS] rejected url=%s',url); socket.destroy();
  }
});

function setupChannel(wss, clients, label, getSnap) {
  wss.on('connection',(ws,req)=>{
    clients.add(ws); ws.isAlive=true; ws.connAt=Date.now();
    const ip=req?.headers?.['x-forwarded-for']||req?.socket?.remoteAddress||'?';
    console.log('[%s] ✅ connected ip=%s total=%d',label,ip,clients.size);
    try { const s=getSnap(); if(s) ws.send(s); } catch{}
    ws.on('pong',()=>{ws.isAlive=true;});
    ws.on('close',code=>{clients.delete(ws);console.log('[%s] disconnected code=%d left=%d',label,code,clients.size);});
    ws.on('error',()=>clients.delete(ws));
    ws.ping();
  });
  setInterval(()=>{
    let dead=0;
    for(const ws of clients){
      if(!ws.isAlive){ws.terminate();clients.delete(ws);dead++;continue;}
      ws.isAlive=false; try{ws.ping();}catch{}
    }
    if(dead>0) console.log('[%s] cleaned %d dead. active=%d',label,dead,clients.size);
  },15000);
}

setupChannel(mcxWSS,mcxClients,'MCX-FEED',()=>lastMCX);
setupChannel(fxWSS, fxClients, 'FX-FEED', ()=>lastFX);

// ── ISOLATED broadcast functions ──────────────────────────────────
function broadcastMCX() {
  mcxBC++;
  const msg=JSON.stringify(buildMCXPayload()); lastMCX=msg;
  if(!mcxClients.size) return;
  const buf=Buffer.from(msg);
  for(const ws of mcxClients){if(ws.readyState===WebSocket.OPEN){try{ws.send(buf);}catch{mcxClients.delete(ws);}}}
}

function broadcastFX() {
  fxBC++;
  const msg=JSON.stringify(buildFXPayload()); lastFX=msg;
  if(!fxClients.size) return;
  const buf=Buffer.from(msg);
  for(const ws of fxClients){if(ws.readyState===WebSocket.OPEN){try{ws.send(buf);}catch{fxClients.delete(ws);}}}
}

// ═══════════════════════════════════════════════════════════════════
// TOKEN MANAGER — TOTP with window drift fix
// ═══════════════════════════════════════════════════════════════════
function b32decode(s) {
  const A='ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  s=s.toUpperCase().replace(/=+$/,'').replace(/\s/g,'');
  let bits=0,val=0; const out=[];
  for(const ch of s){const i=A.indexOf(ch);if(i===-1)continue;val=(val<<5)|i;bits+=5;if(bits>=8){bits-=8;out.push((val>>bits)&0xFF);}}
  return Buffer.from(out);
}

// Generate TOTP for a given 30s counter (drift: t-1, t, t+1)
function totpForCounter(secret,counter) {
  try {
    const key=b32decode(secret),tb=Buffer.alloc(8);
    tb.writeUInt32BE(Math.floor(counter/0x100000000),0);
    tb.writeUInt32BE(counter>>>0,4);
    const h=crypto.createHmac('sha1',key).update(tb).digest();
    const off=h[19]&0xF;
    const code=((h[off]&0x7F)<<24|(h[off+1]&0xFF)<<16|(h[off+2]&0xFF)<<8|(h[off+3]&0xFF))%1000000;
    return String(code).padStart(6,'0');
  } catch{return null;}
}

function getTOTPCodes(secret) {
  const t=Math.floor(Date.now()/1000/30);
  return [...new Set([0,-1,1].map(o=>totpForCounter(secret,t+o)).filter(Boolean))];
}

let currentToken   = process.env.DHAN_ACCESS_TOKEN||'';
let tokenRenewedAt = currentToken.length>100?'startup-env':null;
let tokenAppliedAt = currentToken.length>100?Date.now():0;
let renewAttempts  = 0, renewTimer = null;

function applyToken(t,src) {
  currentToken=t; tokenRenewedAt=new Date().toISOString(); tokenAppliedAt=Date.now();
  renewAttempts=0; if(renewTimer){clearTimeout(renewTimer);renewTimer=null;}
  console.log('[TOKEN] ✅ applied via %s len=%d',src,t.length);
  if(dhanWS&&dhanWS.readyState!==WebSocket.CLOSED){try{dhanWS.terminate();}catch{}}
  dhanStatus='disconnected';
  setTimeout(connectDhan,2000);
}

function checkEnvToken() {
  const env=process.env.DHAN_ACCESS_TOKEN||'';
  if(env&&env!==currentToken&&env.length>100){console.log('[TOKEN] 🔄 env token changed len=%d',env.length);applyToken(env,'env');return true;}
  return false;
}
setInterval(checkEnvToken,60*1000);

async function renewToken() {
  const cid=process.env.DHAN_CLIENT_ID||''; if(!cid){console.warn('[TOKEN] DHAN_CLIENT_ID missing');return false;}
  if(checkEnvToken()) return true;

  const pin=process.env.DHAN_PIN||'',secret=process.env.DHAN_TOTP_SECRET||'';
  if(pin&&secret) {
    const codes=getTOTPCodes(secret);
    console.log('[TOKEN] TOTP trying codes: %s',codes.join(','));
    for(const totp of codes) {
      try {
        const r=await axios.post('https://auth.dhan.co/app/generateAccessToken',{},{
          params:{dhanClientId:cid,pin,totp},
          headers:{'Content-Type':'application/json'},
          timeout:25000,
        });
        const t=r.data?.accessToken||r.data?.access_token||r.data?.data?.accessToken;
        if(t&&t.length>100){console.log('[TOKEN] ✅ TOTP success code=%s',totp);applyToken(t,'TOTP');return true;}
        console.warn('[TOKEN] code=%s no token: %s',totp,JSON.stringify(r.data).slice(0,100));
      } catch(e) {
        const s=e.response?.status;
        console.warn('[TOKEN] code=%s HTTP %s — %s',totp,s||'timeout',e.message.slice(0,80));
        if(s===429||s===400||s===401) break;
        await new Promise(r=>setTimeout(r,2000));
      }
    }
  } else {
    if(!pin)    console.warn('[TOKEN] ⚠️  DHAN_PIN not set');
    if(!secret) console.warn('[TOKEN] ⚠️  DHAN_TOTP_SECRET not set');
  }

  if(currentToken) {
    try {
      const r=await axios.post(DHAN_BASE+'/RenewToken',{},{
        headers:{'access-token':currentToken,'dhanClientId':cid,'Content-Type':'application/json'},
        timeout:12000,
      });
      const t=r.data?.accessToken||r.data?.access_token||r.data?.data?.accessToken;
      if(t&&t.length>100){applyToken(t,'RenewToken');return true;}
    } catch(e) {
      const s=e.response?.status;
      if(s===400||s===401) console.warn('[TOKEN] RenewToken 401 — token expired. Update DHAN_ACCESS_TOKEN on Render.');
      else console.warn('[TOKEN] RenewToken %s — %s',s||'timeout',e.message.slice(0,60));
    }
  }
  console.warn('[TOKEN] ❌ All methods failed. Update DHAN_ACCESS_TOKEN on Render dashboard.');
  return false;
}

async function renewWithRetry() {
  const ok=await renewToken();
  if(!ok){
    renewAttempts++;
    const d=Math.min(renewAttempts*2*60*1000,15*60*1000);
    console.warn('[TOKEN] retry #%d in %dm',renewAttempts,(d/60000).toFixed(0));
    renewTimer=setTimeout(renewWithRetry,d);
  }
}

function msUntilIST(hh,mm) {
  const now=Date.now(),ist=new Date(now+5.5*3600000);
  const y=ist.getUTCFullYear(),mo=ist.getUTCMonth(),d=ist.getUTCDate();
  const um=hh*60+mm-330,uh=Math.floor(um/60),uM=((um%60)+60)%60;
  let t=new Date(Date.UTC(y,mo,d,(uh+24)%24,uM,0,0));
  if(t.getTime()<=now) t.setUTCDate(t.getUTCDate()+1);
  return t.getTime()-now;
}

// ═══════════════════════════════════════════════════════════════════
// DHAN ADAPTER — calls broadcastMCX() ONLY
// ═══════════════════════════════════════════════════════════════════
let dhanWS=null,dhanStatus='disconnected',dhanReconnects=0,dhanPackets=0,dhanLastTickAt=0;
let dhanReconnTimer=null,dhanPingTimer=null,dhanBackoff=1000,dhanLastConnAt=null;

function parseDhan(buf) {
  try {
    if(!buf||buf.length<8) return null;
    const fc=buf.readUInt8(0),secId=buf.readInt32LE(4).toString();
    if(fc===50) return null;
    if(fc===6&&buf.length>=16){const pc=buf.readFloatLE(8);return isFinite(pc)&&pc>0?{type:'pc',secId,pc:Math.round(pc)}:null;}
    if(fc===2&&buf.length>=16){const l=buf.readFloatLE(8);return isFinite(l)&&l>100?{type:'t',secId,ltp:Math.round(l)}:null;}
    if(fc===4&&buf.length>=50){
      const l=buf.readFloatLE(8);if(!isFinite(l)||l<=100) return null;
      return {type:'q',secId,ltp:Math.round(l),open:Math.round(buf.readFloatLE(34))||0,high:Math.round(buf.readFloatLE(42))||0,low:Math.round(buf.readFloatLE(46))||0};
    }
    if(fc===8&&buf.length>=62){
      const l=buf.readFloatLE(8);if(!isFinite(l)||l<=100) return null;
      let b=Math.round(l),a=Math.round(l);
      if(buf.length>=82){const bf=buf.readFloatLE(74),af=buf.readFloatLE(78);if(isFinite(bf)&&bf>100)b=Math.round(bf);if(isFinite(af)&&af>100)a=Math.round(af);}
      return {type:'f',secId,ltp:Math.round(l),bid:b,ask:a,
        open:buf.length>49?Math.round(buf.readFloatLE(46)):0,
        high:buf.length>57?Math.round(buf.readFloatLE(54)):0,
        low: buf.length>61?Math.round(buf.readFloatLE(58)):0};
    }
  } catch{}
  return null;
}

function dhanSubscribe(ws) {
  const insts=Object.keys(TOKEN_MAP).map(s=>({ExchangeSegment:'MCX_COMM',SecurityId:s}));
  const send=(obj,ms)=>setTimeout(()=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj));},ms);
  send({RequestCode:15,InstrumentCount:insts.length,InstrumentList:insts},0);
  send({RequestCode:17,InstrumentCount:insts.length,InstrumentList:insts},300);
  console.log('[DHAN] subscribed %d insts RC15+RC17',insts.length);
}

function connectDhan() {
  if(!currentToken||!process.env.DHAN_CLIENT_ID){console.warn('[DHAN] no creds — retry 10s');setTimeout(connectDhan,10000);return;}
  if(dhanStatus==='connecting'||dhanStatus==='connected') return;
  dhanStatus='connecting'; dhanPackets=0; dhanLastConnAt=new Date().toISOString();
  const url=`wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(currentToken)}&clientId=${encodeURIComponent(process.env.DHAN_CLIENT_ID)}&authType=2`;
  const ws=new WebSocket(url,{handshakeTimeout:15000}); dhanWS=ws;

  ws.on('open',()=>{
    dhanStatus='connected';dhanReconnects=0;dhanBackoff=1000;dhanLastTickAt=Date.now();
    console.log('[DHAN] ✅ connected tokenLen=%d',currentToken.length);
    dhanSubscribe(ws);
    if(dhanPingTimer) clearInterval(dhanPingTimer);
    dhanPingTimer=setInterval(()=>{if(ws.readyState===WebSocket.OPEN)ws.ping();},20000);
  });

  ws.on('message',data=>{
    if(typeof data==='string') return;
    const buf=Buffer.isBuffer(data)?data:Buffer.from(data); dhanPackets++;
    const tick=parseDhan(buf);if(!tick) return;
    dhanLastTickAt=Date.now();
    const key=TOKEN_MAP[tick.secId];if(!key) return;
    if(tick.type==='pc'){
      if(key==='gold')   writeMCX('gold',  {ltp:0,prevClose:tick.pc,src:'dhan_ws'});
      if(key==='silver') writeMCX('silver',{ltp:0,prevClose:tick.pc,src:'dhan_ws'});
      return;
    }
    const changed=writeMCX(key,{...tick,src:'dhan_ws'});
    if(changed) broadcastMCX(); // ← Dhan only. FCS never touched.
  });

  ws.on('pong',()=>{dhanLastTickAt=Date.now();});
  ws.on('close',code=>{
    dhanStatus='disconnected';
    if(dhanPingTimer){clearInterval(dhanPingTimer);dhanPingTimer=null;}
    console.warn('[DHAN] closed code=%d pkts=%d',code,dhanPackets);
    if(code===1008){console.warn('[DHAN] auth fail → renew');renewWithRetry();}
    else if(dhanReconnects>=5){console.warn('[DHAN] %d reconnects → renew',dhanReconnects);renewWithRetry();}
    dhanScheduleReconn();
  });
  ws.on('error',e=>console.warn('[DHAN] err:',e.message));
}

function dhanScheduleReconn() {
  if(dhanReconnTimer) return;
  dhanReconnects++;
  const delay=Math.min(dhanBackoff+Math.random()*1000,30000);
  dhanBackoff=Math.min(dhanBackoff*2,30000);
  dhanReconnTimer=setTimeout(()=>{dhanReconnTimer=null;connectDhan();},delay);
  console.log('[DHAN] reconnect #%d in %ds',dhanReconnects,(delay/1000).toFixed(1));
}

setInterval(()=>{
  if(dhanStatus==='disconnected'&&!dhanReconnTimer){connectDhan();return;}
  if(dhanStatus==='connected'&&dhanLastTickAt>0&&Date.now()-dhanLastTickAt>45000){
    console.warn('[DHAN] stale 45s — reconnecting');
    try{dhanWS.terminate();}catch{}
    dhanStatus='disconnected';dhanScheduleReconn();
  }
},10000);

// ═══════════════════════════════════════════════════════════════════
// FCS ADAPTER — calls broadcastFX() ONLY
// ═══════════════════════════════════════════════════════════════════
let fcsWS=null,fcsStatus='disconnected',fcsReconn=0,fcsPkts=0;
let fcsReconnTimer=null,fcsPingTimer=null,fcsBackoff=2000;

function connectFCS() {
  const key=process.env.FCS_API_KEY||'';if(!key) return;
  if(fcsStatus==='connecting'||fcsStatus==='connected') return;
  fcsStatus='connecting';
  const ws=new WebSocket(`wss://ws-v4.fcsapi.com/ws?access_key=${key}`,{handshakeTimeout:15000}); fcsWS=ws;

  ws.on('open',()=>{
    fcsStatus='connected';fcsReconn=0;fcsBackoff=2000;
    console.log('[FCS] ✅ connected');
    ['FX:XAUUSD','FX:XAGUSD','FX:USDINR'].forEach(sym=>{
      ws.send(JSON.stringify({type:'join_symbol',symbol:sym,timeframe:'0'}));
    });
    if(fcsPingTimer) clearInterval(fcsPingTimer);
    fcsPingTimer=setInterval(()=>{if(ws.readyState===WebSocket.OPEN)ws.ping();},25000);
  });

  ws.on('message',raw=>{
    let msg;try{msg=JSON.parse(raw);}catch{return;}
    if(!msg||msg.type!=='price'||!msg.prices) return;
    fcsPkts++;
    const p=msg.prices,sym=msg.symbol||'';
    // FCS v4: c=close, b=bid, a=ask, h=high, l=low
    const price=p.c||p.close||0,bid=p.b||p.bid||0,ask=p.a||p.ask||0;
    const high=p.h||p.high||0,low=p.l||p.low||0;
    let changed=false;
    if(sym==='FX:XAUUSD')      changed=writeFX('XAU',{price,bid,ask,high,low,src:'fcs_ws'});
    else if(sym==='FX:XAGUSD') changed=writeFX('XAG',{price,bid,ask,high,low,src:'fcs_ws'});
    else if(sym==='FX:USDINR') changed=writeFX('INR',{price,bid,ask,high,low,src:'fcs_ws'});
    if(changed) broadcastFX(); // ← FCS only. Dhan never touched.
  });

  ws.on('close',code=>{
    fcsStatus='disconnected';
    if(fcsPingTimer){clearInterval(fcsPingTimer);fcsPingTimer=null;}
    fcsReconn++;
    const d=Math.min(fcsBackoff+Math.random()*1000,60000);
    fcsBackoff=Math.min(fcsBackoff*2,60000);
    console.warn('[FCS] closed code=%d reconnect in %ds',code,(d/1000).toFixed(1));
    fcsReconnTimer=setTimeout(()=>{fcsReconnTimer=null;connectFCS();},d);
  });
  ws.on('error',e=>console.warn('[FCS] err:',e.message));
}
setInterval(()=>{if(process.env.FCS_API_KEY&&fcsStatus==='disconnected'&&!fcsReconnTimer)connectFCS();},30000);

// ═══════════════════════════════════════════════════════════════════
// TD ADAPTER + REST POLLS — all call broadcastFX()
// ═══════════════════════════════════════════════════════════════════
let tdWS=null,tdStatus='disconnected',tdPkts=0,tdReconn=0,tdReconnTimer=null,tdPingTimer=null;

function connectTD() {
  const key=process.env.TWELVE_DATA_KEY||'';if(!key) return;
  if(tdStatus==='connecting'||tdStatus==='connected') return;
  tdStatus='connecting';
  const ws=new WebSocket(`wss://ws.twelvedata.com/v1/quotes/price?apikey=${key}`,{handshakeTimeout:15000}); tdWS=ws;
  ws.on('open',()=>{
    tdStatus='connected';tdReconn=0;
    ws.send(JSON.stringify({action:'subscribe',params:{symbols:'XAU/USD'}}));
    if(tdPingTimer) clearInterval(tdPingTimer);
    tdPingTimer=setInterval(()=>{if(ws.readyState===WebSocket.OPEN)ws.ping();},20000);
  });
  ws.on('message',data=>{
    let msg;try{msg=JSON.parse(data);}catch{return;}
    if(msg.event==='heartbeat') return;
    if(msg.event==='price'&&msg.symbol==='XAU/USD'&&msg.price){
      const p=parseFloat(msg.price);
      if(p>3000&&p<9000){tdPkts++;const ch=writeFX('XAU',{price:p,bid:parseFloat(msg.bid||p),ask:parseFloat(msg.ask||p),src:'td_ws'});if(ch)broadcastFX();}
    }
  });
  ws.on('close',()=>{
    tdStatus='disconnected';if(tdPingTimer){clearInterval(tdPingTimer);tdPingTimer=null;}
    tdReconn++;const d=Math.min(3000*Math.pow(2,Math.min(tdReconn-1,4)),30000);
    tdReconnTimer=setTimeout(()=>{tdReconnTimer=null;connectTD();},d);
  });
  ws.on('error',e=>console.warn('[TD] err:',e.message));
}
setInterval(()=>{if(process.env.TWELVE_DATA_KEY&&tdStatus==='disconnected'&&!tdReconnTimer)connectTD();},30000);

async function pollXAG() {
  const key=process.env.TWELVE_DATA_KEY||'';
  const srcs=[
    async()=>{if(!key)throw 0;const r=await axios.get('https://api.twelvedata.com/price',{params:{symbol:'XAG/USD',apikey:key},timeout:7000});return parseFloat(r.data?.price);},
    async()=>{const r=await axios.get('https://open.er-api.com/v6/latest/XAG',{timeout:6000});return r.data?.rates?.USD;},
    async()=>{const r=await axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/xag.json',{timeout:7000});return r.data?.xag?.usd;},
  ];
  for(const fn of srcs){try{const p=await fn();if(p>20&&p<300){writeFX('XAG',{price:p,src:'rest'});broadcastFX();return;}}catch{}}
}

async function pollINR() {
  const srcs=[
    async()=>{const r=await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000});return r.data.rates.INR;},
    async()=>{const r=await axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000});return r.data.rates.INR;},
    async()=>{const r=await axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json',{timeout:5000});return r.data.usd.inr;},
  ];
  for(const fn of srcs){try{const v=await fn();if(v>70&&v<115){writeFX('INR',{price:v,bid:r2(v-0.03),ask:r2(v+0.03),src:'rest'});broadcastFX();return;}}catch{}}
}

async function pollDailyHL() {
  const key=process.env.TWELVE_DATA_KEY||'';if(!key) return;
  try {
    const r=await axios.get('https://api.twelvedata.com/quote',{params:{symbol:'XAU/USD,XAG/USD,USD/INR',apikey:key},timeout:12000});
    const d=r.data,xau=d?.['XAU/USD']||d,xag=d?.['XAG/USD'],inr=d?.['USD/INR'];
    if(xau?.high&&xau?.low){const h=parseFloat(xau.high),l=parseFloat(xau.low);if(h>3000){S.xauHigh=r2(h);S.xauLow=r2(l);}}
    if(xag?.high&&xag?.low){const h=parseFloat(xag.high),l=parseFloat(xag.low);if(h>20){S.xagHigh=r3(h);S.xagLow=r3(l);}}
    if(inr?.high&&inr?.low){const h=parseFloat(inr.high),l=parseFloat(inr.low);if(h>70){S.usdInrHigh=r2(h);S.usdInrLow=r2(l);}}
    broadcastFX();console.log('[TD-QUOTE] H/L updated');
  } catch(e){console.warn('[TD-QUOTE] fail:',e.message.slice(0,60));}
}

let ohlcCalls=0,ohlcErr=null,ohlcBO=0;
async function pollOHLC() {
  const token=currentToken,cid=process.env.DHAN_CLIENT_ID||'';
  if(!token||!cid||Date.now()<ohlcBO) return;
  if(dhanStatus==='connected'&&dhanLastTickAt>0&&Date.now()-dhanLastTickAt<4000) return;
  const secIds=[AC.gold?.secId,AC.goldNext?.secId,AC.silver?.secId,AC.silverNext?.secId].filter(Boolean).map(Number);
  try {
    const r=await axios.post(DHAN_BASE+'/marketfeed/ohlc',{MCX_COMM:secIds},{
      headers:{Accept:'application/json','Content-Type':'application/json','access-token':token,'client-id':cid},timeout:5000});
    const seg=r.data?.data?.MCX_COMM;if(!seg){ohlcErr='no MCX_COMM';return;}
    ohlcCalls++;ohlcErr=null;
    const applyRow=(sid,key)=>{const row=seg[String(sid)];if(!row)return;const l=row.last_price||0,o=row.ohlc||{};if(l>0)writeMCX(key,{ltp:Math.round(l),open:Math.round(o.open||0),high:Math.round(o.high||0),low:Math.round(o.low||0),src:'dhan_ohlc'});};
    applyRow(AC.gold?.secId,'gold');applyRow(AC.goldNext?.secId,'goldNext');
    applyRow(AC.silver?.secId,'silver');applyRow(AC.silverNext?.secId,'silverNext');
    broadcastMCX();
  } catch(e) {
    ohlcErr=e.message;
    if(e.response?.status===429){ohlcBO=Date.now()+60000;console.warn('[OHLC] 429 backoff 60s');}
    if(e.response?.status===401){console.warn('[OHLC] 401 → renew token');ohlcBO=Date.now()+30000;if(renewAttempts===0&&!renewTimer)renewWithRetry();}
  }
}

// ═══════════════════════════════════════════════════════════════════
// HTTP ROUTES
// ═══════════════════════════════════════════════════════════════════
app.get('/rates',(req,res)=>res.json(buildFullPayload()));
app.get('/debug',(req,res)=>res.json({
  server:'RR Jewellers v16',uptime:Math.round(process.uptime())+'s',
  feed:{mcx:{clients:mcxClients.size,broadcasts:mcxBC},fx:{clients:fxClients.size,broadcasts:fxBC}},
  dhan:{status:dhanStatus,packets:dhanPackets,reconnects:dhanReconnects,tickAgeMs:dhanLastTickAt?Date.now()-dhanLastTickAt:null,lastConnAt:dhanLastConnAt},
  fcs:{status:fcsStatus,packets:fcsPkts,reconnects:fcsReconn,hasKey:!!process.env.FCS_API_KEY},
  td:{status:tdStatus,packets:tdPkts,reconnects:tdReconn,hasKey:!!process.env.TWELVE_DATA_KEY},
  ohlc:{calls:ohlcCalls,lastError:ohlcErr,backoffUntil:ohlcBO>Date.now()?new Date(ohlcBO).toISOString():null},
  token:{renewedAt:tokenRenewedAt,ageMs:tokenAppliedAt?Date.now()-tokenAppliedAt:null,renewAttempts,len:currentToken.length,hasPin:!!process.env.DHAN_PIN,hasTotp:!!process.env.DHAN_TOTP_SECRET},
  rateCache:S,contracts:AC,marketOpen:isMCXOpen(),
}));
app.get('/ping',(req,res)=>res.json({ok:true,ts:Date.now(),mcxClients:mcxClients.size,fxClients:fxClients.size,dhan:dhanStatus}));
app.get('/health',(req,res)=>{const ok=dhanStatus==='connected'||fcsStatus==='connected';res.status(ok?200:503).json({ok,dhan:dhanStatus,fcs:fcsStatus});});
app.get('/token-renew',async(req,res)=>{await renewWithRetry();res.json({tokenRenewedAt,len:currentToken.length,dhan:dhanStatus});});
app.get('/updates',async(req,res)=>{
  try{
    if(!SHEET_ID) throw new Error('no SHEET_ID');
    const r=await axios.get(`https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`,{timeout:8000});
    const data=JSON.parse(r.data.replace(/.*?({.*}).*/s,'$1'));
    res.json({success:true,updates:data.table.rows.map(row=>({date:row.c[0]?.v||'',title:row.c[1]?.v||'',content:row.c[2]?.v||'',image:row.c[3]?.v||''})).filter(r=>r.title)});
  }catch{res.json({success:true,updates:[{date:'Today',title:'Welcome to R.R. Jewellers',content:'Indicative gold & silver rates.',image:''}]});}
});
app.get('/',(req,res)=>res.json({status:'RR Jewellers v16',dhan:dhanStatus,fcs:fcsStatus,td:tdStatus,mcxClients:mcxClients.size,fxClients:fxClients.size}));

// ═══════════════════════════════════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════════════════════════════════
server.listen(PORT,'0.0.0.0',async()=>{
  console.log('[START] RR Jewellers v16 port=%s',PORT);
  console.log('[START] token len=%d renewedAt=%s',currentToken.length,tokenRenewedAt);

  if(currentToken.length>100){
    console.log('[START] ✅ token valid — connecting Dhan directly');
    connectDhan();
  } else {
    console.warn('[START] ⚠️  no token — renewWithRetry');
    await renewWithRetry();
    connectDhan();
  }

  connectFCS(); connectTD();

  pollXAG();   setInterval(pollXAG,    3*60*1000);
  pollINR();   setInterval(pollINR,    5*60*1000);
  pollDailyHL(); setInterval(pollDailyHL,15*60*1000);
  setInterval(()=>{if(isMCXOpen())pollOHLC();},5000);

  // Periodic snapshots — so new clients get data even in quiet markets
  setInterval(broadcastMCX,3000);
  setInterval(broadcastFX, 5000);

  const sched=(hh,mm,fn)=>{const ms=msUntilIST(hh,mm);console.log('[SCHED] %d:%02d IST in %dm',hh,mm,(ms/60000).toFixed(0));setTimeout(()=>{fn();sched(hh,mm,fn);},ms);};
  sched(8,30,()=>{console.log('[TOKEN] 8:30AM IST renew');renewWithRetry();});
  sched(9,0, ()=>{resetHL();broadcastFX();});

  if(SELF_URL){setInterval(()=>{axios.get(SELF_URL+'/ping').catch(()=>{});},4*60*1000);console.log('[START] self-ping:',SELF_URL);}
  console.log('[START] ✅ /feed/mcx + /feed/fx ready on port %s',PORT);
});
