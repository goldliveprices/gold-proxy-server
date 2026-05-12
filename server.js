'use strict';
// RR Jewellers v14 — Single File Production
// All modules inline. Deploy only this file to GitHub.
// Required npm: express, ws, axios

const express   = require('express');
const http      = require('http');
const axios     = require('axios');
const WebSocket = require('ws');
const crypto    = require('crypto');

const app    = express();
const server = http.createServer(app);
app.use(express.json());
app.use((req, res, next) => { res.setHeader('Access-Control-Allow-Origin', '*'); next(); });

// ── ENV ──────────────────────────────────────────────────────
const PORT     = process.env.PORT     || 3000;
const SELF_URL = process.env.SELF_URL || '';
const SHEET_ID = process.env.SHEET_ID || '';
const DHAN_BASE = 'https://api.dhan.co/v2';

function goldPct()   { return parseFloat(process.env.GOLD_MARGIN_PCT   || '0'); }
function silverPct() { return parseFloat(process.env.SILVER_MARGIN_PCT || '0'); }

// ═══════════════════════════════════════════════════
// CACHE ENGINE — single source of truth
// ═══════════════════════════════════════════════════
const S = {
  goldLtp:0,goldBid:0,goldAsk:0,goldHigh:0,goldLow:0,goldOpen:0,goldPrevClose:0,
  goldNextLtp:0,goldNextBid:0,goldNextAsk:0,goldNextHigh:0,goldNextLow:0,
  silverLtp:0,silverBid:0,silverAsk:0,silverHigh:0,silverLow:0,silverOpen:0,silverPrevClose:0,
  silverNextLtp:0,silverNextBid:0,silverNextAsk:0,silverNextHigh:0,silverNextLow:0,
  xauUsd:0,xauBid:0,xauAsk:0,xauHigh:0,xauLow:0,
  xagUsd:0,xagBid:0,xagAsk:0,xagHigh:0,xagLow:0,
  usdInr:0,usdInrBid:0,usdInrAsk:0,usdInrHigh:0,usdInrLow:Infinity,
  mcxSrc:'init',fxSrc:'init',
  mcxAt:0,fxAt:0,xauAt:0,xagAt:0,inrAt:0,
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
    S.xauAt = now;
  } else if (sym === 'XAG' && d.price > 20 && d.price < 300) {
    if (d.price !== S.xagUsd) { S.xagUsd = r3(d.price); changed = true; }
    if (d.bid  > 0) S.xagBid  = r3(d.bid);
    if (d.ask  > 0) S.xagAsk  = r3(d.ask);
    if (d.high > 0) S.xagHigh = r3(d.high);
    if (d.low  > 0) S.xagLow  = r3(d.low);
    S.xagAt = now;
  } else if (sym === 'INR' && d.price > 70 && d.price < 115) {
    if (d.price !== S.usdInr) { S.usdInr = r2(d.price); changed = true; }
    if (d.bid  > 0) S.usdInrBid  = r2(d.bid);
    if (d.ask  > 0) S.usdInrAsk  = r2(d.ask);
    if (d.high > 0) S.usdInrHigh = r2(d.high);
    if (d.low  > 0 && d.low < S.usdInrLow) S.usdInrLow = r2(d.low);
    S.inrAt = now;
  }
  if (changed) { S.fxSrc = d.src || 'api'; S.fxAt = now; }
  return changed;
}

function resetHL() {
  S.xauHigh=0;S.xauLow=0;S.xagHigh=0;S.xagLow=0;
  S.usdInrHigh=0;S.usdInrLow=Infinity;
  S.goldHigh=0;S.goldLow=0;S.silverHigh=0;S.silverLow=0;
  S.goldNextHigh=0;S.goldNextLow=0;S.silverNextHigh=0;S.silverNextLow=0;
  console.log('[CACHE] Daily H/L reset');
}

function r2(v){ return Math.round(v*100)/100; }
function r3(v){ return Math.round(v*1000)/1000; }

// ═══════════════════════════════════════════════════
// TICK ENGINE — contracts + payload builder
// ═══════════════════════════════════════════════════
const GOLD_C = [
  {secId:'459277',display:'GOLD JUN26',expiry:'2026-06-05'},
  {secId:'466583',display:'GOLD AUG26',expiry:'2026-08-05'},
  {secId:'483079',display:'GOLD OCT26',expiry:'2026-10-05'},
  {secId:'495213',display:'GOLD DEC26',expiry:'2026-12-04'},
  {secId:'559933',display:'GOLD FEB27',expiry:'2027-02-05'},
];
const SILV_C = [
  {secId:'464150',display:'SILVER JUL26',expiry:'2026-07-03'},
  {secId:'471725',display:'SILVER SEP26',expiry:'2026-09-04'},
  {secId:'495214',display:'SILVER DEC26',expiry:'2026-12-04'},
  {secId:'564619',display:'SILVER MAR27',expiry:'2027-03-05'},
];

let AC = {};
let TOKEN_MAP = {};

function pickCN(list) {
  const now = new Date();
  const sorted = list.map(c=>({...c,ed:new Date(c.expiry)})).filter(c=>!isNaN(c.ed)).sort((a,b)=>a.ed-b.ed);
  const up = sorted.filter(c=>c.ed>=now);
  if (up.length>=2) return {cur:up[0],nxt:up[1]};
  const last=sorted.slice(-2);
  return {cur:last[0]||sorted[0],nxt:last[1]||sorted[0]};
}

function refreshAC() {
  const g=pickCN(GOLD_C), s=pickCN(SILV_C);
  AC = {gold:g.cur,goldNext:g.nxt,silver:s.cur,silverNext:s.nxt};
  TOKEN_MAP = {
    [g.cur.secId]:'gold',[g.nxt.secId]:'goldNext',
    [s.cur.secId]:'silver',[s.nxt.secId]:'silverNext',
  };
}
refreshAC();
setInterval(refreshAC, 6*3600*1000);

function isMCXOpen() {
  const d=new Date(Date.now()+5.5*3600000);
  const dow=d.getUTCDay(), t=d.getUTCHours()*60+d.getUTCMinutes();
  if(dow===0) return false;
  return dow===6?(t>=540&&t<840):(t>=540&&t<1435);
}

function nn(v){ return (v&&v>0)?v:null; }

function buildPayload() {
  const gm=goldPct(), sm=silverPct();
  const gSell = S.goldLtp>0   ? Math.round(S.goldLtp  *(1+gm/100)) : null;
  const sSell = S.silverLtp>0 ? Math.round(S.silverLtp*(1+sm/100)) : null;
  return {
    ts:Date.now(), src:S.mcxSrc, fxSrc:S.fxSrc,
    mktOpen:isMCXOpen(),
    goldSell:gSell, silverSell:sSell,
    // Legacy compat
    success:true, source:S.mcxSrc,
    goldPer10g:gSell, silverPerKg:sSell,
    xauUsd:nn(S.xauUsd), xagUsd:nn(S.xagUsd), usdInr:nn(S.usdInr),
    f:{
      g: {ltp:nn(S.goldLtp),      bid:nn(S.goldBid),      ask:nn(S.goldAsk),
          high:nn(S.goldHigh),    low:nn(S.goldLow),      open:nn(S.goldOpen),
          pc:nn(S.goldPrevClose), con:AC.gold.display,    exp:AC.gold.expiry},
      gN:{ltp:nn(S.goldNextLtp),  bid:nn(S.goldNextBid),  ask:nn(S.goldNextAsk),
          high:nn(S.goldNextHigh),low:nn(S.goldNextLow),  con:AC.goldNext.display, exp:AC.goldNext.expiry},
      s: {ltp:nn(S.silverLtp),    bid:nn(S.silverBid),    ask:nn(S.silverAsk),
          high:nn(S.silverHigh),  low:nn(S.silverLow),    open:nn(S.silverOpen),
          pc:nn(S.silverPrevClose),con:AC.silver.display,  exp:AC.silver.expiry},
      sN:{ltp:nn(S.silverNextLtp),bid:nn(S.silverNextBid),ask:nn(S.silverNextAsk),
          high:nn(S.silverNextHigh),low:nn(S.silverNextLow),con:AC.silverNext.display,exp:AC.silverNext.expiry},
    },
    sp:{
      xauUsd:nn(S.xauUsd),xauBid:nn(S.xauBid),xauAsk:nn(S.xauAsk),xauHigh:nn(S.xauHigh),xauLow:nn(S.xauLow),
      xagUsd:nn(S.xagUsd),xagBid:nn(S.xagBid),xagAsk:nn(S.xagAsk),xagHigh:nn(S.xagHigh),xagLow:nn(S.xagLow),
      usdInr:nn(S.usdInr),usdInrBid:nn(S.usdInrBid),usdInrAsk:nn(S.usdInrAsk),
      usdInrHigh:nn(S.usdInrHigh),usdInrLow:S.usdInrLow===Infinity?null:nn(S.usdInrLow),
    },
    // Also flat spot for legacy HTML
    spot:{
      xauUsd:nn(S.xauUsd),xauBid:nn(S.xauBid),xauAsk:nn(S.xauAsk),xauHigh:nn(S.xauHigh),xauLow:nn(S.xauLow),
      xagUsd:nn(S.xagUsd),xagBid:nn(S.xagBid),xagAsk:nn(S.xagAsk),xagHigh:nn(S.xagHigh),xagLow:nn(S.xagLow),
      usdInr:nn(S.usdInr),usdInrBid:nn(S.usdInrBid),usdInrAsk:nn(S.usdInrAsk),
      usdInrHigh:nn(S.usdInrHigh),usdInrLow:S.usdInrLow===Infinity?null:nn(S.usdInrLow),
    },
    futures:{
      gold:      {ltp:nn(S.goldLtp),      bid:nn(S.goldBid),      ask:nn(S.goldAsk),      high:nn(S.goldHigh),    low:nn(S.goldLow),    open:nn(S.goldOpen)},
      silver:    {ltp:nn(S.silverLtp),    bid:nn(S.silverBid),    ask:nn(S.silverAsk),    high:nn(S.silverHigh),  low:nn(S.silverLow),  open:nn(S.silverOpen)},
      goldNext:  {ltp:nn(S.goldNextLtp),  bid:nn(S.goldNextBid),  ask:nn(S.goldNextAsk),  high:nn(S.goldNextHigh),low:nn(S.goldNextLow)},
      silverNext:{ltp:nn(S.silverNextLtp),bid:nn(S.silverNextBid),ask:nn(S.silverNextAsk),high:nn(S.silverNextHigh),low:nn(S.silverNextLow)},
    },
    margin:{g:gm,s:sm},
    rateCache:S, contracts:AC, tokenMap:TOKEN_MAP,
  };
}

// ═══════════════════════════════════════════════════
// FEED SERVER — HTML WebSocket push (/feed)
// Uses noServer+upgrade for Render compatibility
// ═══════════════════════════════════════════════════
const feedWSS = new WebSocket.Server({ noServer:true, perMessageDeflate:false });
const feedClients = new Set();
let lastPayload = null;
let broadcastCount = 0;

// Handle HTTP→WS upgrade manually (works on Render reverse proxy)
server.on('upgrade', (req, socket, head) => {
  const url = req.url || '';
  if (url === '/feed' || url.startsWith('/feed?') || url === '/') {
    feedWSS.handleUpgrade(req, socket, head, (ws) => {
      feedWSS.emit('connection', ws, req);
    });
  } else {
    socket.destroy();
  }
});

feedWSS.on('connection', (ws) => {
  feedClients.add(ws);
  ws.isAlive = true;
  console.log('[FEED] Client connected. total=%d', feedClients.size);
  // Send snapshot immediately
  try {
    ws.send(lastPayload || JSON.stringify({ts:Date.now(),src:'init',mktOpen:false,success:true}));
  } catch(e) {}
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('close', () => { feedClients.delete(ws); console.log('[FEED] Client left. total=%d', feedClients.size); });
  ws.on('error', () => feedClients.delete(ws));
  ws.ping();
});

// Heartbeat every 15s
setInterval(() => {
  for (const ws of feedClients) {
    if (!ws.isAlive) { ws.terminate(); feedClients.delete(ws); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch(e) {}
  }
}, 15000);

function broadcast() {
  broadcastCount++;
  const msg = JSON.stringify(buildPayload());
  lastPayload = msg;
  if (feedClients.size === 0) return;
  const buf = Buffer.from(msg);
  for (const ws of feedClients) {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(buf); } catch(e) { feedClients.delete(ws); }
    }
  }
}

// ═══════════════════════════════════════════════════
// TOKEN MANAGER — pure Node.js TOTP, no speakeasy
// ═══════════════════════════════════════════════════
function b32decode(s) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  s = s.toUpperCase().replace(/=+$/,'').replace(/\s/g,'');
  let bits=0,val=0; const out=[];
  for (const ch of s) {
    const i=alpha.indexOf(ch); if(i===-1) continue;
    val=(val<<5)|i; bits+=5;
    if(bits>=8){bits-=8;out.push((val>>bits)&0xFF);}
  }
  return Buffer.from(out);
}

function getTOTP(secret) {
  try {
    const key=b32decode(secret);
    const t=Math.floor(Date.now()/1000/30);
    const tb=Buffer.alloc(8);
    tb.writeUInt32BE(Math.floor(t/0x100000000),0);
    tb.writeUInt32BE(t>>>0,4);
    const hmac=crypto.createHmac('sha1',key).update(tb).digest();
    const off=hmac[19]&0xF;
    const code=((hmac[off]&0x7F)<<24|(hmac[off+1]&0xFF)<<16|(hmac[off+2]&0xFF)<<8|(hmac[off+3]&0xFF))%1000000;
    return String(code).padStart(6,'0');
  } catch(e) { console.warn('[TOKEN] TOTP error:',e.message); return null; }
}

let currentToken = process.env.DHAN_ACCESS_TOKEN || '';
let tokenRenewedAt = null;
let renewAttempts = 0;
let renewTimer = null;

function applyToken(t, src) {
  currentToken = t;
  tokenRenewedAt = new Date().toISOString();
  renewAttempts = 0;
  if (renewTimer) { clearTimeout(renewTimer); renewTimer=null; }
  console.log('[TOKEN] Applied via %s len=%d at %s', src, t.length, tokenRenewedAt);
  // Reconnect Dhan with new token
  if (dhanWS && dhanWS.readyState !== WebSocket.CLOSED) {
    try { dhanWS.terminate(); } catch(e) {}
  }
  dhanStatus = 'disconnected';
  setTimeout(connectDhan, 2000);
}

async function renewToken() {
  const cid = process.env.DHAN_CLIENT_ID||'';  if (!cid) return false;

  // Method 0: env var updated on Render dashboard
  const env = process.env.DHAN_ACCESS_TOKEN||'';  if (env && env !== currentToken && env.length > 100) {
    console.log('[TOKEN] New env var detected, applying');
    applyToken(env, 'env-update'); return true;
  }

  // Method 1: TOTP — try up to 3 times (TOTP changes every 30s, retry gets fresh code)
  const pin=process.env.DHAN_PIN||'', secret=process.env.DHAN_TOTP_SECRET||'';
  if (pin && secret) {
    for (let attempt=0; attempt<3; attempt++) {
      // Generate fresh TOTP on each attempt (window may have shifted)
      const totp = getTOTP(secret);
      if (!totp) { console.warn('[TOKEN] TOTP generation failed'); break; }
      console.log('[TOKEN] TOTP attempt %d code=%s', attempt+1, totp);
      try {
        const r = await axios.post(
          'https://auth.dhan.co/app/generateAccessToken',
          {},
          {
            params: { dhanClientId:cid, pin, totp },
            headers: { 'Content-Type':'application/json' },
            timeout: 20000, // 20s — Dhan endpoint can be slow
          }
        );
        const t = r.data?.accessToken||r.data?.access_token||r.data?.data?.accessToken;
        if (t && t.length > 100) {
          console.log('[TOKEN] TOTP success attempt=%d', attempt+1);
          applyToken(t, 'TOTP'); return true;
        }
        console.warn('[TOKEN] TOTP attempt %d no token:', attempt+1, JSON.stringify(r.data).slice(0,100));
      } catch(e) {
        const s = e.response?.status;
        console.warn('[TOKEN] TOTP attempt %d fail: status=%s msg=%s', attempt+1, s, e.message.slice(0,60));
        if (s === 429) { console.warn('[TOKEN] Rate limited — stop retrying'); break; }
        if (s === 400 || s === 401) { console.warn('[TOKEN] Bad credentials — check DHAN_PIN and DHAN_TOTP_SECRET'); break; }
        // Timeout or 5xx — wait 2s and retry with fresh TOTP
        if (attempt < 2) await new Promise(r=>setTimeout(r, 2000));
      }
    }
  } else {
    if (!pin)    console.warn('[TOKEN] DHAN_PIN not set');
    if (!secret) console.warn('[TOKEN] DHAN_TOTP_SECRET not set');
  }

  // Method 2: RenewToken (works only if current token still active)
  if (currentToken) {
    console.log('[TOKEN] Trying RenewToken fallback');
    try {
      const r = await axios.post(DHAN_BASE+'/RenewToken',{},{
        headers:{'access-token':currentToken,'dhanClientId':cid,'Content-Type':'application/json'},
        timeout:12000,
      });
      const t=r.data?.accessToken||r.data?.access_token||r.data?.data?.accessToken;
      if (t&&t.length>100) { applyToken(t,'RenewToken'); return true; }
    } catch(e) {
      const s=e.response?.status;
      if (s===400||s===401) console.warn('[TOKEN] Token expired — TOTP must work for auto-renew');
      else console.warn('[TOKEN] RenewToken fail:',s,e.message.slice(0,60));
    }
  }

  console.warn('[TOKEN] All methods failed. Manual token update needed on Render.');
  return false;
}

async function renewWithRetry() {
  const ok = await renewToken();
  if (!ok) {
    renewAttempts++;
    // Retry: 2min → 5min → 10min → 15min max
    const d = Math.min(renewAttempts * 2 * 60 * 1000, 15 * 60 * 1000);
    console.warn('[TOKEN] Retry #%d in %dm', renewAttempts, (d/60000).toFixed(0));
    renewTimer = setTimeout(renewWithRetry, d);
  }
}

function msUntilIST(hh,mm) {
  const now=Date.now(), ist=new Date(now+5.5*3600000);
  const y=ist.getUTCFullYear(),mo=ist.getUTCMonth(),d=ist.getUTCDate();
  const um=hh*60+mm-330, uh=Math.floor(um/60), uM=um%60;
  let t=new Date(Date.UTC(y,mo,d,(uh+24)%24,uM,0,0));
  if (t.getTime()<=now) t.setUTCDate(t.getUTCDate()+1);
  return t.getTime()-now;
}

// ═══════════════════════════════════════════════════
// DHAN ADAPTER — RC15 fastest ticks
// ═══════════════════════════════════════════════════
let dhanWS = null;
let dhanStatus = 'disconnected';
let dhanReconnects = 0;
let dhanPackets = 0;
let dhanLastTickAt = 0;
let dhanReconnTimer = null;
let dhanPingTimer = null;
let dhanBackoff = 1000;

function parseDhan(buf) {
  try {
    if (!buf||buf.length<8) return null;
    const fc=buf.readUInt8(0), secId=buf.readInt32LE(4).toString();
    if (fc===50) return null;
    if (fc===6&&buf.length>=16) { const pc=buf.readFloatLE(8); return isFinite(pc)&&pc>0?{type:'pc',secId,pc:Math.round(pc)}:null; }
    if (fc===2&&buf.length>=16) { const l=buf.readFloatLE(8); return isFinite(l)&&l>100?{type:'t',secId,ltp:Math.round(l)}:null; }
    if (fc===4&&buf.length>=50) {
      const l=buf.readFloatLE(8); if(!isFinite(l)||l<=100) return null;
      return {type:'q',secId,ltp:Math.round(l),
        open:Math.round(buf.readFloatLE(34))||0,
        high:Math.round(buf.readFloatLE(42))||0,
        low: Math.round(buf.readFloatLE(46))||0};
    }
    if (fc===8&&buf.length>=62) {
      const l=buf.readFloatLE(8); if(!isFinite(l)||l<=100) return null;
      let b=Math.round(l),a=Math.round(l);
      if(buf.length>=82){const bf=buf.readFloatLE(74),af=buf.readFloatLE(78);if(isFinite(bf)&&bf>100)b=Math.round(bf);if(isFinite(af)&&af>100)a=Math.round(af);}
      return {type:'f',secId,ltp:Math.round(l),bid:b,ask:a,
        open:buf.length>49?Math.round(buf.readFloatLE(46)):0,
        high:buf.length>57?Math.round(buf.readFloatLE(54)):0,
        low: buf.length>61?Math.round(buf.readFloatLE(58)):0};
    }
  } catch(e) {}
  return null;
}

function dhanSubscribe(ws) {
  const insts = Object.keys(TOKEN_MAP).map(secId=>({ExchangeSegment:'MCX_COMM',SecurityId:secId}));
  const send=(obj,ms)=>setTimeout(()=>{ if(ws.readyState===WebSocket.OPEN) ws.send(JSON.stringify(obj)); },ms);
  send({RequestCode:15,InstrumentCount:insts.length,InstrumentList:insts},0);
  send({RequestCode:17,InstrumentCount:insts.length,InstrumentList:insts},300);
  console.log('[DHAN] Subscribed %d instruments',insts.length);
}

function connectDhan() {
  if (!currentToken || !process.env.DHAN_CLIENT_ID) { console.warn('[DHAN] No credentials'); return; }
  if (dhanStatus==='connecting'||dhanStatus==='connected') return;
  dhanStatus='connecting'; dhanPackets=0;
  const url=`wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(currentToken)}&clientId=${encodeURIComponent(process.env.DHAN_CLIENT_ID)}&authType=2`;
  const ws=new WebSocket(url,{handshakeTimeout:15000});
  dhanWS=ws;

  ws.on('open',()=>{
    dhanStatus='connected'; dhanReconnects=0; dhanBackoff=1000; dhanLastTickAt=Date.now();
    console.log('[DHAN] ✅ Connected');
    dhanSubscribe(ws);
    if(dhanPingTimer) clearInterval(dhanPingTimer);
    dhanPingTimer=setInterval(()=>{ if(ws.readyState===WebSocket.OPEN) ws.ping(); },20000);
  });

  ws.on('message',(data)=>{
    if(typeof data==='string') return;
    const buf=Buffer.isBuffer(data)?data:Buffer.from(data);
    dhanPackets++;
    const tick=parseDhan(buf); if(!tick) return;
    dhanLastTickAt=Date.now();
    const key=TOKEN_MAP[tick.secId]; if(!key) return;
    if(tick.type==='pc'){
      if(key==='gold')   writeMCX('gold',  {ltp:0,prevClose:tick.pc,src:'dhan_ws'});
      if(key==='silver') writeMCX('silver',{ltp:0,prevClose:tick.pc,src:'dhan_ws'});
      return;
    }
    const changed=writeMCX(key,{...tick,src:'dhan_ws'});
    if(changed) broadcast();
  });

  ws.on('pong',()=>{ dhanLastTickAt=Date.now(); });
  ws.on('close',(code)=>{
    dhanStatus='disconnected';
    if(dhanPingTimer){clearInterval(dhanPingTimer);dhanPingTimer=null;}
    console.warn('[DHAN] Closed code=%d pkts=%d',code,dhanPackets);
    if(code===1008||dhanReconnects>=5) renewWithRetry();
    dhanScheduleReconn();
  });
  ws.on('error',(e)=>console.warn('[DHAN] err:',e.message));
}

function dhanScheduleReconn() {
  if(dhanReconnTimer) return;
  dhanReconnects++;
  const jitter=Math.random()*1000;
  const delay=Math.min(dhanBackoff+jitter,30000);
  dhanBackoff=Math.min(dhanBackoff*2,30000);
  dhanReconnTimer=setTimeout(()=>{dhanReconnTimer=null;connectDhan();},delay);
  console.log('[DHAN] Reconnect #%d in %ds',dhanReconnects,(delay/1000).toFixed(1));
}

// Dhan watchdog
setInterval(()=>{
  if(dhanStatus==='disconnected'&&!dhanReconnTimer) { connectDhan(); return; }
  if(dhanStatus==='connected'&&dhanLastTickAt>0&&Date.now()-dhanLastTickAt>45000) {
    console.warn('[DHAN] Stale 45s — reconnecting');
    try{dhanWS.terminate();}catch(e){}
    dhanStatus='disconnected'; dhanScheduleReconn();
  }
},10000);

// ═══════════════════════════════════════════════════
// FCS ADAPTER — XAU/XAG/USDINR live WS
// ═══════════════════════════════════════════════════
let fcsWS=null, fcsStatus='disconnected', fcsReconn=0, fcsPkts=0;
let fcsReconnTimer=null, fcsPingTimer=null, fcsBackoff=2000;

function connectFCS() {
  const key=process.env.FCS_API_KEY||''; if(!key) return;
  if(fcsStatus==='connecting'||fcsStatus==='connected') return;
  fcsStatus='connecting';
  const ws=new WebSocket(`wss://ws-v4.fcsapi.com/ws?access_key=${key}`,{handshakeTimeout:15000});
  fcsWS=ws;
  ws.on('open',()=>{
    fcsStatus='connected'; fcsReconn=0; fcsBackoff=2000;
    console.log('[FCS] ✅ Connected');
    ['FX:XAUUSD','FX:XAGUSD','FX:USDINR'].forEach(sym=>{
      ws.send(JSON.stringify({type:'join_symbol',symbol:sym,timeframe:'0'}));
    });
    if(fcsPingTimer) clearInterval(fcsPingTimer);
    fcsPingTimer=setInterval(()=>{ if(ws.readyState===WebSocket.OPEN) ws.ping(); },25000);
  });
  ws.on('message',(raw)=>{
    let msg; try{msg=JSON.parse(raw);}catch{return;}
    if(!msg||msg.type!=='price'||!msg.prices) return;
    fcsPkts++;
    const p=msg.prices, sym=msg.symbol||'';
    const price=p.c||p.close||0, bid=p.b||p.bid||0, ask=p.a||p.ask||0;
    let changed=false;
    if(sym==='FX:XAUUSD') changed=writeFX('XAU',{price,bid,ask,src:'fcs_ws'});
    else if(sym==='FX:XAGUSD') changed=writeFX('XAG',{price,bid,ask,src:'fcs_ws'});
    else if(sym==='FX:USDINR') changed=writeFX('INR',{price,bid,ask,src:'fcs_ws'});
    if(changed) broadcast();
  });
  ws.on('close',(code)=>{
    fcsStatus='disconnected';
    if(fcsPingTimer){clearInterval(fcsPingTimer);fcsPingTimer=null;}
    fcsReconn++;
    const d=Math.min(fcsBackoff+Math.random()*1000,60000);
    fcsBackoff=Math.min(fcsBackoff*2,60000);
    console.warn('[FCS] Closed code=%d reconnect in %ds',code,(d/1000).toFixed(1));
    fcsReconnTimer=setTimeout(()=>{fcsReconnTimer=null;connectFCS();},d);
  });
  ws.on('error',(e)=>console.warn('[FCS] err:',e.message));
}

setInterval(()=>{ if(process.env.FCS_API_KEY&&fcsStatus==='disconnected'&&!fcsReconnTimer) connectFCS(); },30000);

// ═══════════════════════════════════════════════════
// TWELVE DATA — XAU WS + XAG/INR REST
// ═══════════════════════════════════════════════════
let tdWS=null, tdStatus='disconnected', tdPkts=0, tdReconn=0, tdReconnTimer=null, tdPingTimer=null;

function connectTD() {
  const key=process.env.TWELVE_DATA_KEY||''; if(!key) return;
  if(tdStatus==='connecting'||tdStatus==='connected') return;
  tdStatus='connecting';
  const ws=new WebSocket(`wss://ws.twelvedata.com/v1/quotes/price?apikey=${key}`,{handshakeTimeout:15000});
  tdWS=ws;
  ws.on('open',()=>{
    tdStatus='connected'; tdReconn=0;
    ws.send(JSON.stringify({action:'subscribe',params:{symbols:'XAU/USD'}}));
    if(tdPingTimer) clearInterval(tdPingTimer);
    tdPingTimer=setInterval(()=>{ if(ws.readyState===WebSocket.OPEN) ws.ping(); },20000);
  });
  ws.on('message',(data)=>{
    let msg; try{msg=JSON.parse(data);}catch{return;}
    if(msg.event==='heartbeat') return;
    if(msg.event==='price'&&msg.symbol==='XAU/USD'&&msg.price){
      const p=parseFloat(msg.price);
      if(p>3000&&p<9000){
        tdPkts++;
        const changed=writeFX('XAU',{price:p,bid:parseFloat(msg.bid||p),ask:parseFloat(msg.ask||p),src:'td_ws'});
        if(changed) broadcast();
      }
    }
  });
  ws.on('close',()=>{
    tdStatus='disconnected';
    if(tdPingTimer){clearInterval(tdPingTimer);tdPingTimer=null;}
    tdReconn++;
    const d=Math.min(3000*Math.pow(2,Math.min(tdReconn-1,4)),30000);
    tdReconnTimer=setTimeout(()=>{tdReconnTimer=null;connectTD();},d);
  });
  ws.on('error',(e)=>console.warn('[TD] err:',e.message));
}

setInterval(()=>{ if(process.env.TWELVE_DATA_KEY&&tdStatus==='disconnected'&&!tdReconnTimer) connectTD(); },30000);

// XAG REST every 3min
async function pollXAG() {
  const key=process.env.TWELVE_DATA_KEY||'';
  const sources=[
    async ()=>{ if(!key) throw 0; const r=await axios.get('https://api.twelvedata.com/price',{params:{symbol:'XAG/USD',apikey:key},timeout:7000}); return parseFloat(r.data?.price); },
    async ()=>{ const r=await axios.get('https://open.er-api.com/v6/latest/XAG',{timeout:6000}); return r.data?.rates?.USD; },
    async ()=>{ const r=await axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/xag.json',{timeout:7000}); return r.data?.xag?.usd; },
  ];
  for(const fn of sources){
    try{ const p=await fn(); if(p>20&&p<300){writeFX('XAG',{price:p,src:'rest'});broadcast();return;} }catch(e){}
  }
}

// USD/INR REST every 5min
async function pollINR() {
  const sources=[
    async ()=>{ const r=await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000}); return r.data.rates.INR; },
    async ()=>{ const r=await axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000}); return r.data.rates.INR; },
    async ()=>{ const r=await axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json',{timeout:5000}); return r.data.usd.inr; },
  ];
  for(const fn of sources){
    try{
      const v=await fn();
      if(v>70&&v<115){
        writeFX('INR',{price:v,bid:r2(v-0.03),ask:r2(v+0.03),src:'rest'});
        broadcast(); return;
      }
    }catch(e){}
  }
}

// Daily H/L from TD /quote every 15min
async function pollDailyHL() {
  const key=process.env.TWELVE_DATA_KEY||''; if(!key) return;
  try{
    const r=await axios.get('https://api.twelvedata.com/quote',{params:{symbol:'XAU/USD,XAG/USD,USD/INR',apikey:key},timeout:12000});
    const d=r.data;
    const xau=d?.['XAU/USD']||d,xag=d?.['XAG/USD'],inr=d?.['USD/INR'];
    if(xau?.high&&xau?.low){const h=parseFloat(xau.high),l=parseFloat(xau.low);if(h>3000){S.xauHigh=r2(h);S.xauLow=r2(l);}}
    if(xag?.high&&xag?.low){const h=parseFloat(xag.high),l=parseFloat(xag.low);if(h>20){S.xagHigh=r3(h);S.xagLow=r3(l);}}
    if(inr?.high&&inr?.low){const h=parseFloat(inr.high),l=parseFloat(inr.low);if(h>70){S.usdInrHigh=r2(h);S.usdInrLow=r2(l);}}
    broadcast();
    console.log('[TD-QUOTE] H/L updated');
  }catch(e){console.warn('[TD-QUOTE] fail:',e.message.slice(0,60));}
}

// OHLC REST backup
let ohlcCalls=0, ohlcErr=null, ohlcBackoff=0;
async function pollOHLC() {
  const token=currentToken, cid=process.env.DHAN_CLIENT_ID||'';
  if(!token||!cid||Date.now()<ohlcBackoff) return;
  if(dhanStatus==='connected'&&dhanLastTickAt>0&&Date.now()-dhanLastTickAt<4000) return;
  const secIds=[parseInt(AC.gold.secId),parseInt(AC.goldNext.secId),parseInt(AC.silver.secId),parseInt(AC.silverNext.secId)];
  try{
    const r=await axios.post(DHAN_BASE+'/marketfeed/ohlc',{MCX_COMM:secIds},{
      headers:{Accept:'application/json','Content-Type':'application/json','access-token':token,'client-id':cid},timeout:5000});
    const seg=r.data?.data?.MCX_COMM; if(!seg){ohlcErr='no MCX_COMM';return;}
    ohlcCalls++; ohlcErr=null;
    const applyRow=(sid,key)=>{const row=seg[String(sid)];if(!row)return;const l=row.last_price||0,o=row.ohlc||{};if(l>0)writeMCX(key,{ltp:Math.round(l),open:Math.round(o.open||0),high:Math.round(o.high||0),low:Math.round(o.low||0),src:'dhan_ohlc'});};
    applyRow(AC.gold.secId,'gold');applyRow(AC.goldNext.secId,'goldNext');
    applyRow(AC.silver.secId,'silver');applyRow(AC.silverNext.secId,'silverNext');
    broadcast();
  }catch(e){ohlcErr=e.message;if(e.response?.status===429){ohlcBackoff=Date.now()+60000;console.warn('[OHLC] 429 backoff 60s');}}
}

// ═══════════════════════════════════════════════════
// HTTP ROUTES
// ═══════════════════════════════════════════════════
app.get('/rates',(req,res)=>res.json(buildPayload()));

app.get('/debug',(req,res)=>res.json({
  server:'RR Jewellers v14',
  feed:{clients:feedClients.size,broadcasts:broadcastCount},
  dhan:{wsStatus:dhanStatus,packets:dhanPackets,reconnects:dhanReconnects,tickAgeMs:dhanLastTickAt?Date.now()-dhanLastTickAt:null,lastConnect:null},
  fcs:{status:fcsStatus,packets:fcsPkts,reconnects:fcsReconn,hasKey:!!process.env.FCS_API_KEY},
  td:{status:tdStatus,packets:tdPkts,reconnects:tdReconn,hasKey:!!process.env.TWELVE_DATA_KEY},
  ohlc:{calls:ohlcCalls,lastError:ohlcErr,backoffUntil:ohlcBackoff>Date.now()?new Date(ohlcBackoff).toISOString():null},
  rateCache:S, contracts:AC, tokenMap:TOKEN_MAP,
  marketOpen:isMCXOpen(),
  tokenRenewedAt, renewAttempts,
  env:{DHAN_CLIENT_ID:!!process.env.DHAN_CLIENT_ID,DHAN_PIN:!!process.env.DHAN_PIN,DHAN_TOTP_SECRET:!!process.env.DHAN_TOTP_SECRET,TWELVE_DATA_KEY:!!process.env.TWELVE_DATA_KEY,FCS_API_KEY:!!process.env.FCS_API_KEY,tokenLen:currentToken.length},
}));

app.get('/ping',(req,res)=>res.json({ok:true,ts:Date.now(),clients:feedClients.size,dhan:dhanStatus,tokenRenewedAt}));
app.get('/health',(req,res)=>{ const ok=dhanStatus==='connected'||fcsStatus==='connected'; res.status(ok?200:503).json({ok,dhan:dhanStatus,fcs:fcsStatus}); });

app.get('/token-renew',async(req,res)=>{ await renewWithRetry(); res.json({tokenRenewedAt,dhan:dhanStatus,tokenLen:currentToken.length}); });

app.get('/updates',async(req,res)=>{
  try{
    if(!SHEET_ID) throw new Error('no SHEET_ID');
    const r=await axios.get(`https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`,{timeout:8000});
    const data=JSON.parse(r.data.replace(/.*?({.*}).*/s,'$1'));
    res.json({success:true,updates:data.table.rows.map(row=>({date:row.c[0]?.v||'',title:row.c[1]?.v||'',content:row.c[2]?.v||'',image:row.c[3]?.v||''})).filter(r=>r.title)});
  }catch(e){res.json({success:true,updates:[{date:'Today',title:'Welcome to R.R. Jewellers',content:'Live gold & silver rates.',image:''}]});}
});

app.get('/',(req,res)=>res.json({status:'RR Jewellers v14',dhan:dhanStatus,fcs:fcsStatus,td:tdStatus,clients:feedClients.size,tokenRenewedAt}));

// ═══════════════════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════════════════
server.listen(PORT,'0.0.0.0',async()=>{
  console.log('[START] RR Jewellers v14 port=%s',PORT);

  await renewWithRetry();
  connectDhan();
  connectFCS();
  connectTD();

  // Polls
  pollXAG();   setInterval(pollXAG,  3*60*1000);
  pollINR();   setInterval(pollINR,  5*60*1000);
  pollDailyHL(); setInterval(pollDailyHL,15*60*1000);
  setInterval(()=>{ if(isMCXOpen()) pollOHLC(); },5000);

  // Periodic broadcast every 2s (new clients get data even during slow market)
  setInterval(broadcast, 2000);

  // Daily schedules
  const sched=(hh,mm,fn)=>{ const ms=msUntilIST(hh,mm); setTimeout(()=>{fn();sched(hh,mm,fn);},ms); };
  sched(8,30,()=>{ console.log('[TOKEN] 8:30AM IST renew'); renewWithRetry(); });
  sched(9,0, ()=>{ resetHL(); });

  // Self-ping keepalive
  if(SELF_URL) setInterval(()=>{ axios.get(SELF_URL+'/ping').catch(()=>{}); },4*60*1000);

  console.log('[START] All systems initialized. /feed WS ready.');
});
