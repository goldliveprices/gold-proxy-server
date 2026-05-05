'use strict';
// RR Jewellers Hybrid v9 — Production Final
// All known bugs fixed, clean code

const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

const PORT              = process.env.PORT              || 3000;
const SELF_URL          = process.env.SELF_URL          || '';
const SHEET_ID          = process.env.SHEET_ID          || '';
const DHAN_CLIENT_ID    = process.env.DHAN_CLIENT_ID    || '';
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN || '';
const DHAN_BASE         = 'https://api.dhan.co/v2';

// ═══════════════════════════════════════
// MCX CONTRACTS — Verified secIds (May 2026)
// Gold  cycles: Jan Mar May Jul Sep Nov
// Silver cycles: Feb Apr Jun Aug Nov
// IMPORTANT: Keep this list updated every ~2 months
// ═══════════════════════════════════════
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
    .map(function(c) { return Object.assign({}, c, { ed: new Date(c.expiry) }); })
    .filter(function(c) { return !isNaN(c.ed); })
    .sort(function(a,b) { return a.ed - b.ed; });
  var up = sorted.filter(function(c) { return c.ed >= now; });
  if (up.length >= 2) return { current: up[0], next: up[1] };
  if (up.length === 1) return { current: sorted[sorted.length-2]||up[0], next: up[0] };
  var last = sorted.slice(-2);
  return { current: last[0]||sorted[0], next: last[1]||sorted[0] };
}
function getAC() {
  return { gold: pickCurrentAndNext(GOLD_CONTRACTS), silver: pickCurrentAndNext(SILVER_CONTRACTS) };
}

// ═══════════════════════════════════════
// RATE CACHE
// ═══════════════════════════════════════
var RC = {
  goldLtp:0, goldOpen:0, goldHigh:0, goldLow:0, goldPrevClose:0, goldBid:0, goldAsk:0,
  goldNextLtp:0, goldNextBid:0, goldNextAsk:0, goldNextHigh:0, goldNextLow:0,
  silverLtp:0, silverOpen:0, silverHigh:0, silverLow:0, silverPrevClose:0, silverBid:0, silverAsk:0,
  silverNextLtp:0, silverNextBid:0, silverNextAsk:0, silverNextHigh:0, silverNextLow:0,
  source:'init', updatedAt:null,
};

function applyTick(key, tick) {
  if (!tick || tick.ltp <= 0) return;
  var ltp = tick.ltp;
  if (key === 'gold') {
    RC.goldLtp = ltp;
    if (tick.high > 0) RC.goldHigh = tick.high;
    if (tick.low  > 0) RC.goldLow  = tick.low;
    if (tick.open > 0) RC.goldOpen = tick.open;
    if (tick.prevClose > 0) RC.goldPrevClose = tick.prevClose;
    RC.goldBid = tick.bid > 0 ? tick.bid : ltp - 30;
    RC.goldAsk = tick.ask > 0 ? tick.ask : ltp + 30;
  } else if (key === 'goldNext') {
    RC.goldNextLtp  = ltp;
    RC.goldNextBid  = tick.bid  > 0 ? tick.bid  : ltp - 50;
    RC.goldNextAsk  = tick.ask  > 0 ? tick.ask  : ltp + 50;
    RC.goldNextHigh = tick.high > 0 ? tick.high : RC.goldNextHigh;
    RC.goldNextLow  = tick.low  > 0 ? tick.low  : RC.goldNextLow;
  } else if (key === 'silver') {
    RC.silverLtp = ltp;
    if (tick.high > 0) RC.silverHigh = tick.high;
    if (tick.low  > 0) RC.silverLow  = tick.low;
    if (tick.open > 0) RC.silverOpen = tick.open;
    if (tick.prevClose > 0) RC.silverPrevClose = tick.prevClose;
    RC.silverBid = tick.bid > 0 ? tick.bid : ltp - 100;
    RC.silverAsk = tick.ask > 0 ? tick.ask : ltp + 100;
  } else if (key === 'silverNext') {
    RC.silverNextLtp  = ltp;
    RC.silverNextBid  = tick.bid  > 0 ? tick.bid  : ltp - 200;
    RC.silverNextAsk  = tick.ask  > 0 ? tick.ask  : ltp + 200;
    RC.silverNextHigh = tick.high > 0 ? tick.high : RC.silverNextHigh;
    RC.silverNextLow  = tick.low  > 0 ? tick.low  : RC.silverNextLow;
  }
  RC.updatedAt = new Date().toISOString();
}

// ═══════════════════════════════════════
// MCX OPEN CHECK (IST)
// ═══════════════════════════════════════
function isMCXOpen() {
  var d   = new Date(Date.now() + 5.5*3600000);
  var dow = d.getUTCDay();
  var t   = d.getUTCHours()*60 + d.getUTCMinutes();
  if (dow === 0) return false;
  return dow === 6 ? (t >= 540 && t < 840) : (t >= 540 && t < 1435);
}

// ═══════════════════════════════════════
// FOREX CACHE — refreshed every 5 min
// ═══════════════════════════════════════
var FX = { usdInr:94.5, xauUsd:3310, xagUsd:32.8, updatedAt:null, src:'init' };

async function refreshForex() {
  // USD/INR
  var usdInr = 0, src = '';
  var fxList = [
    ['frankfurter',  function(){ return axios.get('https://api.frankfurter.app/latest?from=USD&to=INR',{timeout:5000}).then(function(r){return r.data.rates.INR;}); }],
    ['open.er-api',  function(){ return axios.get('https://open.er-api.com/v6/latest/USD',{timeout:5000}).then(function(r){return r.data.rates.INR;}); }],
    ['fawazahmed0',  function(){ return axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json',{timeout:5000}).then(function(r){return r.data.usd.inr;}); }],
  ];
  for (var i=0; i<fxList.length && !usdInr; i++) {
    try { var v = await fxList[i][1](); if (v>70&&v<110) { usdInr=v; src=fxList[i][0]; } } catch(e){}
  }
  if (!usdInr) { usdInr = FX.usdInr; src = 'cached'; }

  // XAU/USD + XAG/USD
  var xauUsd = 0, xagUsd = 0;
  try {
    var r = await axios.get('https://api.metals.live/v1/spot/gold,silver',{timeout:6000});
    if (Array.isArray(r.data)) {
      var g=r.data.find(function(x){return x.gold;}), s=r.data.find(function(x){return x.silver;});
      if (g&&g.gold>3000) xauUsd=g.gold;
      if (s&&s.silver>20) xagUsd=s.silver;
    }
  } catch(e){}
  if (!xauUsd) {
    try {
      var res = await Promise.all([
        axios.get('https://www.gold-api.com/price/XAU',{timeout:7000}),
        axios.get('https://www.gold-api.com/price/XAG',{timeout:7000}),
      ]);
      if (res[0].data.price>3000) xauUsd = res[0].data.price;
      if (res[1].data.price>20)   xagUsd = res[1].data.price;
    } catch(e){}
  }
  if (!xauUsd) { xauUsd=FX.xauUsd; xagUsd=FX.xagUsd; }

  // Round cleanly
  FX.usdInr    = Math.round(usdInr * 100) / 100;
  FX.xauUsd    = Math.round(xauUsd * 100) / 100;
  FX.xagUsd    = Math.round(xagUsd * 1000) / 1000;
  FX.updatedAt = new Date().toISOString();
  FX.src       = src;
  console.log('[FOREX] usdInr=%s xauUsd=%s xagUsd=%s src=%s', FX.usdInr, FX.xauUsd, FX.xagUsd, FX.src);
}

function spotDerived() {
  var F = 1.103;
  return {
    goldPer10g:  Math.round((FX.xauUsd/31.1035)*10*FX.usdInr*F),
    silverPerKg: Math.round((FX.xagUsd/31.1035)*1000*FX.usdInr*F),
  };
}

// ═══════════════════════════════════════
// TOKEN AUTO-RENEW (every 20hr)
// ═══════════════════════════════════════
var currentToken    = DHAN_ACCESS_TOKEN;
var tokenRenewedAt  = null;

async function renewToken() {
  if (!currentToken || !DHAN_CLIENT_ID) return false;
  try {
    var r = await axios.post(DHAN_BASE+'/RenewToken', {}, {
      headers: { 'access-token':currentToken, 'dhanClientId':DHAN_CLIENT_ID, 'Content-Type':'application/json' },
      timeout: 12000,
    });
    var t = r.data?.accessToken || r.data?.access_token || r.data?.data?.accessToken;
    if (t) {
      currentToken   = t;
      tokenRenewedAt = new Date().toISOString();
      console.log('[TOKEN] Renewed at', tokenRenewedAt);
      // Reconnect WS with new token
      if (WS.ws) { try { WS.ws.terminate(); } catch(e){} }
      WS.status = 'disconnected';
      setTimeout(connectDhan, 3000);
      return true;
    }
    console.warn('[TOKEN] No token in response:', JSON.stringify(r.data).slice(0,100));
    return false;
  } catch(e) {
    console.warn('[TOKEN] Renew failed:', e.response?.status, e.message.slice(0,60));
    return false;
  }
}

// ═══════════════════════════════════════
// WEBSOCKET
// ═══════════════════════════════════════
var WS = {
  ws:null, status:'disconnected', reconnectTimer:null, reconnectCount:0,
  lastConnectAt:null, lastDisconnectAt:null, lastDisconnectCode:null,
  lastTickAt:null, packetsReceived:0, lastRawHex:'', lastTextMsg:'',
};
var TOKEN_MAP = {};

function buildTokenMap() {
  var ac = getAC();
  TOKEN_MAP = {};
  TOKEN_MAP[ac.gold.current.secId]   = 'gold';
  TOKEN_MAP[ac.gold.next.secId]      = 'goldNext';
  TOKEN_MAP[ac.silver.current.secId] = 'silver';
  TOKEN_MAP[ac.silver.next.secId]    = 'silverNext';
  console.log('[TOKENMAP]', JSON.stringify(TOKEN_MAP));
}

function subscribeWS(ws) {
  var ac = getAC();
  var instruments = [
    {ExchangeSegment:'MCX_COMM', SecurityId:ac.gold.current.secId},
    {ExchangeSegment:'MCX_COMM', SecurityId:ac.gold.next.secId},
    {ExchangeSegment:'MCX_COMM', SecurityId:ac.silver.current.secId},
    {ExchangeSegment:'MCX_COMM', SecurityId:ac.silver.next.secId},
  ];
  // Subscribe Ticker(15), Quote(17), Full(21) at intervals
  [[15,0],[17,600],[21,1200]].forEach(function(p) {
    setTimeout(function() {
      if (ws.readyState !== WebSocket.OPEN) return;
      ws.send(JSON.stringify({RequestCode:p[0], InstrumentCount:instruments.length, InstrumentList:instruments}));
      console.log('[WS] Subscribe code=%d sent', p[0]);
    }, p[1]);
  });
}

// Dhan v2 binary packet parser
// Header(8 bytes): [0]=feedCode, [1-2]=msgLen(int16LE), [3]=exchSeg, [4-7]=secId(int32LE)
// Ticker(2):  [8-11]=LTP(float32LE), [12-15]=LTT(int32LE)
// Quote(4):   [8-11]=LTP, [12-13]=LTQ, [14-17]=LTT, [18-21]=ATP,
//             [22-25]=Vol, [26-29]=TotalSell, [30-33]=TotalBuy,
//             [34-37]=Open, [38-41]=Close, [42-45]=High, [46-49]=Low
// Full(8):    [8-11]=LTP, [12-13]=LTQ, [14-17]=LTT, [18-21]=ATP,
//             [22-25]=Vol, [26-29]=TotalSell, [30-33]=TotalBuy,
//             [34-37]=OI, [38-41]=OIHigh, [42-45]=OILow,
//             [46-49]=Open, [50-53]=Close, [54-57]=High, [58-61]=Low,
//             [62+]=MarketDepth (5x20bytes)
//             Depth: BidQty(4)+AskQty(4)+BidOrders(2)+AskOrders(2)+BidPrice(4)+AskPrice(4)
//             1st BidPrice = 62+12=74, 1st AskPrice = 62+16=78
function parseBuf(buf) {
  try {
    if (!buf || buf.length < 8) return null;
    var fc    = buf.readUInt8(0);
    var secId = buf.readInt32LE(4).toString();

    if (fc === 50) {
      WS.lastDisconnectCode = buf.length >= 10 ? buf.readInt16LE(8) : 0;
      console.warn('[WS] Server disconnect code:', WS.lastDisconnectCode);
      return null;
    }

    // Prev Close
    if (fc === 6 && buf.length >= 16) {
      var pc = buf.readFloatLE(8);
      if (isFinite(pc) && pc > 0) return { type:'prevClose', secId:secId, prevClose:Math.round(pc) };
      return null;
    }

    // Ticker
    if (fc === 2 && buf.length >= 16) {
      var ltp2 = buf.readFloatLE(8);
      if (!isFinite(ltp2) || ltp2 <= 100) return null;
      return { type:'ticker', secId:secId, ltp:Math.round(ltp2) };
    }

    // Quote — correct offsets per Dhan v2 docs
    if (fc === 4 && buf.length >= 50) {
      var ltp4 = buf.readFloatLE(8);
      if (!isFinite(ltp4) || ltp4 <= 100) return null;
      return {
        type:'quote', secId:secId, ltp:Math.round(ltp4),
        open: Math.round(buf.readFloatLE(34)) || 0,
        high: Math.round(buf.readFloatLE(42)) || 0,
        low:  Math.round(buf.readFloatLE(46)) || 0,
      };
    }

    // Full — with bid/ask from market depth
    if (fc === 8 && buf.length >= 62) {
      var ltp8 = buf.readFloatLE(8);
      if (!isFinite(ltp8) || ltp8 <= 100) return null;
      var open8 = buf.length>49 ? Math.round(buf.readFloatLE(46)) : 0;
      var high8 = buf.length>57 ? Math.round(buf.readFloatLE(54)) : 0;
      var low8  = buf.length>61 ? Math.round(buf.readFloatLE(58)) : 0;
      var bid8  = Math.round(ltp8), ask8 = Math.round(ltp8);
      if (buf.length >= 82) {
        var b = buf.readFloatLE(74);
        var a = buf.readFloatLE(78);
        if (isFinite(b) && b > 100) bid8 = Math.round(b);
        if (isFinite(a) && a > 100) ask8 = Math.round(a);
      }
      return { type:'full', secId:secId, ltp:Math.round(ltp8), bid:bid8, ask:ask8, open:open8, high:high8, low:low8 };
    }
    return null;
  } catch(e) { return null; }
}

function scheduleReconnect() {
  if (WS.reconnectTimer) return;
  WS.reconnectCount++;
  var delay = Math.min(3000 * Math.pow(2, Math.min(WS.reconnectCount,4)), 30000);
  WS.reconnectTimer = setTimeout(function(){ WS.reconnectTimer=null; connectDhan(); }, delay);
  console.log('[WS] Reconnect in %ds attempt=%d', delay/1000, WS.reconnectCount);
}

function connectDhan() {
  if (!DHAN_CLIENT_ID || !currentToken) { console.warn('[WS] No credentials'); return; }
  if (WS.status === 'connecting' || WS.status === 'connected') return;
  WS.status = 'connecting';
  WS.lastConnectAt = new Date().toISOString();
  WS.packetsReceived = 0; WS.lastRawHex = '';
  buildTokenMap();
  var wsUrl = 'wss://api-feed.dhan.co?version=2&token=' + encodeURIComponent(currentToken) +
              '&clientId=' + encodeURIComponent(DHAN_CLIENT_ID) + '&authType=2';
  var ws = new WebSocket(wsUrl, { handshakeTimeout:15000 });
  WS.ws = ws;

  ws.on('open', function() {
    WS.status = 'connected'; WS.reconnectCount = 0;
    console.log('[WS] Connected — tokenLen=%d', currentToken.length);
    subscribeWS(ws);
  });

  ws.on('message', function(data) {
    if (typeof data === 'string') {
      WS.lastTextMsg = data.slice(0,500);
      console.log('[WS] Text:', WS.lastTextMsg);
      return;
    }
    var buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    WS.packetsReceived++;
    if (WS.packetsReceived <= 3) WS.lastRawHex = buf.slice(0,32).toString('hex');

    var tick = parseBuf(buf);
    if (!tick) return;
    WS.lastTickAt = Date.now();

    if (tick.type === 'prevClose') {
      if (TOKEN_MAP[tick.secId] === 'gold')   RC.goldPrevClose   = tick.prevClose;
      if (TOKEN_MAP[tick.secId] === 'silver') RC.silverPrevClose = tick.prevClose;
      return;
    }

    var key = TOKEN_MAP[tick.secId];
    if (!key) return;
    applyTick(key, tick);
    RC.source = 'dhan_ws_live';
  });

  ws.on('close', function(code) {
    WS.status = 'disconnected';
    WS.lastDisconnectAt = new Date().toISOString();
    WS.lastDisconnectCode = code;
    console.warn('[WS] Closed code=%s packets=%d', code, WS.packetsReceived);
    scheduleReconnect();
  });

  ws.on('error', function(err) { console.warn('[WS] Error:', err.message); });
}

// ═══════════════════════════════════════
// OHLC REST BACKUP (every 1s when market open, WS tick stale)
// ═══════════════════════════════════════
var lastOhlcError = null, ohlcCallCount = 0;

function pollOhlc() {
  if (!DHAN_CLIENT_ID || !currentToken) return;
  var ac = getAC();
  var secIds = [
    parseInt(ac.gold.current.secId,10),
    parseInt(ac.gold.next.secId,10),
    parseInt(ac.silver.current.secId,10),
    parseInt(ac.silver.next.secId,10),
  ];
  axios.post(DHAN_BASE+'/marketfeed/ohlc', {MCX_COMM:secIds}, {
    headers: { 'Accept':'application/json', 'Content-Type':'application/json', 'access-token':currentToken, 'client-id':DHAN_CLIENT_ID },
    timeout: 5000,
  }).then(function(resp) {
    var seg = resp.data && resp.data.data && resp.data.data['MCX_COMM'];
    if (!seg) { lastOhlcError = 'No MCX_COMM'; return; }
    ohlcCallCount++; lastOhlcError = null;
    var wsLive = WS.status==='connected' && WS.lastTickAt && Date.now()-WS.lastTickAt < 5000;
    if (wsLive) return; // WS is fresh — don't override
    function applyRow(secId, key) {
      var row = seg[String(secId)]; if (!row) return;
      var ltp = row.last_price || 0, ohlc = row.ohlc || {};
      if (ltp > 0) {
        applyTick(key, { ltp:Math.round(ltp), open:ohlc.open?Math.round(ohlc.open):0, high:ohlc.high?Math.round(ohlc.high):0, low:ohlc.low?Math.round(ohlc.low):0 });
        RC.source = 'dhan_ohlc_rest';
      }
    }
    applyRow(ac.gold.current.secId,   'gold');
    applyRow(ac.gold.next.secId,      'goldNext');
    applyRow(ac.silver.current.secId, 'silver');
    applyRow(ac.silver.next.secId,    'silverNext');
  }).catch(function(e) {
    lastOhlcError = e.response && e.response.data ? JSON.stringify(e.response.data).slice(0,100) : e.message;
  });
}

// ═══════════════════════════════════════
// ROUTES
// ═══════════════════════════════════════
app.get('/rates', function(req, res) {
  var ac   = getAC();
  var spot = spotDerived();
  var hasLive = RC.goldLtp > 0;
  var src  = hasLive ? RC.source : 'spot_derived';

  var goldBase   = hasLive ? RC.goldLtp   : spot.goldPer10g;
  var silverBase = hasLive ? RC.silverLtp : spot.silverPerKg;

  res.json({
    success:    true,
    source:     src,
    marketOpen: isMCXOpen(),
    goldPer10g:  goldBase,
    silverPerKg: silverBase,
    futures: {
      gold: {
        ltp:      RC.goldLtp,
        bid:      RC.goldBid,
        ask:      RC.goldAsk,
        high:     RC.goldHigh,
        low:      RC.goldLow,
        open:     RC.goldOpen,
        prevClose:RC.goldPrevClose,
        contract: ac.gold.current.display,
        expiry:   ac.gold.current.expiry,
      },
      goldNext: {
        ltp:     RC.goldNextLtp,
        bid:     RC.goldNextBid,
        ask:     RC.goldNextAsk,
        high:    RC.goldNextHigh,
        low:     RC.goldNextLow,
        contract:ac.gold.next.display,
        expiry:  ac.gold.next.expiry,
      },
      silver: {
        ltp:      RC.silverLtp,
        bid:      RC.silverBid,
        ask:      RC.silverAsk,
        high:     RC.silverHigh,
        low:      RC.silverLow,
        open:     RC.silverOpen,
        prevClose:RC.silverPrevClose,
        contract: ac.silver.current.display,
        expiry:   ac.silver.current.expiry,
      },
      silverNext: {
        ltp:     RC.silverNextLtp,
        bid:     RC.silverNextBid,
        ask:     RC.silverNextAsk,
        high:    RC.silverNextHigh,
        low:     RC.silverNextLow,
        contract:ac.silver.next.display,
        expiry:  ac.silver.next.expiry,
      },
    },
    spotDerived:    spot,
    usdInr:         FX.usdInr,
    xauUsd:         FX.xauUsd,
    xagUsd:         FX.xagUsd,
    forexUpdatedAt: FX.updatedAt,
    wsStatus:       WS.status,
    wsTickAgeMs:    WS.lastTickAt ? Date.now()-WS.lastTickAt : null,
    updatedAt:      RC.updatedAt,
    timestamp:      new Date().toISOString(),
  });
});

app.get('/debug', function(req, res) {
  res.json({
    server:'RR Jewellers v9', mode:'WS+OHLC+spot',
    wsStatus:WS.status, wsPacketsReceived:WS.packetsReceived,
    lastRawHex:WS.lastRawHex, lastTextMsg:WS.lastTextMsg,
    lastDisconnectCode:WS.lastDisconnectCode, reconnectCount:WS.reconnectCount,
    lastConnectAt:WS.lastConnectAt, lastDisconnectAt:WS.lastDisconnectAt,
    lastTickAt:WS.lastTickAt ? new Date(WS.lastTickAt).toISOString() : null,
    wsTickAgeMs:WS.lastTickAt ? Date.now()-WS.lastTickAt : null,
    ohlcCallCount:ohlcCallCount, lastOhlcError:lastOhlcError,
    activeContracts:getAC(), tokenMap:TOKEN_MAP,
    rateCache:RC, forexCache:FX,
    marketOpen:isMCXOpen(), tokenRenewedAt:tokenRenewedAt,
    env:{DHAN_CLIENT_ID:!!DHAN_CLIENT_ID,clientIdLen:DHAN_CLIENT_ID.length,DHAN_ACCESS_TOKEN:!!currentToken,tokenLen:currentToken.length},
  });
});

app.get('/token-renew', async function(req,res) {
  var ok = await renewToken();
  res.json({ success:ok, tokenRenewedAt:tokenRenewedAt, wsStatus:WS.status });
});

app.get('/spot-test', function(req,res) { res.json({ spot:spotDerived(), forex:FX }); });
app.get('/ping',      function(req,res) { res.json({ ok:true, ts:Date.now() }); });

app.get('/updates', async function(req,res) {
  try {
    if (!SHEET_ID) throw new Error('no SHEET_ID');
    var url = 'https://docs.google.com/spreadsheets/d/'+SHEET_ID+'/gviz/tq?tqx=out:json&sheet=Updates';
    var r   = await axios.get(url, { timeout:8000 });
    var json = r.data.replace(/.*?({.*}).*/s,'$1');
    var data = JSON.parse(json);
    var rows = data.table.rows.map(function(row) {
      return { date:row.c[0]?.v||'', title:row.c[1]?.v||'', content:row.c[2]?.v||'', image:row.c[3]?.v||'' };
    });
    res.json({ success:true, updates:rows.filter(function(r){return r.title;}) });
  } catch(e) {
    res.json({ success:true, updates:[{ date:'Today', title:'Welcome to R.R. Jewellers', content:'Live gold & silver rates.', image:'' }] });
  }
});

app.get('/', function(req,res) {
  res.json({ status:'RR Jewellers v9', wsStatus:WS.status, endpoints:['/rates','/debug','/ping','/token-renew','/spot-test','/updates'] });
});

// ═══════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════
app.listen(PORT, '0.0.0.0', async function() {
  console.log('[STARTUP] RR Jewellers v9 port=%s tokenLen=%d', PORT, DHAN_ACCESS_TOKEN.length);

  // Renew token immediately on startup (fresh token)
  var renewed = await renewToken();
  if (!renewed) {
    console.log('[STARTUP] Using provided token (renew failed or no API key)');
    currentToken = DHAN_ACCESS_TOKEN;
  }

  await refreshForex();
  connectDhan();

  setInterval(function() { if (isMCXOpen()) pollOhlc(); }, 1000);
  setInterval(refreshForex, 5*60*1000);
  setInterval(function() { if (WS.status==='disconnected' && !WS.reconnectTimer) connectDhan(); }, 30*1000);
  setInterval(function() { axios.get((SELF_URL||'http://localhost:'+PORT)+'/ping').catch(function(){}); }, 4*60*1000);
  setInterval(function() { buildTokenMap(); }, 24*60*60*1000);

  // Token auto-renew every 20 hours
  setInterval(async function() {
    console.log('[TOKEN] 20hr auto-renew triggered');
    await renewToken();
  }, 20*60*60*1000);
});
