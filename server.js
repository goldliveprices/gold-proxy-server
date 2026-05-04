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
const DHAN_BASE         = 'https://api.dhan.co/v2';

const GOLD_CONTRACTS = [
  { secId: '459277', display: 'GOLD JUN FUT',  expiry: '2026-06-05' },
  { secId: '466583', display: 'GOLD AUG FUT',  expiry: '2026-08-05' },
  { secId: '483079', display: 'GOLD OCT FUT',  expiry: '2026-10-05' },
  { secId: '495213', display: 'GOLD DEC FUT',  expiry: '2026-12-04' },
  { secId: '559933', display: 'GOLD FEB FUT',  expiry: '2027-02-05' },
];
const SILVER_CONTRACTS = [
  { secId: '457532', display: 'SILVER MAY FUT', expiry: '2026-05-05' },
  { secId: '464150', display: 'SILVER JUL FUT', expiry: '2026-07-03' },
  { secId: '471725', display: 'SILVER SEP FUT', expiry: '2026-09-04' },
  { secId: '495214', display: 'SILVER DEC FUT', expiry: '2026-12-04' },
  { secId: '564619', display: 'SILVER MAR FUT', expiry: '2027-03-05' },
];

function pickCurrentAndNext(contracts) {
  const now = new Date();
  const sorted = contracts
    .map(function(c) { return Object.assign({}, c, { expiryDate: new Date(c.expiry) }); })
    .filter(function(c) { return !isNaN(c.expiryDate); })
    .sort(function(a, b) { return a.expiryDate - b.expiryDate; });
  const upcoming = sorted.filter(function(c) { return c.expiryDate >= now; });
  if (upcoming.length >= 2) return { current: upcoming[0], next: upcoming[1] };
  if (upcoming.length === 1) return { current: sorted[sorted.length - 2] || upcoming[0], next: upcoming[0] };
  const last = sorted.slice(-2);
  return { current: last[0] || sorted[0], next: last[1] || sorted[0] };
}

function getActiveContracts() {
  return { gold: pickCurrentAndNext(GOLD_CONTRACTS), silver: pickCurrentAndNext(SILVER_CONTRACTS) };
}

const rateCache = {
  goldLtp: 0, goldOpen: 0, goldHigh: 0, goldLow: 0, goldPrevClose: 0, goldNextLtp: 0,
  silverLtp: 0, silverOpen: 0, silverHigh: 0, silverLow: 0, silverPrevClose: 0, silverNextLtp: 0,
  source: 'init', updatedAt: null,
};

function updateCacheFromTick(key, tick) {
  if (!tick || !tick.ltp || tick.ltp <= 0) return;
  if (key === 'gold') {
    rateCache.goldLtp = tick.ltp;
    if (tick.high > 0) rateCache.goldHigh = tick.high;
    if (tick.low > 0) rateCache.goldLow = tick.low;
    if (tick.open > 0) rateCache.goldOpen = tick.open;
    if (tick.prevClose > 0) rateCache.goldPrevClose = tick.prevClose;
  } else if (key === 'goldNext') {
    rateCache.goldNextLtp = tick.ltp;
  } else if (key === 'silver') {
    rateCache.silverLtp = tick.ltp;
    if (tick.high > 0) rateCache.silverHigh = tick.high;
    if (tick.low > 0) rateCache.silverLow = tick.low;
    if (tick.open > 0) rateCache.silverOpen = tick.open;
    if (tick.prevClose > 0) rateCache.silverPrevClose = tick.prevClose;
  } else if (key === 'silverNext') {
    rateCache.silverNextLtp = tick.ltp;
  }
  rateCache.updatedAt = new Date().toISOString();
}

function isMCXOpen() {
  var d = new Date(Date.now() + 5.5 * 3600000);
  var dow = d.getUTCDay();
  var t = d.getUTCHours() * 60 + d.getUTCMinutes();
  if (dow === 0) return false;
  return dow === 6 ? (t >= 540 && t < 840) : (t >= 540 && t < 1435);
}

var forexCache = { usdInr: 94.5, xauUsd: 3310, xagUsd: 32.8, updatedAt: null, src: 'init' };

function refreshForex() {
  var fxSrc = [
    function() { return axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 5000 }).then(function(r) { return r.data.rates.INR; }); },
    function() { return axios.get('https://open.er-api.com/v6/latest/USD', { timeout: 5000 }).then(function(r) { return r.data.rates.INR; }); },
  ];
  var p = Promise.resolve(0);
  fxSrc.forEach(function(fn) {
    p = p.then(function(v) {
      if (v > 70 && v < 110) { forexCache.usdInr = v; return v; }
      return fn().catch(function() { return 0; });
    });
  });
  var spotP = axios.get('https://api.metals.live/v1/spot/gold,silver', { timeout: 6000 })
    .then(function(r) {
      if (Array.isArray(r.data)) {
        var g = r.data.find(function(x) { return x.gold; });
        var s = r.data.find(function(x) { return x.silver; });
        if (g && g.gold > 3000) forexCache.xauUsd = g.gold;
        if (s && s.silver > 20) forexCache.xagUsd = s.silver;
      }
    })
    .catch(function() {
      return Promise.all([
        axios.get('https://www.gold-api.com/price/XAU', { timeout: 7000 }),
        axios.get('https://www.gold-api.com/price/XAG', { timeout: 7000 }),
      ]).then(function(res) {
        if (res[0].data.price > 3000) forexCache.xauUsd = res[0].data.price;
        if (res[1].data.price > 20) forexCache.xagUsd = res[1].data.price;
      }).catch(function() {});
    });
  return Promise.all([p, spotP]).then(function() {
    forexCache.updatedAt = new Date().toISOString();
    console.log('[FOREX] usdInr=%s xauUsd=%s', forexCache.usdInr, forexCache.xauUsd);
  });
}

function getSpotDerived() {
  var F = 1.103;
  return {
    goldPer10g:  Math.round((forexCache.xauUsd / 31.1035) * 10   * forexCache.usdInr * F),
    silverPerKg: Math.round((forexCache.xagUsd / 31.1035) * 1000 * forexCache.usdInr * F),
  };
}

var WS = {
  ws: null, status: 'disconnected',
  reconnectTimer: null, reconnectCount: 0,
  lastConnectAt: null, lastDisconnectAt: null,
  lastTickAt: null, lastDisconnectCode: null,
  packetsReceived: 0, lastRawHex: '', lastTextMsg: '',
};
var TOKEN_MAP = {};

function buildTokenMap() {
  var ac = getActiveContracts();
  TOKEN_MAP = {};
  TOKEN_MAP[ac.gold.current.secId]   = 'gold';
  TOKEN_MAP[ac.gold.next.secId]      = 'goldNext';
  TOKEN_MAP[ac.silver.current.secId] = 'silver';
  TOKEN_MAP[ac.silver.next.secId]    = 'silverNext';
}

function subscribeWS(ws) {
  var ac = getActiveContracts();
  var instruments = [
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.gold.current.secId },
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.gold.next.secId },
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.silver.current.secId },
    { ExchangeSegment: 'MCX_COMM', SecurityId: ac.silver.next.secId },
    { ExchangeSegment: 'NSE_EQ',   SecurityId: '1333' },
  ];
  [[15, 0], [17, 600], [21, 1200]].forEach(function(pair) {
    var code = pair[0], delay = pair[1];
    setTimeout(function() {
      if (ws.readyState !== WebSocket.OPEN) return;
      ws.send(JSON.stringify({ RequestCode: code, InstrumentCount: instruments.length, InstrumentList: instruments }));
      console.log('[WS] Subscribe RequestCode=%d sent', code);
    }, delay);
  });
}

function parseBuf(buf) {
  try {
    if (!buf || buf.length < 8) return null;
    var fc    = buf.readUInt8(0);
    var secId = buf.readInt32LE(4).toString();
    if (fc === 50) { WS.lastDisconnectCode = buf.length >= 10 ? buf.readInt16LE(8) : 0; return null; }
    if (fc === 6 && buf.length >= 16) return { type: 'prevClose', secId: secId, prevClose: Math.round(buf.readFloatLE(8)) };
    if (fc === 2 && buf.length >= 16) {
      var ltp2 = buf.readFloatLE(8);
      if (!isFinite(ltp2) || ltp2 <= 0) return null;
      return { type: 'ticker', secId: secId, ltp: Math.round(ltp2) };
    }
    if (fc === 4 && buf.length >= 50) {
      var ltp4 = buf.readFloatLE(8);
      if (!isFinite(ltp4) || ltp4 <= 0) return null;
      return { type: 'quote', secId: secId, ltp: Math.round(ltp4), open: Math.round(buf.readFloatLE(34)), high: Math.round(buf.readFloatLE(42)), low: Math.round(buf.readFloatLE(46)) };
    }
    return null;
  } catch(e) { return null; }
}

function scheduleReconnect() {
  if (WS.reconnectTimer) return;
  WS.reconnectCount++;
  var delay = Math.min(3000 * Math.pow(2, Math.min(WS.reconnectCount, 4)), 30000);
  WS.reconnectTimer = setTimeout(function() { WS.reconnectTimer = null; connectDhan(); }, delay);
  console.log('[WS] Reconnect in %ds attempt=%d', delay / 1000, WS.reconnectCount);
}

function connectDhan() {
  if (!DHAN_CLIENT_ID || !DHAN_ACCESS_TOKEN) { console.warn('[WS] No credentials'); return; }
  if (WS.status === 'connecting' || WS.status === 'connected') return;
  WS.status = 'connecting';
  WS.lastConnectAt = new Date().toISOString();
  WS.packetsReceived = 0; WS.lastRawHex = '';
  buildTokenMap();
  var wsUrl = 'wss://api-feed.dhan.co?version=2&token=' + encodeURIComponent(DHAN_ACCESS_TOKEN) + '&clientId=' + encodeURIComponent(DHAN_CLIENT_ID) + '&authType=2';
  var ws = new WebSocket(wsUrl, { handshakeTimeout: 15000 });
  WS.ws = ws;
  ws.on('open', function() {
    WS.status = 'connected'; WS.reconnectCount = 0;
    console.log('[WS] Connected tokenLen=%d', DHAN_ACCESS_TOKEN.length);
    subscribeWS(ws);
  });
  ws.on('message', function(data) {
    if (typeof data === 'string') { WS.lastTextMsg = data.slice(0, 500); console.log('[WS] Text:', WS.lastTextMsg); return; }
    var buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    WS.packetsReceived++;
    if (WS.packetsReceived <= 5) WS.lastRawHex = buf.slice(0, 32).toString('hex');
    var tick = parseBuf(buf);
    if (!tick) return;
    WS.lastTickAt = Date.now();
    var key = TOKEN_MAP[tick.secId];
    if (!key) { if (tick.secId === '1333') console.log('[WS] NSE test tick ltp=%d', tick.ltp); return; }
    if (tick.type === 'prevClose') {
      if (key === 'gold') rateCache.goldPrevClose = tick.prevClose;
      if (key === 'silver') rateCache.silverPrevClose = tick.prevClose;
      return;
    }
    updateCacheFromTick(key, tick);
    rateCache.source = 'dhan_ws_live';
    console.log('[WS] Tick %s ltp=%d', key, tick.ltp);
  });
  ws.on('close', function(code) {
    WS.status = 'disconnected';
    WS.lastDisconnectAt = new Date().toISOString();
    console.warn('[WS] Closed code=%s packets=%d', code, WS.packetsReceived);
    scheduleReconnect();
  });
  ws.on('error', function(err) { console.warn('[WS] Error:', err.message); });
}

var lastOhlcError = null;
var ohlcCallCount = 0;

function pollOhlcOnce() {
  if (!DHAN_CLIENT_ID || !DHAN_ACCESS_TOKEN) return Promise.resolve();
  var ac = getActiveContracts();
  var secIds = [
    parseInt(ac.gold.current.secId, 10),
    parseInt(ac.gold.next.secId, 10),
    parseInt(ac.silver.current.secId, 10),
    parseInt(ac.silver.next.secId, 10),
  ];
  return axios.post(DHAN_BASE + '/marketfeed/ohlc', { MCX_COMM: secIds }, {
    headers: { 'Accept': 'application/json', 'Content-Type': 'application/json', 'access-token': DHAN_ACCESS_TOKEN, 'client-id': DHAN_CLIENT_ID },
    timeout: 5000,
  }).then(function(resp) {
    var seg = resp.data && resp.data.data && resp.data.data['MCX_COMM'];
    if (!seg) { lastOhlcError = 'No MCX_COMM in response'; return; }
    ohlcCallCount++; lastOhlcError = null;
    var wsLive = WS.status === 'connected' && WS.lastTickAt && Date.now() - WS.lastTickAt < 5000;
    function applyOhlc(secId, key) {
      if (wsLive) return;
      var row = seg[String(secId)]; if (!row) return;
      var ltp = row.last_price || 0, ohlc = row.ohlc || {};
      if (ltp > 0) {
        updateCacheFromTick(key, { ltp: Math.round(ltp), open: ohlc.open ? Math.round(ohlc.open) : 0, high: ohlc.high ? Math.round(ohlc.high) : 0, low: ohlc.low ? Math.round(ohlc.low) : 0 });
        rateCache.source = 'dhan_ohlc_rest';
        if (ohlcCallCount <= 3) console.log('[OHLC] %s ltp=%d', key, Math.round(ltp));
      }
    }
    applyOhlc(ac.gold.current.secId, 'gold');
    applyOhlc(ac.gold.next.secId, 'goldNext');
    applyOhlc(ac.silver.current.secId, 'silver');
    applyOhlc(ac.silver.next.secId, 'silverNext');
  }).catch(function(e) {
    lastOhlcError = e.response && e.response.data ? JSON.stringify(e.response.data) : e.message;
  });
}

app.get('/rates', function(req, res) {
  var spot = getSpotDerived();
  var ac   = getActiveContracts();
  var src  = rateCache.goldLtp > 0 ? rateCache.source : 'spot_derived';
  res.json({
    success: true, source: src, marketOpen: isMCXOpen(),
    goldPer10g:  rateCache.goldLtp   || spot.goldPer10g,
    silverPerKg: rateCache.silverLtp || spot.silverPerKg,
    futures: {
      gold:       { ltp: rateCache.goldLtp,      open: rateCache.goldOpen,   high: rateCache.goldHigh,   low: rateCache.goldLow,   prevClose: rateCache.goldPrevClose,   contract: ac.gold.current.display,   expiry: ac.gold.current.expiry },
      goldNext:   { ltp: rateCache.goldNextLtp,  contract: ac.gold.next.display,   expiry: ac.gold.next.expiry },
      silver:     { ltp: rateCache.silverLtp,    open: rateCache.silverOpen, high: rateCache.silverHigh, low: rateCache.silverLow, prevClose: rateCache.silverPrevClose, contract: ac.silver.current.display, expiry: ac.silver.current.expiry },
      silverNext: { ltp: rateCache.silverNextLtp,contract: ac.silver.next.display, expiry: ac.silver.next.expiry },
    },
    spotDerived: spot, wsStatus: WS.status,
    wsTickAgeMs: WS.lastTickAt ? Date.now() - WS.lastTickAt : null,
    updatedAt: rateCache.updatedAt, forexUpdatedAt: forexCache.updatedAt,
    usdInr: forexCache.usdInr, timestamp: new Date().toISOString(),
  });
});

app.get('/debug', function(req, res) {
  var ac = getActiveContracts();
  res.json({
    server: 'RR Jewellers Hybrid v7', mode: 'WS primary + OHLC REST secondary + spot fallback',
    wsStatus: WS.status, wsPacketsReceived: WS.packetsReceived,
    lastRawHex: WS.lastRawHex, lastTextMsg: WS.lastTextMsg,
    lastDisconnectCode: WS.lastDisconnectCode, reconnectCount: WS.reconnectCount,
    lastConnectAt: WS.lastConnectAt, lastDisconnectAt: WS.lastDisconnectAt,
    lastTickAt: WS.lastTickAt ? new Date(WS.lastTickAt).toISOString() : null,
    wsTickAgeMs: WS.lastTickAt ? Date.now() - WS.lastTickAt : null,
    ohlcCallCount: ohlcCallCount, lastOhlcError: lastOhlcError,
    activeContracts: ac, tokenMap: TOKEN_MAP,
    rateCache: rateCache, forexCache: forexCache,
    marketOpen: isMCXOpen(),
    env: { DHAN_CLIENT_ID: !!DHAN_CLIENT_ID, clientIdLen: DHAN_CLIENT_ID.length, DHAN_ACCESS_TOKEN: !!DHAN_ACCESS_TOKEN, tokenLen: DHAN_ACCESS_TOKEN.length },
  });
});

app.get('/ping', function(req, res) { res.json({ ok: true, ts: Date.now() }); });
app.get('/', function(req, res) { res.json({ status: 'RR Jewellers Hybrid v7', endpoints: ['/rates', '/debug', '/ping'] }); });

app.listen(PORT, '0.0.0.0', function() {
  console.log('[STARTUP] RR Jewellers Hybrid v7 port=%s tokenLen=%d', PORT, DHAN_ACCESS_TOKEN.length);
  refreshForex().catch(function(e) { console.warn('[STARTUP] forex:', e.message); });
  connectDhan();
  setInterval(function() { if (isMCXOpen()) pollOhlcOnce(); }, 1000);
  setInterval(refreshForex, 5 * 60 * 1000);
  setInterval(function() { if (WS.status === 'disconnected' && !WS.reconnectTimer) connectDhan(); }, 30 * 1000);
  setInterval(function() { axios.get((SELF_URL || 'http://localhost:' + PORT) + '/ping').catch(function() {}); }, 4 * 60 * 1000);
  setInterval(function() { buildTokenMap(); console.log('[ROLLOVER] Token map rebuilt:', TOKEN_MAP); }, 24 * 60 * 60 * 1000);
});
