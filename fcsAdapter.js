'use strict';
// ── fcsAdapter.js ─────────────────────────────────────────────
// FCS WebSocket primary for spot forex: XAU/USD, XAG/USD, USD/INR
// TD WebSocket fallback for XAU/USD
// REST waterfall for XAG/USD
// REST fallback for USD/INR

const WebSocket = require('ws');
const axios     = require('axios');
const cache     = require('./cacheEngine');

const ENV = {
  get fcsKey()  { return process.env.FCS_API_KEY       || ''; },
  get tdKey()   { return process.env.TWELVE_DATA_KEY   || ''; },
  get mpKey()   { return process.env.METALPRICEAPI_KEY || ''; },
};

let onBroadcast = null;

// ════════════════════════════════════════════════════════════
// FCS WebSocket — XAU/USD, XAG/USD, USD/INR live ticks
// ════════════════════════════════════════════════════════════
const FCS = {
  ws: null, status: 'disconnected',
  reconnects: 0, packets: 0,
  reconnectTimer: null, pingTimer: null,
  backoffMs: 2000,
};

function connectFCS() {
  if (!ENV.fcsKey) return;
  if (FCS.status === 'connecting' || FCS.status === 'connected') return;
  FCS.status = 'connecting';

  const ws = new WebSocket(`wss://ws-v4.fcsapi.com/ws?access_key=${ENV.fcsKey}`, {
    handshakeTimeout: 15000,
  });
  FCS.ws = ws;

  ws.on('open', () => {
    FCS.status = 'connected'; FCS.reconnects = 0; FCS.backoffMs = 2000;
    console.log('[FCS] ✅ Connected');
    // Subscribe to tick-level (timeframe=0) for max speed
    ['FX:XAUUSD', 'FX:XAGUSD', 'FX:USDINR'].forEach(sym => {
      ws.send(JSON.stringify({ type: 'join_symbol', symbol: sym, timeframe: '0' }));
    });

    if (FCS.pingTimer) clearInterval(FCS.pingTimer);
    FCS.pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.ping();
    }, 25000);
  });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || msg.type !== 'price') return;

    const p   = msg.prices;
    const sym = msg.symbol || '';
    if (!p) return;
    FCS.packets++;

    const price = p.c || p.close || 0;
    const bid   = p.b || p.bid   || 0;
    const ask   = p.a || p.ask   || 0;
    let changed = false;

    if (sym === 'FX:XAUUSD')
      changed = cache.writeFX('XAU', { price, bid, ask, source: 'fcs_ws' });
    else if (sym === 'FX:XAGUSD')
      changed = cache.writeFX('XAG', { price, bid, ask, source: 'fcs_ws' });
    else if (sym === 'FX:USDINR')
      changed = cache.writeFX('INR', { price, bid, ask, source: 'fcs_ws' });

    if (changed && onBroadcast) onBroadcast();
  });

  ws.on('pong', () => {});
  ws.on('close', (code) => {
    FCS.status = 'disconnected';
    if (FCS.pingTimer) { clearInterval(FCS.pingTimer); FCS.pingTimer = null; }
    FCS.reconnects++;
    const jitter = Math.random() * 1000;
    const delay  = Math.min(FCS.backoffMs + jitter, 60000);
    FCS.backoffMs = Math.min(FCS.backoffMs * 2, 60000);
    console.warn('[FCS] Closed code=%d reconnect in %ds', code, (delay/1000).toFixed(1));
    FCS.reconnectTimer = setTimeout(() => { FCS.reconnectTimer = null; connectFCS(); }, delay);
  });
  ws.on('error', (e) => console.warn('[FCS] err:', e.message));
}

// ════════════════════════════════════════════════════════════
// Twelve Data WebSocket — XAU/USD live (170ms latency)
// Fallback when FCS is not connected
// ════════════════════════════════════════════════════════════
const TD = {
  ws: null, status: 'disconnected',
  reconnects: 0, packets: 0,
  reconnectTimer: null, pingTimer: null,
};

function connectTD() {
  if (!ENV.tdKey) return;
  if (TD.status === 'connecting' || TD.status === 'connected') return;
  TD.status = 'connecting';

  const ws = new WebSocket(`wss://ws.twelvedata.com/v1/quotes/price?apikey=${ENV.tdKey}`, {
    handshakeTimeout: 15000,
  });
  TD.ws = ws;

  ws.on('open', () => {
    TD.status = 'connected'; TD.reconnects = 0;
    ws.send(JSON.stringify({ action: 'subscribe', params: { symbols: 'XAU/USD' } }));
    if (TD.pingTimer) clearInterval(TD.pingTimer);
    TD.pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.ping();
    }, 20000);
  });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }
    if (msg.event === 'heartbeat') return;
    if (msg.event === 'price' && msg.symbol === 'XAU/USD' && msg.price) {
      const p = parseFloat(msg.price);
      if (p > 3000 && p < 9000) {
        TD.packets++;
        const changed = cache.writeFX('XAU', {
          price: p,
          bid:   parseFloat(msg.bid  || p),
          ask:   parseFloat(msg.ask  || p),
          source: 'td_ws',
        });
        if (changed && onBroadcast) onBroadcast();
      }
    }
  });

  ws.on('close', (code) => {
    TD.status = 'disconnected';
    if (TD.pingTimer) { clearInterval(TD.pingTimer); TD.pingTimer = null; }
    TD.reconnects++;
    const delay = Math.min(3000 * Math.pow(2, Math.min(TD.reconnects - 1, 4)), 30000);
    TD.reconnectTimer = setTimeout(() => { TD.reconnectTimer = null; connectTD(); }, delay);
  });
  ws.on('error', (e) => console.warn('[TD] err:', e.message));
}

// ════════════════════════════════════════════════════════════
// REST polls — XAG/USD + USD/INR + daily H/L
// ════════════════════════════════════════════════════════════

// XAG/USD — 5-source waterfall, every 3 min
async function pollXAG() {
  // 1. Twelve Data /price
  if (ENV.tdKey) {
    try {
      const r = await axios.get('https://api.twelvedata.com/price', {
        params: { symbol: 'XAG/USD', apikey: ENV.tdKey }, timeout: 7000 });
      const p = parseFloat(r.data?.price);
      if (p > 20 && p < 300) {
        cache.writeFX('XAG', { price: p, source: 'td_rest' });
        if (onBroadcast) onBroadcast();
        return;
      }
    } catch {}
  }
  // 2. open.er-api (XAG base)
  try {
    const r = await axios.get('https://open.er-api.com/v6/latest/XAG', { timeout: 6000 });
    const p = r.data?.rates?.USD;
    if (p > 20 && p < 300) {
      cache.writeFX('XAG', { price: p, source: 'open.er-api' });
      if (onBroadcast) onBroadcast();
      return;
    }
  } catch {}
  // 3. fawazahmed0 CDN (unlimited, no key)
  try {
    const r = await axios.get(
      'https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/xag.json',
      { timeout: 7000 });
    const p = r.data?.xag?.usd;
    if (p > 20 && p < 300) {
      cache.writeFX('XAG', { price: p, source: 'fawazahmed0' });
      if (onBroadcast) onBroadcast();
      return;
    }
  } catch {}
  // 4. Frankfurter XAG
  try {
    const r = await axios.get('https://api.frankfurter.app/latest?from=USD&to=XAG', { timeout: 5000 });
    const rate = r.data?.rates?.XAG;
    if (rate > 0) {
      const p = Math.round((1 / rate) * 1000) / 1000;
      if (p > 20 && p < 300) {
        cache.writeFX('XAG', { price: p, source: 'frankfurter' });
        if (onBroadcast) onBroadcast();
      }
    }
  } catch {}
}

// USD/INR REST — every 5 min
async function pollUSDINR() {
  const sources = [
    ['frankfurter',  () => axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', {timeout:5000}).then(r=>r.data.rates.INR)],
    ['open.er-api',  () => axios.get('https://open.er-api.com/v6/latest/USD', {timeout:5000}).then(r=>r.data.rates.INR)],
    ['fawazahmed0',  () => axios.get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json', {timeout:5000}).then(r=>r.data.usd.inr)],
  ];
  for (const [src, fn] of sources) {
    try {
      const v = await fn();
      if (v > 70 && v < 115) {
        const bid = Math.round((v - 0.03) * 100) / 100;
        const ask = Math.round((v + 0.03) * 100) / 100;
        cache.writeFX('INR', { price: v, bid, ask, source: src });
        if (onBroadcast) onBroadcast();
        return;
      }
    } catch {}
  }
}

// Daily H/L from TD /quote batch — every 15 min
async function pollDailyHL() {
  if (!ENV.tdKey) return;
  try {
    const r = await axios.get('https://api.twelvedata.com/quote', {
      params: { symbol: 'XAU/USD,XAG/USD,USD/INR', apikey: ENV.tdKey },
      timeout: 12000,
    });
    const d = r.data;
    const xau = d?.['XAU/USD'] || d;
    const xag = d?.['XAG/USD'];
    const inr = d?.['USD/INR'];

    if (xau?.high && xau?.low) {
      const h = parseFloat(xau.high), l = parseFloat(xau.low);
      if (h > 3000) { cache.writeFX('XAU', { price: 0, high: h, low: l, source: 'td_quote' }); }
    }
    if (xag?.high && xag?.low) {
      const h = parseFloat(xag.high), l = parseFloat(xag.low);
      if (h > 20) { cache.writeFX('XAG', { price: 0, high: h, low: l, source: 'td_quote' }); }
    }
    if (inr?.high && inr?.low) {
      const h = parseFloat(inr.high), l = parseFloat(inr.low);
      if (h > 70) {
        cache.writeFX('INR', { price: 0, high: h, low: l,
          bid: inr.bid ? parseFloat(inr.bid) : 0,
          ask: inr.ask ? parseFloat(inr.ask) : 0,
          source: 'td_quote' });
      }
    }
    if (onBroadcast) onBroadcast();
    console.log('[TD-QUOTE] H/L batch updated');
  } catch (e) { console.warn('[TD-QUOTE] fail:', e.message.slice(0, 60)); }
}

// XAG daily H/L via time_series
async function pollXAGHL() {
  if (!ENV.tdKey) return;
  try {
    const r = await axios.get('https://api.twelvedata.com/time_series', {
      params: { symbol: 'XAG/USD', interval: '1day', outputsize: 1, apikey: ENV.tdKey },
      timeout: 10000,
    });
    const vals = r.data?.values;
    if (vals?.length > 0) {
      const h = parseFloat(vals[0].high), l = parseFloat(vals[0].low);
      if (h > 20 && h < 300) {
        cache.writeFX('XAG', { price: 0, high: h, low: l, source: 'td_timeseries' });
        console.log('[XAG-HL] H=%s L=%s', h, l);
      }
    }
  } catch (e) { console.warn('[XAG-HL] fail:', e.message.slice(0, 60)); }
}

// FCS/TD watchdogs
function startWatchdogs() {
  setInterval(() => {
    if (ENV.fcsKey && FCS.status === 'disconnected' && !FCS.reconnectTimer) connectFCS();
  }, 30000);
  setInterval(() => {
    if (ENV.tdKey && TD.status === 'disconnected' && !TD.reconnectTimer) connectTD();
  }, 30000);
}

function startPolls() {
  // XAG every 3min (but skip if FCS WS has it live)
  pollXAG();
  setInterval(() => {
    if (FCS.status !== 'connected') pollXAG();
  }, 3 * 60 * 1000);

  // USD/INR REST every 5min (backup for FCS WS)
  pollUSDINR();
  setInterval(() => {
    if (FCS.status !== 'connected') pollUSDINR();
    else pollUSDINR(); // always run as a sanity check
  }, 5 * 60 * 1000);

  // Daily H/L every 15min
  pollDailyHL();
  setInterval(pollDailyHL, 15 * 60 * 1000);

  // XAG H/L every 15min
  pollXAGHL();
  setInterval(pollXAGHL, 15 * 60 * 1000);
}

function onTick(fn) { onBroadcast = fn; }

function getStats() {
  return {
    fcs:  { status: FCS.status,  packets: FCS.packets,  reconnects: FCS.reconnects, hasKey: !!ENV.fcsKey },
    td:   { status: TD.status,   packets: TD.packets,   reconnects: TD.reconnects,  hasKey: !!ENV.tdKey },
  };
}

function start() {
  connectFCS();
  connectTD();
  startPolls();
  startWatchdogs();
}

module.exports = { start, onTick, getStats };
