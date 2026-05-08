'use strict';
// ── dhanAdapter.js ────────────────────────────────────────────
// Dhan WS primary MCX feed.
// RC15 (Ticker) = fastest, smallest packet (~16 bytes)
// RC17 (Quote) = OHLC, sent once after RC15
// Binary little-endian parser per Dhan v2 spec.

const WebSocket    = require('ws');
const cache        = require('./cacheEngine');
const tickEngine   = require('./tickEngine');
const tokenManager = require('./tokenManager');

const ENV = {
  get clientId() { return process.env.DHAN_CLIENT_ID || ''; },
};

const state = {
  ws: null,
  status: 'disconnected',
  reconnects: 0,
  reconnectTimer: null,
  pingTimer: null,
  lastTickAt: 0,
  packetsRx: 0,
  lastConnectAt: null,
  // Reconnect backoff
  backoffMs: 1000,
};

let onBroadcast = null; // injected by server.js

// ── Binary parser ─────────────────────────────────────────────
function parseBuf(buf) {
  try {
    if (!buf || buf.length < 8) return null;
    const fc    = buf.readUInt8(0);
    const secId = buf.readInt32LE(4).toString();
    if (fc === 50) return null; // disconnect packet

    // FC6 = PrevClose
    if (fc === 6 && buf.length >= 16) {
      const pc = buf.readFloatLE(8);
      return isFinite(pc) && pc > 0 ? { type: 'prevClose', secId, prevClose: Math.round(pc) } : null;
    }
    // FC2 = Ticker (RC15 response) — LTP only
    if (fc === 2 && buf.length >= 16) {
      const ltp = buf.readFloatLE(8);
      return isFinite(ltp) && ltp > 100 ? { type: 'ticker', secId, ltp: Math.round(ltp) } : null;
    }
    // FC4 = Quote (RC17 response) — LTP + OHLC
    if (fc === 4 && buf.length >= 50) {
      const ltp  = buf.readFloatLE(8);  if (!isFinite(ltp) || ltp <= 100) return null;
      const open = buf.length > 37 ? buf.readFloatLE(34) : 0;
      const high = buf.length > 45 ? buf.readFloatLE(42) : 0;
      const low  = buf.length > 49 ? buf.readFloatLE(46) : 0;
      return { type: 'quote', secId,
        ltp: Math.round(ltp), open: Math.round(open) || 0,
        high: Math.round(high) || 0, low: Math.round(low) || 0 };
    }
    // FC8 = Full (depth + OHLC)
    if (fc === 8 && buf.length >= 62) {
      const ltp = buf.readFloatLE(8); if (!isFinite(ltp) || ltp <= 100) return null;
      const open = buf.length > 49 ? Math.round(buf.readFloatLE(46)) : 0;
      const high = buf.length > 57 ? Math.round(buf.readFloatLE(54)) : 0;
      const low  = buf.length > 61 ? Math.round(buf.readFloatLE(58)) : 0;
      let bid = Math.round(ltp), ask = Math.round(ltp);
      if (buf.length >= 82) {
        const bf = buf.readFloatLE(74), af = buf.readFloatLE(78);
        if (isFinite(bf) && bf > 100) bid = Math.round(bf);
        if (isFinite(af) && af > 100) ask = Math.round(af);
      }
      return { type: 'full', secId, ltp: Math.round(ltp), bid, ask, open, high, low };
    }
    return null;
  } catch { return null; }
}

// ── Subscribe ─────────────────────────────────────────────────
function subscribe(ws) {
  const tm = tickEngine.getTokenMap();
  const instruments = Object.entries(tm).map(([secId]) => ({
    ExchangeSegment: 'MCX_COMM',
    SecurityId: secId,
  }));

  const send = (obj, delay) => setTimeout(() => {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  }, delay);

  // RC15 first — instant LTP ticks
  send({ RequestCode: 15, InstrumentCount: instruments.length, InstrumentList: instruments }, 0);
  // RC17 after 300ms — get OHLC without doubling tick frequency
  send({ RequestCode: 17, InstrumentCount: instruments.length, InstrumentList: instruments }, 300);

  console.log('[DHAN] Subscribed %d instruments RC15+RC17', instruments.length);
}

// ── Connect ───────────────────────────────────────────────────
function connect() {
  const token    = tokenManager.getToken();
  const clientId = ENV.clientId;
  if (!token || !clientId) {
    console.warn('[DHAN] Missing token or clientId — retrying in 10s');
    state.reconnectTimer = setTimeout(connect, 10000);
    return;
  }
  if (state.status === 'connecting' || state.status === 'connected') return;

  state.status       = 'connecting';
  state.lastConnectAt = new Date().toISOString();
  state.packetsRx    = 0;

  const url = `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(clientId)}&authType=2`;
  const ws  = new WebSocket(url, { handshakeTimeout: 15000 });
  state.ws  = ws;

  ws.on('open', () => {
    state.status    = 'connected';
    state.reconnects = 0;
    state.backoffMs  = 1000;
    state.lastTickAt = Date.now();
    console.log('[DHAN] ✅ Connected');
    subscribe(ws);

    // Keepalive ping every 20s
    if (state.pingTimer) clearInterval(state.pingTimer);
    state.pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.ping();
    }, 20000);
  });

  ws.on('message', (data) => {
    if (typeof data === 'string') return; // text frames ignored
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    state.packetsRx++;

    const tick = parseBuf(buf);
    if (!tick) return;

    state.lastTickAt = Date.now();
    const tm  = tickEngine.getTokenMap();
    const key = tm[tick.secId];
    if (!key) return;

    if (tick.type === 'prevClose') {
      if (key === 'gold')   cache.writeMCX('gold',   { prevClose: tick.prevClose, ltp: 0 });
      if (key === 'silver') cache.writeMCX('silver', { prevClose: tick.prevClose, ltp: 0 });
      return;
    }

    const changed = cache.writeMCX(key, { ...tick, source: 'dhan_ws' });
    // Broadcast on every changed tick — this is the hot path
    if (changed && onBroadcast) onBroadcast();
  });

  ws.on('pong', () => { state.lastTickAt = Date.now(); });

  ws.on('close', (code) => {
    state.status = 'disconnected';
    if (state.pingTimer) { clearInterval(state.pingTimer); state.pingTimer = null; }
    console.warn('[DHAN] Closed code=%d pkts=%d', code, state.packetsRx);

    // Token expired → trigger renew
    if (code === 1008 || state.reconnects >= 5) {
      console.warn('[DHAN] Triggering token renew after %d reconnects', state.reconnects);
      tokenManager.renewWithRetry();
    }
    scheduleReconnect();
  });

  ws.on('error', (e) => console.warn('[DHAN] WS error:', e.message));
}

function scheduleReconnect() {
  if (state.reconnectTimer) return;
  state.reconnects++;
  // Exponential backoff with jitter: 1s→2s→4s→8s→16s max 30s
  const jitter  = Math.random() * 1000;
  const delay   = Math.min(state.backoffMs + jitter, 30000);
  state.backoffMs = Math.min(state.backoffMs * 2, 30000);

  state.reconnectTimer = setTimeout(() => {
    state.reconnectTimer = null;
    connect();
  }, delay);
  console.log('[DHAN] Reconnect #%d in %ds', state.reconnects, (delay / 1000).toFixed(1));
}

// Watchdog — reconnect if stale > 45s or disconnected
function startWatchdog() {
  setInterval(() => {
    if (state.status === 'disconnected' && !state.reconnectTimer) {
      connect();
      return;
    }
    if (state.status === 'connected' && state.lastTickAt > 0) {
      const age = Date.now() - state.lastTickAt;
      if (age > 45000) {
        console.warn('[DHAN] Stale feed %ds — reconnecting', (age / 1000).toFixed(0));
        if (state.ws) state.ws.terminate();
        state.status = 'disconnected';
        scheduleReconnect();
      }
    }
  }, 10000);
}

function onTick(fn) { onBroadcast = fn; }

function getStats() {
  return {
    wsStatus:    state.status,
    packets:     state.packetsRx,
    reconnects:  state.reconnects,
    tickAgeMs:   state.lastTickAt ? Date.now() - state.lastTickAt : null,
    lastConnect: state.lastConnectAt,
  };
}

module.exports = { connect, startWatchdog, onTick, getStats };
