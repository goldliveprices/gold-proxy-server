'use strict';
// ── feedServer.js ─────────────────────────────────────────────
// Manages /feed WebSocket connections to HTML clients.
// Receives broadcasts from dhanAdapter + fcsAdapter via tickEngine.

const WebSocket = require('ws');
const tickEngine = require('./tickEngine');

let wss = null;
const clients = new Set();
let lastPayload = null;
let broadcastCount = 0;

// Called once from server.js with the HTTP server
function init(httpServer) {
  wss = new WebSocket.Server({
    server: httpServer,
    path: '/feed',
    // No compression — reduces latency
    perMessageDeflate: false,
  });

  wss.on('connection', (ws, req) => {
    clients.add(ws);
    ws.isAlive = true;
    ws.connAt = Date.now();
    console.log('[FEED] Client connected. total=%d', clients.size);

    // Always send snapshot — build fresh if lastPayload null
    try {
      const snap = lastPayload || JSON.stringify({ ts: Date.now(), src: 'init', mktOpen: false });
      ws.send(snap);
    } catch (e) {}

    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('close', (code) => {
      clients.delete(ws);
      console.log('[FEED] Client disconnected code=%d remaining=%d', code, clients.size);
    });
    ws.on('error', (e) => {
      clients.delete(ws);
    });
    // Send ping immediately to confirm connection alive
    ws.ping();
  });

  wss.on('error', (e) => console.error('[FEED] WSS error:', e.message));

  // Heartbeat — 15s ping to keep Render WS alive and detect dead clients
  setInterval(() => {
    let dead = 0;
    for (const ws of clients) {
      if (!ws.isAlive) {
        ws.terminate();
        clients.delete(ws);
        dead++;
        continue;
      }
      ws.isAlive = false;
      try { ws.ping(); } catch {}
    }
    if (dead > 0) console.log('[FEED] Heartbeat: removed %d dead clients', dead);
  }, 15000);

  console.log('[FEED] WebSocket server initialized on /feed');
}

// Called by dhanAdapter/fcsAdapter on every tick
// Also called periodically to push cached rates
function broadcast(goldPct, silverPct) {
  const payload = tickEngine.buildPayload(goldPct, silverPct);
  const msg = JSON.stringify(payload);
  lastPayload = msg; // always cache — used for new client snapshots
  broadcastCount++;

  if (clients.size === 0) return;
  const buf = Buffer.from(msg);
  let sent = 0;
  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(buf); sent++; }
      catch (e) { clients.delete(ws); }
    }
  }
}

function getStats() {
  return { clients: clients.size, broadcasts: broadcastCount };
}

module.exports = { init, broadcast, getStats };
