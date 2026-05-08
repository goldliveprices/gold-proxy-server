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
  wss = new WebSocket.Server({ server: httpServer, path: '/feed' });

  wss.on('connection', (ws, req) => {
    clients.add(ws);
    ws.isAlive = true;
    ws.connAt = Date.now();

    // Send current snapshot immediately
    if (lastPayload) ws.send(lastPayload);

    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('close', () => clients.delete(ws));
    ws.on('error', () => clients.delete(ws));
  });

  // Heartbeat — keep Render free-tier WS alive, detect dead clients
  setInterval(() => {
    for (const ws of clients) {
      if (!ws.isAlive) { ws.terminate(); clients.delete(ws); continue; }
      ws.isAlive = false;
      ws.ping();
    }
  }, 15000);

  console.log('[FEED] WebSocket server initialized on /feed');
}

// Called by dhanAdapter or fcsAdapter when new data arrives
function broadcast(goldPct, silverPct) {
  if (clients.size === 0) return;
  const payload = tickEngine.buildPayload(goldPct, silverPct);
  const msg = JSON.stringify(payload);
  lastPayload = msg; // cache for new connections
  broadcastCount++;

  const buf = Buffer.from(msg);
  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(buf);
  }
}

function getStats() {
  return { clients: clients.size, broadcasts: broadcastCount };
}

module.exports = { init, broadcast, getStats };
