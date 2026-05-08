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
  // Use noServer mode + manual upgrade handling for Render compatibility
  // Render's reverse proxy passes WebSocket upgrades — path filtering done manually
  wss = new WebSocket.Server({
    noServer: true,
    perMessageDeflate: false,
  });

  // Handle HTTP upgrade → WebSocket upgrade manually
  // Only accept connections to /feed path
  httpServer.on('upgrade', (req, socket, head) => {
    const url = req.url || '';
    // Accept /feed and /feed?* and / (some clients may omit path)
    if (url === '/feed' || url.startsWith('/feed?') || url === '/') {
      wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit('connection', ws, req);
      });
    } else {
      console.log('[FEED] Rejected upgrade to:', url);
      socket.destroy();
    }
  });

  wss.on('connection', (ws, req) => {
    clients.add(ws);
    ws.isAlive = true;
    ws.connAt = Date.now();
    const ip = req.headers['x-forwarded-for'] || req.socket?.remoteAddress || 'unknown';
    console.log('[FEED] Client connected ip=%s total=%d', ip, clients.size);

    // Always send current snapshot immediately
    try {
      const snap = lastPayload || JSON.stringify({
        ts: Date.now(), src: 'init', mktOpen: false,
        goldSell: null, silverSell: null,
        f: {g:{},gN:{},s:{},sN:{}}, sp: {},
        success: true, message: 'connected'
      });
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
    ws.ping(); // confirm alive immediately
  });

  wss.on('error', (e) => console.error('[FEED] WSS error:', e.message));

  // Heartbeat every 15s — keeps Render free-tier WS alive
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
    if (dead > 0) console.log('[FEED] Heartbeat cleaned %d dead', dead);
  }, 15000);

  console.log('[FEED] WebSocket server ready (noServer mode, /feed path)');
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
