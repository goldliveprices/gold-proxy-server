'use strict';
// ── server.js ─────────────────────────────────────────────────
// RR Jewellers v13 — Production
// Architecture: Dhan WS → cacheEngine → feedServer → HTML WS
//               FCS WS  → cacheEngine → feedServer → HTML WS
//               (independent, each at own speed)

const express      = require('express');
const http         = require('http');
const axios        = require('axios');
const cache        = require('./cacheEngine');
const tickEngine   = require('./tickEngine');
const feedServer   = require('./feedServer');
const dhanAdapter  = require('./dhanAdapter');
const fcsAdapter   = require('./fcsAdapter');
const tokenManager = require('./tokenManager');

const app    = express();
const server = http.createServer(app);
app.use(require('express').json());

// ── CORS ──────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  next();
});

// ── Env helpers ───────────────────────────────────────────────
const PORT       = process.env.PORT       || 3000;
const SELF_URL   = process.env.SELF_URL   || '';
const SHEET_ID   = process.env.SHEET_ID   || '';
function goldPct()   { return process.env.GOLD_MARGIN_PCT   || '0'; }
function silverPct() { return process.env.SILVER_MARGIN_PCT || '0'; }

// ── Broadcast helper — passed to adapters ─────────────────────
function broadcast() {
  feedServer.broadcast(goldPct(), silverPct());
}

// ── OHLC REST backup — every 5s when Dhan WS is stale ────────
let ohlcCalls = 0, ohlcError = null, ohlcBackoffUntil = 0;

async function pollOHLC() {
  const token    = tokenManager.getToken();
  const clientId = process.env.DHAN_CLIENT_ID || '';
  if (!token || !clientId) return;
  if (Date.now() < ohlcBackoffUntil) return;

  const stats = dhanAdapter.getStats();
  if (stats.wsStatus === 'connected' && stats.tickAgeMs !== null && stats.tickAgeMs < 4000) return;

  const ac = tickEngine.getContracts();
  const secIds = [
    parseInt(ac.gold.secId, 10),
    parseInt(ac.goldNext.secId, 10),
    parseInt(ac.silver.secId, 10),
    parseInt(ac.silverNext.secId, 10),
  ];

  try {
    const r = await axios.post(
      'https://api.dhan.co/v2/marketfeed/ohlc',
      { MCX_COMM: secIds },
      {
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          'access-token': token,
          'client-id': clientId,
        },
        timeout: 5000,
      }
    );
    const seg = r.data?.data?.MCX_COMM;
    if (!seg) { ohlcError = 'No MCX_COMM'; return; }
    ohlcCalls++; ohlcError = null;

    const applyRow = (secId, key) => {
      const row = seg[String(secId)];
      if (!row) return;
      const ltp  = row.last_price || 0;
      const ohlc = row.ohlc || {};
      if (ltp > 0) {
        cache.writeMCX(key, {
          ltp: Math.round(ltp), source: 'dhan_ohlc',
          open: ohlc.open  ? Math.round(ohlc.open)  : 0,
          high: ohlc.high  ? Math.round(ohlc.high)  : 0,
          low:  ohlc.low   ? Math.round(ohlc.low)   : 0,
        });
      }
    };
    applyRow(ac.gold.secId,       'gold');
    applyRow(ac.goldNext.secId,   'goldNext');
    applyRow(ac.silver.secId,     'silver');
    applyRow(ac.silverNext.secId, 'silverNext');
    broadcast();
  } catch (e) {
    ohlcError = e.message;
    if (e.response?.status === 429) {
      ohlcBackoffUntil = Date.now() + 60000;
      console.warn('[OHLC] 429 — backoff 60s');
    }
  }
}

// ── Routes ────────────────────────────────────────────────────
app.get('/rates', (req, res) => {
  res.json(tickEngine.buildPayload(goldPct(), silverPct()));
});

app.get('/debug', (req, res) => {
  const s = cache.get();
  res.json({
    server:    'RR Jewellers v13',
    feed:      feedServer.getStats(),
    dhan:      dhanAdapter.getStats(),
    ...fcsAdapter.getStats(),
    ohlc:      { calls: ohlcCalls, lastError: ohlcError, backoffUntil: ohlcBackoffUntil > Date.now() ? new Date(ohlcBackoffUntil).toISOString() : null },
    rateCache: s,
    contracts: tickEngine.getContracts(),
    tokenMap:  tickEngine.getTokenMap(),
    marketOpen: tickEngine.isMCXOpen(),
    tokenRenewedAt: tokenManager.getTokenRenewedAt(),
    renewAttempts:  tokenManager.getRenewAttempts(),
    env: {
      DHAN_CLIENT_ID:  !!process.env.DHAN_CLIENT_ID,
      DHAN_PIN:        !!process.env.DHAN_PIN,
      DHAN_TOTP_SECRET:!!process.env.DHAN_TOTP_SECRET,
      TWELVE_DATA_KEY: !!process.env.TWELVE_DATA_KEY,
      FCS_API_KEY:     !!process.env.FCS_API_KEY,
      tokenLen:        tokenManager.getToken().length,
    },
  });
});

app.get('/ping', (req, res) => {
  res.json({ ok: true, ts: Date.now(), ...feedServer.getStats(), tokenRenewedAt: tokenManager.getTokenRenewedAt() });
});

app.get('/token-renew', async (req, res) => {
  await tokenManager.renewWithRetry();
  res.json({ tokenRenewedAt: tokenManager.getTokenRenewedAt(), dhan: dhanAdapter.getStats() });
});

app.get('/health', (req, res) => {
  const d = dhanAdapter.getStats();
  const f = fcsAdapter.getStats();
  const ok = d.wsStatus === 'connected' || f.fcs.status === 'connected';
  res.status(ok ? 200 : 503).json({ ok, dhan: d.wsStatus, fcs: f.fcs.status, td: f.td.status });
});

app.get('/updates', async (req, res) => {
  try {
    if (!SHEET_ID) throw new Error('no SHEET_ID');
    const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=Updates`;
    const r   = await axios.get(url, { timeout: 8000 });
    const json = r.data.replace(/.*?({.*}).*/s, '$1');
    const data = JSON.parse(json);
    const rows = data.table.rows.map(row => ({
      date:    row.c[0]?.v || '',
      title:   row.c[1]?.v || '',
      content: row.c[2]?.v || '',
      image:   row.c[3]?.v || '',
    }));
    res.json({ success: true, updates: rows.filter(r => r.title) });
  } catch {
    res.json({ success: true, updates: [{ date: 'Today', title: 'Welcome to R.R. Jewellers', content: 'Live gold & silver rates.', image: '' }] });
  }
});

app.get('/', (req, res) => {
  res.json({
    status: 'RR Jewellers v13',
    dhan:   dhanAdapter.getStats().wsStatus,
    fcs:    fcsAdapter.getStats().fcs.status,
    feed:   feedServer.getStats(),
    endpoints: ['/rates', '/debug', '/ping', '/health', '/token-renew', '/updates', '/feed (WS)'],
  });
});

// ── Startup ───────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', async () => {
  console.log('[STARTUP] RR Jewellers v13 port=%s', PORT);

  // 1. Init WS push server
  feedServer.init(server);

  // 2. Token renew on startup
  tokenManager.onRenew(() => dhanAdapter.connect()); // reconnect Dhan after token renew
  await tokenManager.renewWithRetry();

  // 3. Connect adapters — each broadcasts independently at own speed
  dhanAdapter.onTick(broadcast);
  fcsAdapter.onTick(broadcast);

  dhanAdapter.connect();
  dhanAdapter.startWatchdog();
  fcsAdapter.start();

  // 4. OHLC backup every 5s
  setInterval(pollOHLC, 5000);

  // 5b. Periodic broadcast every 2s — ensure any new client gets data
  //     even if no Dhan tick arrives (market slow/closed)
  setInterval(() => {
    broadcast(); // safe — builds from cache, clients.size check inside
  }, 2000);

  // 5. Daily schedules
  tokenManager.scheduleDailyRenew();
  tokenManager.scheduleDailyHLReset();

  // 6. Self-ping (Render free tier keepalive)
  if (SELF_URL) {
    setInterval(() => {
      axios.get(SELF_URL + '/ping').catch(() => {});
    }, 4 * 60 * 1000);
  }

  console.log('[STARTUP] All systems initialized');
});
