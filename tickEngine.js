'use strict';
// ── tickEngine.js ─────────────────────────────────────────────
// Builds normalized payload from cacheEngine state.
// Called by feedServer to broadcast to HTML WS clients.

const cache = require('./cacheEngine');

let contracts = {
  gold:        { secId: '459277', display: 'GOLD JUN26',    expiry: '2026-06-05' },
  goldNext:    { secId: '466583', display: 'GOLD AUG26',    expiry: '2026-08-05' },
  silver:      { secId: '464150', display: 'SILVER JUL26',  expiry: '2026-07-03' },
  silverNext:  { secId: '471725', display: 'SILVER SEP26',  expiry: '2026-09-04' },
};

// All known contracts for auto-selection
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

function pickCurrentAndNext(list) {
  const now = new Date();
  const sorted = list
    .map(c => ({ ...c, ed: new Date(c.expiry) }))
    .filter(c => !isNaN(c.ed))
    .sort((a, b) => a.ed - b.ed);
  const upcoming = sorted.filter(c => c.ed >= now);
  if (upcoming.length >= 2) return { current: upcoming[0], next: upcoming[1] };
  if (upcoming.length === 1) return { current: sorted[sorted.length - 2] || upcoming[0], next: upcoming[0] };
  const last = sorted.slice(-2);
  return { current: last[0] || sorted[0], next: last[1] || sorted[0] };
}

function refreshContracts() {
  const g = pickCurrentAndNext(GOLD_CONTRACTS);
  const s = pickCurrentAndNext(SILVER_CONTRACTS);
  contracts = {
    gold:       g.current,
    goldNext:   g.next,
    silver:     s.current,
    silverNext: s.next,
  };
}

function getTokenMap() {
  return {
    [contracts.gold.secId]:       'gold',
    [contracts.goldNext.secId]:   'goldNext',
    [contracts.silver.secId]:     'silver',
    [contracts.silverNext.secId]: 'silverNext',
  };
}

function getContracts() { return contracts; }

function isMCXOpen() {
  const d = new Date(Date.now() + 5.5 * 3600000);
  const dow = d.getUTCDay();
  const t = d.getUTCHours() * 60 + d.getUTCMinutes();
  if (dow === 0) return false;
  return dow === 6 ? (t >= 540 && t < 840) : (t >= 540 && t < 1435);
}

function n(v) { return v > 0 ? v : null; }

// Build payload — called on every broadcast
function buildPayload(goldMarginPct, silverMarginPct) {
  const s = cache.get();
  const gm = parseFloat(goldMarginPct || 0);
  const sm = parseFloat(silverMarginPct || 0);
  const gSell = s.goldLtp   > 0 ? Math.round(s.goldLtp   * (1 + gm / 100)) : null;
  const sSell = s.silverLtp > 0 ? Math.round(s.silverLtp * (1 + sm / 100)) : null;

  return {
    ts:         Date.now(),
    src:        s.mcxSource,
    fxSrc:      s.fxSource,
    mktOpen:    isMCXOpen(),
    goldSell:   gSell,
    silverSell: sSell,
    f: {
      g:  { ltp:n(s.goldLtp),      bid:n(s.goldBid),      ask:n(s.goldAsk),
            high:n(s.goldHigh),    low:n(s.goldLow),      open:n(s.goldOpen),
            pc:n(s.goldPrevClose),
            con: contracts.gold.display,    exp: contracts.gold.expiry },
      gN: { ltp:n(s.goldNextLtp),   bid:n(s.goldNextBid),  ask:n(s.goldNextAsk),
            high:n(s.goldNextHigh), low:n(s.goldNextLow),
            con: contracts.goldNext.display, exp: contracts.goldNext.expiry },
      s:  { ltp:n(s.silverLtp),    bid:n(s.silverBid),    ask:n(s.silverAsk),
            high:n(s.silverHigh),  low:n(s.silverLow),    open:n(s.silverOpen),
            pc:n(s.silverPrevClose),
            con: contracts.silver.display,  exp: contracts.silver.expiry },
      sN: { ltp:n(s.silverNextLtp), bid:n(s.silverNextBid),ask:n(s.silverNextAsk),
            high:n(s.silverNextHigh),low:n(s.silverNextLow),
            con: contracts.silverNext.display, exp: contracts.silverNext.expiry },
    },
    sp: {
      xauUsd: n(s.xauUsd), xauBid: n(s.xauBid), xauAsk: n(s.xauAsk),
      xauHigh: n(s.xauHigh), xauLow: n(s.xauLow),
      xagUsd: n(s.xagUsd), xagBid: n(s.xagBid), xagAsk: n(s.xagAsk),
      xagHigh: n(s.xagHigh), xagLow: n(s.xagLow),
      usdInr: n(s.usdInr), usdInrBid: n(s.usdInrBid), usdInrAsk: n(s.usdInrAsk),
      usdInrHigh: n(s.usdInrHigh), usdInrLow: s.usdInrLow === Infinity ? null : n(s.usdInrLow),
    },
    margin: { g: gm, s: sm },
    // Legacy compat fields
    success: true,
    source: s.mcxSource,
    goldPer10g: gSell,
    silverPerKg: sSell,
    xauUsd: n(s.xauUsd),
    xagUsd: n(s.xagUsd),
    usdInr: n(s.usdInr),
  };
}

// Initialize
refreshContracts();
// Refresh contracts daily
setInterval(refreshContracts, 6 * 3600 * 1000);

module.exports = { buildPayload, getTokenMap, getContracts, isMCXOpen, refreshContracts };
