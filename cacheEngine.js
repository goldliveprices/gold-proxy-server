'use strict';
// ── cacheEngine.js ────────────────────────────────────────────
// Single source of truth. All adapters write here.
// tickEngine reads and normalizes before broadcast.

const state = {
  // MCX Futures (from Dhan WS)
  goldLtp: 0, goldBid: 0, goldAsk: 0,
  goldHigh: 0, goldLow: 0, goldOpen: 0, goldPrevClose: 0,
  goldNextLtp: 0, goldNextBid: 0, goldNextAsk: 0,
  goldNextHigh: 0, goldNextLow: 0,
  silverLtp: 0, silverBid: 0, silverAsk: 0,
  silverHigh: 0, silverLow: 0, silverOpen: 0, silverPrevClose: 0,
  silverNextLtp: 0, silverNextBid: 0, silverNextAsk: 0,
  silverNextHigh: 0, silverNextLow: 0,

  // Spot Forex (from FCS WS primary, TD/REST fallback)
  xauUsd: 0, xauBid: 0, xauAsk: 0, xauHigh: 0, xauLow: 0,
  xagUsd: 0, xagBid: 0, xagAsk: 0, xagHigh: 0, xagLow: 0,
  usdInr: 0, usdInrBid: 0, usdInrAsk: 0, usdInrHigh: 0, usdInrLow: Infinity,

  // Meta
  mcxSource: 'init',    // dhan_ws | dhan_ohlc | init
  fxSource: 'init',     // fcs_ws | td_ws | td_rest | frankfurter | init
  mcxUpdatedAt: 0,      // ms timestamp
  fxUpdatedAt: 0,
  xauUpdatedAt: 0,
  xagUpdatedAt: 0,
  usdInrUpdatedAt: 0,
};

// Write MCX tick — returns true if any value changed
function writeMCX(key, tick) {
  let changed = false;
  const map = {
    gold:        ['goldLtp','goldBid','goldAsk','goldHigh','goldLow','goldOpen','goldPrevClose'],
    goldNext:    ['goldNextLtp','goldNextBid','goldNextAsk','goldNextHigh','goldNextLow'],
    silver:      ['silverLtp','silverBid','silverAsk','silverHigh','silverLow','silverOpen','silverPrevClose'],
    silverNext:  ['silverNextLtp','silverNextBid','silverNextAsk','silverNextHigh','silverNextLow'],
  };
  const fields = map[key]; if (!fields) return false;
  const prefix = key.replace('Next','').replace('gold','gold').replace('silver','silver');

  if (tick.ltp > 0 && tick.ltp !== state[fields[0]]) {
    state[fields[0]] = tick.ltp; changed = true;
  }
  if (fields[1] && tick.bid > 0 && tick.bid !== state[fields[1]]) { state[fields[1]] = tick.bid; changed = true; }
  if (fields[2] && tick.ask > 0 && tick.ask !== state[fields[2]]) { state[fields[2]] = tick.ask; changed = true; }
  if (fields[3] && tick.high > 0 && tick.high !== state[fields[3]]) { state[fields[3]] = tick.high; changed = true; }
  if (fields[4] && tick.low  > 0 && tick.low  !== state[fields[4]]) { state[fields[4]] = tick.low;  changed = true; }
  if (fields[5] && tick.open > 0 && tick.open !== state[fields[5]]) { state[fields[5]] = tick.open; changed = true; }
  if (fields[6] && tick.prevClose > 0) { state[fields[6]] = tick.prevClose; }

  if (changed) {
    state.mcxSource = tick.source || 'dhan_ws';
    state.mcxUpdatedAt = Date.now();
  }
  return changed;
}

// Write FX tick — returns true if changed
function writeFX(sym, data) {
  let changed = false;
  const now = Date.now();

  if (sym === 'XAU' && data.price > 3000 && data.price < 9000) {
    if (data.price !== state.xauUsd) { state.xauUsd = round(data.price, 2); changed = true; }
    if (data.bid > 0) state.xauBid = round(data.bid, 2);
    if (data.ask > 0) state.xauAsk = round(data.ask, 2);
    if (data.high > 0) state.xauHigh = round(data.high, 2);
    if (data.low  > 0) state.xauLow  = round(data.low,  2);
    state.xauUpdatedAt = now;
  }
  else if (sym === 'XAG' && data.price > 20 && data.price < 300) {
    if (data.price !== state.xagUsd) { state.xagUsd = round(data.price, 3); changed = true; }
    if (data.bid > 0) state.xagBid = round(data.bid, 3);
    if (data.ask > 0) state.xagAsk = round(data.ask, 3);
    if (data.high > 0) state.xagHigh = round(data.high, 3);
    if (data.low  > 0) state.xagLow  = round(data.low,  3);
    state.xagUpdatedAt = now;
  }
  else if (sym === 'INR' && data.price > 70 && data.price < 115) {
    if (data.price !== state.usdInr) { state.usdInr = round(data.price, 2); changed = true; }
    if (data.bid > 0) state.usdInrBid = round(data.bid, 2);
    if (data.ask > 0) state.usdInrAsk = round(data.ask, 2);
    if (data.high > 0) state.usdInrHigh = round(data.high, 2);
    if (data.low  > 0 && data.low < state.usdInrLow) state.usdInrLow = round(data.low, 2);
    state.usdInrUpdatedAt = now;
  }

  if (changed) {
    state.fxSource = data.source || 'fcs_ws';
    state.fxUpdatedAt = now;
  }
  return changed;
}

// Daily H/L reset at 9:00 AM IST
function resetDailyHL() {
  state.xauHigh = 0; state.xauLow = 0;
  state.xagHigh = 0; state.xagLow = 0;
  state.usdInrHigh = 0; state.usdInrLow = Infinity;
  state.goldHigh = 0; state.goldLow = 0;
  state.silverHigh = 0; state.silverLow = 0;
  state.goldNextHigh = 0; state.goldNextLow = 0;
  state.silverNextHigh = 0; state.silverNextLow = 0;
  console.log('[CACHE] Daily H/L reset at 9:00 AM IST');
}

function round(v, d) { const m = Math.pow(10, d); return Math.round(v * m) / m; }
function get() { return state; }

module.exports = { writeMCX, writeFX, resetDailyHL, get };
