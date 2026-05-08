'use strict';
// ── tokenManager.js ───────────────────────────────────────────
// Pure Node.js TOTP (RFC 6238) — zero npm deps.
// Manages Dhan access token lifecycle.

const crypto = require('crypto');
const axios  = require('axios');

let currentToken   = process.env.DHAN_ACCESS_TOKEN || '';
let tokenRenewedAt = null;
let renewAttempts  = 0;
let renewTimer     = null;
let onRenewSuccess = null; // callback(newToken)

// ── TOTP ─────────────────────────────────────────────────────
function base32Decode(s) {
  const alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  s = s.toUpperCase().replace(/=+$/, '').replace(/\s/g, '');
  let bits = 0, val = 0;
  const out = [];
  for (const ch of s) {
    const idx = alpha.indexOf(ch);
    if (idx === -1) continue;
    val = (val << 5) | idx; bits += 5;
    if (bits >= 8) { bits -= 8; out.push((val >> bits) & 0xFF); }
  }
  return Buffer.from(out);
}

function getTOTP(secret) {
  if (!secret) return null;
  try {
    const key = base32Decode(secret);
    const t = Math.floor(Date.now() / 1000 / 30);
    const tb = Buffer.alloc(8);
    tb.writeUInt32BE(Math.floor(t / 0x100000000), 0);
    tb.writeUInt32BE(t >>> 0, 4);
    const hmac = crypto.createHmac('sha1', key).update(tb).digest();
    const offset = hmac[19] & 0xF;
    const code = ((hmac[offset] & 0x7F) << 24 | (hmac[offset+1] & 0xFF) << 16 |
                  (hmac[offset+2] & 0xFF) << 8  | (hmac[offset+3] & 0xFF)) % 1000000;
    return String(code).padStart(6, '0');
  } catch (e) {
    console.error('[TOKEN] TOTP error:', e.message);
    return null;
  }
}

// ── Apply token ───────────────────────────────────────────────
function applyToken(token, source) {
  currentToken   = token;
  tokenRenewedAt = new Date().toISOString();
  renewAttempts  = 0;
  if (renewTimer) { clearTimeout(renewTimer); renewTimer = null; }
  console.log('[TOKEN] ✅ Token applied via %s len=%d at %s', source, token.length, tokenRenewedAt);
  if (onRenewSuccess) onRenewSuccess(token);
}

// ── Renew logic ───────────────────────────────────────────────
async function renewToken() {
  const clientId = process.env.DHAN_CLIENT_ID || '';
  if (!clientId) { console.warn('[TOKEN] DHAN_CLIENT_ID missing'); return false; }

  // Method 0: Env var updated externally (Render dashboard)
  const envToken = process.env.DHAN_ACCESS_TOKEN || '';
  if (envToken && envToken !== currentToken && envToken.length > 100) {
    console.log('[TOKEN] Env var updated — adopting new token');
    applyToken(envToken, 'env-update');
    return true;
  }

  // Method 1: generateAccessToken via TOTP
  // Tries multiple known Dhan auth endpoints
  const pin    = process.env.DHAN_PIN || '';
  const secret = process.env.DHAN_TOTP_SECRET || '';
  if (pin && secret) {
    const totp = getTOTP(secret);
    if (totp) {
      const endpoints = [
        // Query param style
        { url: 'https://auth.dhan.co/app/generateAccessToken', params: { dhanClientId: clientId, pin, totp }, data: {} },
        // Body style
        { url: 'https://api.dhan.co/v2/token/generate', params: {}, data: { dhanClientId: clientId, pin, totp } },
        // Alt endpoint
        { url: 'https://auth.dhan.co/login', params: {}, data: { dhanClientId: clientId, pin, totp } },
      ];
      for (const ep of endpoints) {
        try {
          const r = await axios.post(ep.url, ep.data, {
            params: ep.params,
            headers: { 'Content-Type': 'application/json' },
            timeout: 15000,
          });
          const t = r.data?.accessToken || r.data?.access_token || r.data?.data?.accessToken;
          if (t && t.length > 100) {
            console.log('[TOKEN] TOTP success via', ep.url);
            applyToken(t, 'TOTP');
            return true;
          }
          console.warn('[TOKEN] TOTP no token from', ep.url, JSON.stringify(r.data).slice(0, 80));
        } catch (e) {
          const s = e.response?.status;
          console.warn('[TOKEN] TOTP fail', ep.url, s, e.message.slice(0, 60));
          if (s === 429) break; // rate limited — stop trying
        }
      }
    }
  } else {
    if (!pin)    console.warn('[TOKEN] DHAN_PIN not set — add to Render env vars');
    if (!secret) console.warn('[TOKEN] DHAN_TOTP_SECRET not set — add to Render env vars');
  }

  // Method 2: RenewToken API (works only if current token still valid)
  if (currentToken) {
    try {
      const r = await axios.post(
        'https://api.dhan.co/v2/RenewToken',
        {},
        {
          headers: {
            'access-token': currentToken,
            'dhanClientId': clientId,
            'Content-Type': 'application/json',
          },
          timeout: 12000,
        }
      );
      const t = r.data?.accessToken || r.data?.access_token || r.data?.data?.accessToken;
      if (t && t.length > 100) { applyToken(t, 'RenewToken'); return true; }
    } catch (e) {
      const s = e.response?.status;
      if (s === 400 || s === 401) {
        console.warn('[TOKEN] RenewToken 400/401 — token expired. Set DHAN_PIN+DHAN_TOTP_SECRET on Render.');
      } else {
        console.warn('[TOKEN] RenewToken fail:', s, e.message.slice(0, 60));
      }
    }
  }

  return false;
}

// ── Retry with backoff ────────────────────────────────────────
async function renewWithRetry() {
  const ok = await renewToken();
  if (!ok) {
    renewAttempts++;
    const delay = Math.min(renewAttempts * 3 * 60 * 1000, 15 * 60 * 1000); // 3→6→9→12→15 min
    console.warn('[TOKEN] All methods failed. Retry #%d in %dm', renewAttempts, (delay/60000).toFixed(0));
    renewTimer = setTimeout(renewWithRetry, delay);
  }
}

// ── Daily 8:30 AM IST schedule ────────────────────────────────
function msUntilIST(hh, mm) {
  const nowMs = Date.now();
  const ist   = new Date(nowMs + 5.5 * 3600000);
  const y = ist.getUTCFullYear(), mo = ist.getUTCMonth(), d = ist.getUTCDate();
  const utcMin = hh * 60 + mm - 330;
  const utcH = Math.floor(utcMin / 60), utcM = utcMin % 60;
  let target = new Date(Date.UTC(y, mo, d, (utcH + 24) % 24, utcM, 0, 0));
  if (target.getTime() <= nowMs) target.setUTCDate(target.getUTCDate() + 1);
  return target.getTime() - nowMs;
}

function scheduleDailyRenew() {
  const ms = msUntilIST(8, 30);
  console.log('[TOKEN] Daily renew scheduled in %dm', (ms / 60000).toFixed(0));
  setTimeout(() => {
    console.log('[TOKEN] 8:30 AM IST — generating fresh token');
    renewWithRetry();
    scheduleDailyRenew();
  }, ms);
}

// ── H/L reset schedule ───────────────────────────────────────
function scheduleDailyHLReset(resetFn) {
  const { resetDailyHL } = require('./cacheEngine');
  const ms = msUntilIST(9, 0);
  setTimeout(() => {
    resetDailyHL();
    scheduleDailyHLReset(resetFn);
  }, ms);
}

function getToken()          { return currentToken; }
function getTokenRenewedAt() { return tokenRenewedAt; }
function getRenewAttempts()  { return renewAttempts; }
function onRenew(fn)         { onRenewSuccess = fn; }

module.exports = {
  renewWithRetry, scheduleDailyRenew, scheduleDailyHLReset,
  getToken, getTokenRenewedAt, getRenewAttempts, onRenew,
};
