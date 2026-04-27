const express = require('express');
const cors = require('cors');
const axios = require('axios');
const crypto = require('crypto');
const app = express();

app.use(cors());
app.use(express.json());

// Angel One credentials
const CLIENT_ID   = process.env.CLIENT_ID   || 'AAAA238852';
const API_KEY     = process.env.API_KEY      || 'DPAHMIXr';
const SECRET_KEY  = process.env.SECRET_KEY   || '5b96dff9-ef17-42d9-9331-609e8d560bce';
const TOTP_SECRET = process.env.TOTP_SECRET  || 'XXNWX47RXA5KYW3BB45D4CX474';

// TOTP generator
function generateTOTP(secret) {
  const base32Chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const c of secret.toUpperCase()) {
    const val = base32Chars.indexOf(c);
    if (val >= 0) bits += val.toString(2).padStart(5, '0');
  }
  const bytes = [];
  for (let i = 0; i + 8 <= bits.length; i += 8) {
    bytes.push(parseInt(bits.slice(i, i + 8), 2));
  }
  const key = Buffer.from(bytes);
  const time = Math.floor(Date.now() / 1000 / 30);
  const timeBuf = Buffer.alloc(8);
  timeBuf.writeUInt32BE(Math.floor(time / 0x100000000), 0);
  timeBuf.writeUInt32BE(time >>> 0, 4);
  const hmac = crypto.createHmac('sha1', key).update(timeBuf).digest();
  const offset = hmac[hmac.length - 1] & 0xf;
  const code = ((hmac[offset] & 0x7f) << 24 |
                (hmac[offset+1] & 0xff) << 16 |
                (hmac[offset+2] & 0xff) << 8  |
                (hmac[offset+3] & 0xff)) % 1000000;
  return code.toString().padStart(6, '0');
}

// Cache auth token
let authToken = null;
let authExpiry = 0;

async function getAuthToken() {
  if (authToken && Date.now() < authExpiry) return authToken;
  
  const totp = generateTOTP(TOTP_SECRET);
  console.log('Generating TOTP:', totp);
  
  const res = await axios.post('https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword', {
    clientcode: CLIENT_ID,
    password: process.env.ANGEL_PIN || '1857',
    totp: totp
  }, {
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-UserType': 'USER',
      'X-SourceID': 'WEB',
      'X-ClientLocalIP': '127.0.0.1',
      'X-ClientPublicIP': '35.160.120.126',
      'X-MACAddress': '00:00:00:00:00:00',
      'X-PrivateKey': API_KEY
    }
  });
  
  if (res.data.status && res.data.data && res.data.data.jwtToken) {
    authToken = res.data.data.jwtToken;
    authExpiry = Date.now() + (6 * 60 * 60 * 1000); // 6 hours
    console.log('Angel login successful!');
    return authToken;
  }
  throw new Error('Angel login failed: ' + JSON.stringify(res.data));
}

async function getLTPFromAngel(token, symbolToken, exchange) {
  const res = await axios.post('https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/', {
    mode: 'LTP',
    exchangeTokens: { [exchange]: [symbolToken] }
  }, {
    headers: {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-UserType': 'USER',
      'X-SourceID': 'WEB',
      'X-ClientLocalIP': '127.0.0.1',
      'X-ClientPublicIP': '35.160.120.126',
      'X-MACAddress': '00:00:00:00:00:00',
      'X-PrivateKey': API_KEY
    }
  });
  return res.data;
}

app.get('/', (req, res) => {
  res.json({ status: 'Gold Proxy Server with Angel One Running' });
});

app.get('/rates', async (req, res) => {
  try {
    const token = await getAuthToken();
    
    // MCX Gold token: 57281 (Gold Jun 2025)
    // MCX Silver token: 57295 (Silver Jun 2025)
    const [goldData, silverData] = await Promise.all([
      getLTPFromAngel(token, '57281', 'MCX'),
      getLTPFromAngel(token, '57295', 'MCX')
    ]);
    
    const goldLTP = goldData.data?.fetched?.[0]?.ltp || 0;
    const silverLTP = silverData.data?.fetched?.[0]?.ltp || 0;
    
    // MCX Gold is per 10g, Silver is per kg
    const goldPer10g = goldLTP;
    const silverPerKg = silverLTP;
    
    // Also get forex for SPOT table
    const fxRes = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 5000 });
    const usdInr = fxRes.data.rates.INR;
    const xauUsd = (goldPer10g / 10 / usdInr) * 31.1035;
    const xagUsd = (silverPerKg / 1000 / usdInr) * 31.1035;
    
    res.json({
      success: true,
      source: 'angel_mcx',
      goldPer10g: Math.round(goldPer10g),
      silverPerKg: Math.round(silverPerKg),
      xauUsd: parseFloat(xauUsd.toFixed(2)),
      xagUsd: parseFloat(xagUsd.toFixed(2)),
      usdInr: parseFloat(usdInr.toFixed(2)),
      timestamp: new Date().toISOString()
    });
    
  } catch (angelErr) {
    console.log('Angel failed:', angelErr.message, '- using fallback');
    
    // Fallback to goldprice.org
    try {
      const goldRes = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Referer': 'https://www.goldprice.org/',
          'Origin': 'https://www.goldprice.org'
        },
        timeout: 8000
      });
      const fxRes = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 8000 });
      
      const xauUsd = goldRes.data.items[0].xauPrice;
      const xagUsd = goldRes.data.items[0].xagPrice;
      const usdInr = fxRes.data.rates.INR;
      
      res.json({
        success: true,
        source: 'fallback_goldprice',
        goldPer10g: Math.round((xauUsd/31.1035)*10*usdInr),
        silverPerKg: Math.round((xagUsd/31.1035)*1000*usdInr),
        xauUsd, xagUsd, usdInr,
        timestamp: new Date().toISOString()
      });
    } catch (e) {
      res.json({ success: false, error: e.message });
    }
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Server running on port ' + PORT));
