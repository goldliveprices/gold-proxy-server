const express = require('express');
const cors = require('cors');
const axios = require('axios');
const app = express();

app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
  res.json({ status: 'Gold Proxy Server Running' });
});

app.get('/rates', async (req, res) => {
  try {
    // Try goldprice.org with browser headers
    const goldRes = await axios.get('https://data-asg.goldprice.org/dbXRates/USD', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.goldprice.org/',
        'Origin': 'https://www.goldprice.org'
      },
      timeout: 8000
    });

    const fxRes = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', {
      timeout: 8000
    });

    const xauUsd = goldRes.data.items[0].xauPrice;
    const xagUsd = goldRes.data.items[0].xagPrice;
    const usdInr = fxRes.data.rates.INR;

    const goldPer10g = Math.round((xauUsd / 31.1035) * 10 * usdInr);
    const silverPerKg = Math.round((xagUsd / 31.1035) * 1000 * usdInr);

    res.json({
      success: true,
      xauUsd, xagUsd, usdInr,
      goldPer10g,
      silverPerKg,
      timestamp: new Date().toISOString()
    });

  } catch (err) {
    // Fallback to metals-api free
    try {
      const fxRes = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR', { timeout: 8000 });
      const usdInr = fxRes.data.rates.INR;

      // Use fixed base rates if all else fails
      const xauUsd = 4720;
      const xagUsd = 76.4;

      res.json({
        success: true,
        xauUsd, xagUsd, usdInr,
        goldPer10g: Math.round((xauUsd / 31.1035) * 10 * usdInr),
        silverPerKg: Math.round((xagUsd / 31.1035) * 1000 * usdInr),
        source: 'fallback',
        timestamp: new Date().toISOString()
      });
    } catch (e) {
      res.json({ success: false, error: e.message });
    }
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Server running on port ' + PORT));
