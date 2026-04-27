const express = require('express');
const cors = require('cors');
const axios = require('axios');
const app = express();

app.use(cors());
app.use(express.json());

// Health check
app.get('/', (req, res) => {
  res.json({ status: 'Gold Proxy Server Running' });
});

// Goldprice.org rates
app.get('/rates', async (req, res) => {
  try {
    const gold = await axios.get('https://data-asg.goldprice.org/dbXRates/USD');
    const fx = await axios.get('https://api.frankfurter.app/latest?from=USD&to=INR');
    
    const xauUsd = gold.data.items[0].xauPrice;
    const xagUsd = gold.data.items[0].xagPrice;
    const usdInr = fx.data.rates.INR;
    
    const goldPer10g = (xauUsd / 31.1035) * 10 * usdInr;
    const silverPerKg = (xagUsd / 31.1035) * 1000 * usdInr;
    
    res.json({
      success: true,
      xauUsd: xauUsd,
      xagUsd: xagUsd,
      usdInr: usdInr,
      goldPer10g: Math.round(goldPer10g),
      silverPerKg: Math.round(silverPerKg),
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    res.json({ success: false, error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log('Server running on port ' + PORT);
});
