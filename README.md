# 📈 NSE Intraday Stock Analyzer

This project analyzes **real-time intraday performance** of selected NSE (India) stocks using `yfinance`. It helps identify top gainers/losers, volatility, and risk-adjusted returns across different time slices.

---

## 🔧 Features

- Fetches **1-minute intraday data** from NSE via Yahoo Finance
- Calculates:
  - Daily return (%)
  - Last 1 hour return (%)
  - Last 15 minutes return (%)
  - Volatility
  - Risk-adjusted return
- Highlights:
  - Top 5 & Bottom 5 stocks based on return
  - Live market snapshots
- Handles weekends, holidays, and data gaps
- Can analyze either:
  - A fixed list of top 22 NSE stocks (default)
  - All NSE stocks using `nsetools` (optional)

---

## 📦 Requirements

Install dependencies using:

```bash
pip install yfinance pandas numpy pytz nsetools
