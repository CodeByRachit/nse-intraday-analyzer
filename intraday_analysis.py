import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import concurrent.futures
import warnings
from tqdm import tqdm

warnings.filterwarnings("ignore")

# =====================
# Configuration
# =====================
class Config:
    MIN_DATA_POINTS = 60  # 60 minutes of intraday data
    WORKERS = 10          # Thread pool workers
    TIMEOUT = 10          # Seconds for HTTP requests

# =====================
# Data Validation
# =====================
class MarketDataValidator:
    @staticmethod
    def get_valid_tickers(tickers):
        valid = []
        with tqdm(total=len(tickers), desc="Validating tickers") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=Config.WORKERS) as executor:
                future_to_ticker = {executor.submit(MarketDataValidator._is_valid_ticker, t): t for t in tickers}
                for future in concurrent.futures.as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        if future.result():
                            valid.append(ticker)
                    except Exception as e:
                        print(f"Error validating {ticker}: {str(e)}")
                    pbar.update(1)
        return valid

    @staticmethod
    def _is_valid_ticker(ticker):
        try:
            data = yf.Ticker(ticker).history(period="1d", timeout=Config.TIMEOUT)
            return not data.empty
        except:
            return False

# =====================
# Data Processor
# =====================
class MarketDataProcessor:
    def __init__(self):
        self.ist = pytz.timezone("Asia/Kolkata")
        self.market_day = self._get_market_day()
        self.start_time, self.end_time = self._get_market_hours()

    def _get_market_day(self):
        now = datetime.now(self.ist)
        if now.weekday() >= 5 or now.hour < 9 or now.hour >= 16:
            delta = 1
            while True:
                candidate = now - timedelta(days=delta)
                if candidate.weekday() < 5:
                    return candidate.date()
                delta += 1
        return now.date()

    def _get_market_hours(self):
        base_date = datetime.combine(self.market_day, datetime.min.time())
        market_open = self.ist.localize(
            base_date.replace(hour=9, minute=15)
        market_close = self.ist.localize(
            base_date.replace(hour=15, minute=30)
        return market_open, market_close

    def fetch_intraday_data(self, ticker):
        try:
            data = yf.Ticker(ticker).history(
                interval="1m",
                start=self.start_time,
                end=self.end_time,
                timeout=Config.TIMEOUT
            )
            return data
        except Exception as e:
            print(f"Error fetching {ticker}: {str(e)}")
            return pd.DataFrame()

# =====================
# Analytics Engine
# =====================
class FinancialAnalytics:
    def __init__(self):
        self.processor = MarketDataProcessor()
        self.validator = MarketDataValidator()

    def process_market(self, exchanges):
        all_tickers = []
        for exchange in exchanges:
            if exchange == "NSE":
                all_tickers += [t + ".NS" for t in self._get_nse_symbols()]
            elif exchange == "BSE":
                all_tickers += [t + ".BO" for t in self._get_bse_symbols()]

        valid_tickers = self.validator.get_valid_tickers(all_tickers)
        print(f"\nTotal valid tickers found: {len(valid_tickers)}")

        results = []
        skipped = []
        with tqdm(total=len(valid_tickers), desc="Processing tickers") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=Config.WORKERS) as executor:
                future_to_ticker = {executor.submit(self._process_single_ticker, t): t for t in valid_tickers}
                for future in concurrent.futures.as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                        else:
                            skipped.append(ticker)
                    except Exception as e:
                        print(f"Error processing {ticker}: {str(e)}")
                        skipped.append(ticker)
                    pbar.update(1)

        return results, skipped, len(all_tickers) - len(valid_tickers)

    def _process_single_ticker(self, ticker):
        data = self.processor.fetch_intraday_data(ticker)
        if len(data) < Config.MIN_DATA_POINTS:
            return None

        try:
            returns = data['Close'].pct_change().dropna()
            volatility = returns.std()
            current_price = data['Close'].iloc[-1]
            open_price = data['Close'].iloc[0]
            daily_return_pct = (current_price - open_price) / open_price * 100
            risk_adj_return = daily_return_pct / volatility if volatility != 0 else 0

            market_returns = [r for r in returns if not np.isnan(r)]
            market_mean = np.mean(market_returns)
            market_std = np.std(market_returns)
            z_score = (daily_return_pct - market_mean) / market_std if market_std != 0 else 0

            return {
                "Ticker": ticker,
                "Current Price": current_price,
                "Daily Return (%)": daily_return_pct,
                "Volatility": volatility,
                "Risk-Adj Return": risk_adj_return,
                "Z-Score": z_score
            }
        except Exception as e:
            print(f"Calculation error for {ticker}: {str(e)}")
            return None

    def _get_nse_symbols(self):
        return [
            'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'HINDUNILVR',
            'ICICIBANK', 'KOTAKBANK', 'BHARTIARTL', 'LT', 'SBIN'
        ]

    def _get_bse_symbols(self):
        return [
            'TATASTEEL', 'ONGC', 'ITC', 'SBIN', 'AXISBANK',
            'MARUTI', 'NTPC', 'SUNPHARMA', 'WIPRO', 'DRREDDY'
        ]

# =====================
# Reporting System
# =====================
class AnalyticsReporter:
    @staticmethod
    def generate_report(results, skipped, invalid_count):
        df = pd.DataFrame(results)
        if df.empty:
            print("No valid data to display")
            return

        df['Recommendation'] = np.where(
            (df['Z-Score'] > 1) & (df['Risk-Adj Return'] > 0),
            'Buy',
            np.where(
                (df['Z-Score'] < -1) & (df['Risk-Adj Return'] < 0),
                'Avoid',
                'Neutral'
            )
        )

        print("\n=== Processing Statistics ===")
        print(f"Total tickers scanned: {len(results) + len(skipped) + invalid_count}")
        print(f"Valid tickers found: {len(results) + len(skipped)}")
        print(f"Successfully processed: {len(results)}")
        print(f"Skipped (insufficient data): {len(skipped)}")
        print(f"Invalid tickers ignored: {invalid_count}")

        print("\n=== Top Recommendations ===")
        print(df.sort_values('Risk-Adj Return', ascending=False).head(10))

# =====================
# Main Execution
# =====================
if __name__ == "__main__":
    analytics = FinancialAnalytics()
    results, skipped, invalid_count = analytics.process_market(exchanges=["NSE", "BSE"])
    reporter = AnalyticsReporter()
    reporter.generate_report(results, skipped, invalid_count)