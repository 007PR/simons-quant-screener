"""
Data Fetcher for Indian Stock Markets
======================================

This module handles data acquisition for NSE/BSE stocks.
Multiple data source options with fallbacks.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
import time


class DataFetcher:
    """
    Fetches historical data for Indian stocks.
    Supports multiple data sources with fallback options.
    """
    
    def __init__(self, cache_dir: str = "./data_cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        # NSE Nifty 50 symbols (add .NS for Yahoo Finance)
        self.nifty50 = [
            "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
            "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK",
            "LT", "HCLTECH", "AXISBANK", "ASIANPAINT", "MARUTI",
            "SUNPHARMA", "TITAN", "BAJFINANCE", "DMART", "ULTRACEMCO",
            "NTPC", "NESTLEIND", "WIPRO", "M&M", "POWERGRID",
            "ADANIENT", "ADANIPORTS", "JSWSTEEL", "TATASTEEL", "HINDALCO",
            "TATAMOTORS", "BAJAJFINSV", "TECHM", "ONGC", "COALINDIA",
            "GRASIM", "BPCL", "BRITANNIA", "DIVISLAB", "DRREDDY",
            "CIPLA", "EICHERMOT", "HEROMOTOCO", "APOLLOHOSP", "INDUSINDBK",
            "SBILIFE", "HDFCLIFE", "UPL", "TATACONSUM", "BAJAJ-AUTO"
        ]
        
        # Nifty Next 50 symbols
        self.niftynext50 = [
            "ADANIGREEN", "AMBUJACEM", "AUROPHARMA", "BAJAJHLDNG", "BANKBARODA",
            "BERGEPAINT", "BIOCON", "BOSCHLTD", "CHOLAFIN", "COLPAL",
            "DABUR", "DLF", "GAIL", "GODREJCP", "HAVELLS",
            "ICICIPRULI", "ICICIGI", "INDIGO", "IOC", "JINDALSTEL",
            "LICI", "LUPIN", "MARICO", "MCDOWELL-N", "MOTHERSON",
            "MUTHOOTFIN", "NAUKRI", "NHPC", "PIDILITIND", "PFC",
            "RECLTD", "SAIL", "SBICARD", "SRF", "SHREECEM",
            "SIEMENS", "TATAPOWER", "TATACHEM", "TORNTPHARM", "TRENT",
            "TVSLMOTOR", "VEDL", "ZOMATO", "ZYDUSLIFE", "ABB",
            "ATGL", "CANBK", "HAL", "IRFC", "PNB"
        ]
        
        # High liquidity mid-caps
        self.midcaps = [
            "POLYCAB", "PIIND", "PERSISTENT", "COFORGE", "LTIM",
            "MPHASIS", "ASHOKLEY", "CUMMINSIND", "PAGEIND", "VOLTAS",
            "OBEROIRLTY", "GODREJPROP", "PRESTIGE", "MAXHEALTH", "ALKEM",
            "IPCALAB", "LAURUSLABS", "GMRINFRA", "IRCTC", "INDHOTEL",
            "VBL", "TVSMOTOR", "ESCORTS", "MRF", "ASTRAL",
            "SUPREMEIND", "AFFLE", "TANLA", "LTTS", "CROMPTON",
            "BLUEDART", "CONCOR", "FEDERALBNK", "IDFCFIRSTB", "RBLBANK"
        ]
    
    def get_universe(self, universe: str = "nifty200") -> list:
        """Get list of symbols for a given universe"""
        
        if universe == "nifty50":
            return self.nifty50
        elif universe == "nifty100":
            return self.nifty50 + self.niftynext50[:50]
        elif universe == "nifty200":
            return self.nifty50 + self.niftynext50 + self.midcaps
        else:
            return self.nifty50
    
    def fetch_yfinance(self, symbol: str, days: int = 365) -> pd.DataFrame:
        """
        Fetch data using yfinance (Yahoo Finance).
        Requires: pip install yfinance
        """
        try:
            import yfinance as yf
            
            # Add .NS suffix for NSE stocks
            yf_symbol = f"{symbol}.NS"
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            ticker = yf.Ticker(yf_symbol)
            df = ticker.history(start=start_date, end=end_date)
            
            if len(df) == 0:
                # Try BSE suffix
                yf_symbol = f"{symbol}.BO"
                ticker = yf.Ticker(yf_symbol)
                df = ticker.history(start=start_date, end=end_date)
            
            # Standardize column names
            df.columns = [c.lower() for c in df.columns]
            
            return df
        
        except ImportError:
            print("yfinance not installed. Run: pip install yfinance")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            return pd.DataFrame()
    
    def fetch_nsetools(self, symbol: str) -> pd.DataFrame:
        """
        Fetch data using nsetools/nsepy.
        Requires: pip install nsepy
        """
        try:
            from nsepy import get_history
            
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=365)
            
            df = get_history(symbol=symbol, start=start_date, end=end_date)
            
            # Standardize column names
            df.columns = [c.lower() for c in df.columns]
            
            # Rename columns to match our standard
            column_mapping = {
                'symbol': 'symbol',
                'series': 'series',
                'prev close': 'prev_close',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'vwap': 'vwap',
                'volume': 'volume',
                'turnover': 'turnover',
                'trades': 'trades',
                'deliverable volume': 'deliverable_volume',
                '%deliverble': 'delivery_pct'
            }
            df = df.rename(columns=column_mapping)
            
            return df
        
        except ImportError:
            print("nsepy not installed. Run: pip install nsepy")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            return pd.DataFrame()
    
    def fetch_jugaad(self, symbol: str, days: int = 365) -> pd.DataFrame:
        """
        Fetch data using jugaad-data.
        Requires: pip install jugaad-data
        """
        try:
            from jugaad_data.nse import stock_df
            
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            df = stock_df(symbol=symbol, from_date=start_date, to_date=end_date)
            
            # Standardize column names
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]
            
            # Ensure proper column names
            if 'ltp' in df.columns:
                df = df.rename(columns={'ltp': 'close'})
            
            return df
        
        except ImportError:
            print("jugaad-data not installed. Run: pip install jugaad-data")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            return pd.DataFrame()
    
    def fetch_from_csv(self, symbol: str, csv_dir: str = "./csv_data") -> pd.DataFrame:
        """
        Load data from local CSV files.
        Expected format: symbol.csv with columns: date,open,high,low,close,volume
        """
        filepath = os.path.join(csv_dir, f"{symbol}.csv")
        
        if not os.path.exists(filepath):
            print(f"CSV file not found: {filepath}")
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(filepath, parse_dates=['date'], index_col='date')
            df.columns = [c.lower() for c in df.columns]
            return df
        except Exception as e:
            print(f"Error loading CSV for {symbol}: {e}")
            return pd.DataFrame()
    
    def fetch_stock(self, symbol: str, source: str = "yfinance", days: int = 365) -> pd.DataFrame:
        """
        Fetch stock data from specified source.
        
        Args:
            symbol: Stock symbol (without exchange suffix)
            source: Data source ('yfinance', 'nsepy', 'jugaad', 'csv')
            days: Number of days of history
        
        Returns:
            DataFrame with OHLCV data
        """
        
        if source == "yfinance":
            return self.fetch_yfinance(symbol, days)
        elif source == "nsepy":
            return self.fetch_nsetools(symbol)
        elif source == "jugaad":
            return self.fetch_jugaad(symbol, days)
        elif source == "csv":
            return self.fetch_from_csv(symbol)
        else:
            print(f"Unknown source: {source}")
            return pd.DataFrame()
    
    def fetch_universe(self, universe: str = "nifty50", source: str = "yfinance", 
                       days: int = 365, delay: float = 0.5) -> dict:
        """
        Fetch data for entire universe of stocks.
        
        Args:
            universe: Stock universe ('nifty50', 'nifty100', 'nifty200')
            source: Data source
            days: Number of days of history
            delay: Delay between requests to avoid rate limiting
        
        Returns:
            Dict of {symbol: DataFrame}
        """
        
        symbols = self.get_universe(universe)
        stock_data = {}
        
        print(f"Fetching {len(symbols)} stocks from {source}...")
        
        for i, symbol in enumerate(symbols):
            print(f"  [{i+1}/{len(symbols)}] {symbol}...", end=" ")
            
            df = self.fetch_stock(symbol, source, days)
            
            if len(df) > 0:
                stock_data[symbol] = df
                print(f"OK ({len(df)} rows)")
            else:
                print("FAILED")
            
            if delay > 0:
                time.sleep(delay)
        
        print(f"\nFetched {len(stock_data)}/{len(symbols)} stocks successfully")
        return stock_data
    
    def fetch_index(self, index: str = "NIFTY50", source: str = "yfinance", 
                    days: int = 365) -> pd.DataFrame:
        """Fetch index data for benchmark comparison"""
        
        index_symbols = {
            "NIFTY50": "^NSEI",
            "NIFTYBANK": "^NSEBANK",
            "NIFTYMIDCAP": "NIFTYMIDCAP50.NS",
            "SENSEX": "^BSESN"
        }
        
        if source == "yfinance":
            try:
                import yfinance as yf
                
                yf_symbol = index_symbols.get(index, "^NSEI")
                
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                
                ticker = yf.Ticker(yf_symbol)
                df = ticker.history(start=start_date, end=end_date)
                df.columns = [c.lower() for c in df.columns]
                
                return df
            
            except Exception as e:
                print(f"Error fetching index: {e}")
                return pd.DataFrame()
        
        return pd.DataFrame()
    
    def cache_data(self, stock_data: dict, filename: str = "stock_cache.pkl"):
        """Cache fetched data to disk"""
        import pickle
        
        filepath = os.path.join(self.cache_dir, filename)
        
        # Convert DataFrames to dict format for JSON serialization
        cache_data = {
            'timestamp': datetime.now().isoformat(),
            'data': {sym: df.to_dict() for sym, df in stock_data.items()}
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(cache_data, f)
        
        print(f"Data cached to {filepath}")
    
    def load_cache(self, filename: str = "stock_cache.pkl", max_age_hours: int = 24) -> dict:
        """Load cached data if fresh enough"""
        import pickle
        
        filepath = os.path.join(self.cache_dir, filename)
        
        if not os.path.exists(filepath):
            return {}
        
        try:
            with open(filepath, 'rb') as f:
                cache_data = pickle.load(f)
            
            # Check age
            cache_time = datetime.fromisoformat(cache_data['timestamp'])
            age_hours = (datetime.now() - cache_time).total_seconds() / 3600
            
            if age_hours > max_age_hours:
                print(f"Cache too old ({age_hours:.1f} hours)")
                return {}
            
            # Convert back to DataFrames
            stock_data = {}
            for sym, data in cache_data['data'].items():
                df = pd.DataFrame(data)
                df.index = pd.to_datetime(df.index)
                stock_data[sym] = df
            
            print(f"Loaded {len(stock_data)} stocks from cache ({age_hours:.1f} hours old)")
            return stock_data
        
        except Exception as e:
            print(f"Error loading cache: {e}")
            return {}


def validate_data(df: pd.DataFrame) -> bool:
    """Validate that DataFrame has required columns and data"""
    
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    
    for col in required_columns:
        if col not in df.columns:
            return False
    
    if len(df) < 60:  # Need at least 60 days
        return False
    
    if df['close'].isna().sum() > len(df) * 0.1:  # More than 10% missing
        return False
    
    return True


def generate_sample_data(symbols: list, days: int = 365) -> dict:
    """
    Generate synthetic sample data for testing.
    Useful when you can't fetch real data.
    """
    
    print("Generating sample data for testing...")
    
    stock_data = {}
    dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
    
    for symbol in symbols:
        np.random.seed(hash(symbol) % (2**32))
        
        # Random starting price between 100 and 5000
        start_price = np.random.uniform(100, 5000)
        
        # Generate returns with slight trend and volatility
        trend = np.random.uniform(-0.0002, 0.0003)
        volatility = np.random.uniform(0.015, 0.03)
        
        returns = np.random.normal(trend, volatility, days)
        
        # Add some patterns
        # Momentum clusters
        for i in range(0, days, np.random.randint(20, 50)):
            cluster_size = np.random.randint(5, 15)
            direction = np.random.choice([-1, 1])
            returns[i:i+cluster_size] += direction * 0.005
        
        prices = start_price * np.cumprod(1 + returns)
        
        # Generate OHLC
        open_prices = prices * (1 + np.random.uniform(-0.01, 0.01, days))
        high_prices = np.maximum(prices, open_prices) * (1 + np.random.uniform(0, 0.02, days))
        low_prices = np.minimum(prices, open_prices) * (1 - np.random.uniform(0, 0.02, days))
        
        # Generate volume
        base_volume = np.random.uniform(100000, 10000000)
        volume = base_volume * (1 + np.random.uniform(-0.5, 1.5, days))
        volume = volume.astype(int)
        
        df = pd.DataFrame({
            'open': open_prices,
            'high': high_prices,
            'low': low_prices,
            'close': prices,
            'volume': volume
        }, index=dates)
        
        stock_data[symbol] = df
    
    print(f"Generated data for {len(symbols)} stocks")
    return stock_data


if __name__ == "__main__":
    # Test data fetcher
    fetcher = DataFetcher()
    print("Available universes:")
    print(f"  Nifty 50: {len(fetcher.nifty50)} stocks")
    print(f"  Nifty Next 50: {len(fetcher.niftynext50)} stocks")
    print(f"  Mid-caps: {len(fetcher.midcaps)} stocks")
