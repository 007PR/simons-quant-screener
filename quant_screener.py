"""
Renaissance-Inspired Quantitative Stock Screener for Indian Markets
====================================================================

Philosophy:
- Pure quantitative: No fundamentals, no news, no narratives
- Pattern discovery: Statistical anomalies across price, volume, volatility
- Aggregation over prediction: Multiple weak signals > one strong prediction
- Probability-based: Every signal has an expected edge, not certainty

This screener runs daily and outputs stocks with converging statistical patterns.
"""

import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta
import warnings
import json
import os

warnings.filterwarnings('ignore')


class QuantSignal:
    """Base class for all quantitative signals"""
    
    def __init__(self, name: str, weight: float = 1.0):
        self.name = name
        self.weight = weight
    
    def compute(self, df: pd.DataFrame) -> float:
        """Returns signal strength between -1 and 1"""
        raise NotImplementedError


class MomentumAnomaly(QuantSignal):
    """
    Detects short-term momentum anomalies.
    Renaissance found that momentum persists in short windows.
    """
    
    def __init__(self, window: int = 5):
        super().__init__(f"momentum_{window}d", weight=1.2)
        self.window = window
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < self.window + 20:
            return 0.0
        
        # Current momentum
        recent_return = (df['close'].iloc[-1] / df['close'].iloc[-self.window] - 1)
        
        # Historical distribution of same-window returns
        historical_returns = df['close'].pct_change(self.window).dropna()[:-1]
        
        if len(historical_returns) < 20:
            return 0.0
        
        # Z-score of current momentum vs history
        z_score = (recent_return - historical_returns.mean()) / (historical_returns.std() + 1e-8)
        
        # Normalize to [-1, 1]
        return np.clip(z_score / 3, -1, 1)


class VolumeSpike(QuantSignal):
    """
    Detects unusual volume patterns.
    Volume precedes price - unusual activity signals information.
    """
    
    def __init__(self, lookback: int = 20):
        super().__init__("volume_spike", weight=1.0)
        self.lookback = lookback
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < self.lookback + 5:
            return 0.0
        
        # Recent average volume (last 3 days)
        recent_vol = df['volume'].iloc[-3:].mean()
        
        # Historical average and std
        hist_vol = df['volume'].iloc[-(self.lookback + 3):-3]
        
        if hist_vol.std() == 0:
            return 0.0
        
        z_score = (recent_vol - hist_vol.mean()) / (hist_vol.std() + 1e-8)
        
        # Only positive signals (volume expansion)
        return np.clip(z_score / 3, 0, 1)


class VolatilityCompression(QuantSignal):
    """
    Detects volatility squeeze patterns.
    Low volatility often precedes explosive moves.
    """
    
    def __init__(self, short_window: int = 5, long_window: int = 20):
        super().__init__("vol_compression", weight=1.1)
        self.short_window = short_window
        self.long_window = long_window
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < self.long_window + 10:
            return 0.0
        
        returns = df['close'].pct_change().dropna()
        
        # Short-term vs long-term volatility ratio
        short_vol = returns.iloc[-self.short_window:].std()
        long_vol = returns.iloc[-self.long_window:].std()
        
        if long_vol == 0:
            return 0.0
        
        ratio = short_vol / long_vol
        
        # Compression = ratio < 1, expansion = ratio > 1
        # We want compression (signals potential breakout)
        if ratio < 0.6:
            return 1.0
        elif ratio < 0.8:
            return 0.5
        elif ratio > 1.5:
            return -0.5  # Already expanded, might mean revert
        else:
            return 0.0


class MeanReversionSetup(QuantSignal):
    """
    Statistical mean reversion detection.
    Not RSI - pure statistical deviation from rolling mean.
    """
    
    def __init__(self, window: int = 20):
        super().__init__("mean_reversion", weight=1.0)
        self.window = window
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < self.window + 10:
            return 0.0
        
        # Current price vs rolling mean
        rolling_mean = df['close'].rolling(self.window).mean()
        rolling_std = df['close'].rolling(self.window).std()
        
        current_price = df['close'].iloc[-1]
        current_mean = rolling_mean.iloc[-1]
        current_std = rolling_std.iloc[-1]
        
        if current_std == 0:
            return 0.0
        
        z_score = (current_price - current_mean) / current_std
        
        # Mean reversion: oversold is positive signal, overbought is negative
        # (expecting price to revert)
        if z_score < -2:
            return 0.8  # Strongly oversold - buy signal
        elif z_score < -1.5:
            return 0.5
        elif z_score > 2:
            return -0.8  # Strongly overbought - avoid/short
        elif z_score > 1.5:
            return -0.5
        else:
            return 0.0


class TrendConsistency(QuantSignal):
    """
    Measures trend quality/consistency.
    Smooth trends are more reliable than choppy ones.
    """
    
    def __init__(self, window: int = 20):
        super().__init__("trend_quality", weight=0.8)
        self.window = window
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < self.window + 5:
            return 0.0
        
        prices = df['close'].iloc[-self.window:].values
        x = np.arange(len(prices))
        
        # Linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, prices)
        
        # R-squared indicates trend consistency
        r_squared = r_value ** 2
        
        # Combine direction and quality
        direction = 1 if slope > 0 else -1
        
        # Only signal if trend is consistent (R² > 0.6)
        if r_squared > 0.8:
            return direction * 1.0
        elif r_squared > 0.6:
            return direction * 0.5
        else:
            return 0.0


class RelativeStrength(QuantSignal):
    """
    Relative performance vs market/sector.
    Outperformers tend to continue outperforming.
    """
    
    def __init__(self, window: int = 20):
        super().__init__("relative_strength", weight=1.0)
        self.window = window
        self.market_returns = None  # Will be set by screener
    
    def set_market_benchmark(self, market_df: pd.DataFrame):
        """Set market returns for comparison"""
        self.market_returns = market_df['close'].pct_change(self.window).iloc[-1]
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < self.window + 5 or self.market_returns is None:
            return 0.0
        
        stock_return = df['close'].iloc[-1] / df['close'].iloc[-self.window] - 1
        
        # Excess return over market
        excess_return = stock_return - self.market_returns
        
        # Normalize
        if excess_return > 0.1:
            return 1.0
        elif excess_return > 0.05:
            return 0.5
        elif excess_return < -0.1:
            return -1.0
        elif excess_return < -0.05:
            return -0.5
        else:
            return 0.0


class GapPattern(QuantSignal):
    """
    Detects gap patterns.
    Gaps often signal institutional activity.
    """
    
    def __init__(self):
        super().__init__("gap_pattern", weight=0.9)
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < 10:
            return 0.0
        
        # Check last 3 days for gaps
        gaps = []
        for i in range(-3, 0):
            prev_close = df['close'].iloc[i - 1]
            curr_open = df['open'].iloc[i]
            gap_pct = (curr_open - prev_close) / prev_close
            gaps.append(gap_pct)
        
        # Average recent gap
        avg_gap = np.mean(gaps)
        
        # Significant gap up
        if avg_gap > 0.02:
            return 0.8
        elif avg_gap > 0.01:
            return 0.4
        elif avg_gap < -0.02:
            return -0.8
        elif avg_gap < -0.01:
            return -0.4
        else:
            return 0.0


class DayOfWeekEffect(QuantSignal):
    """
    Time-based anomaly detection.
    Some patterns are day-specific.
    """
    
    def __init__(self):
        super().__init__("day_effect", weight=0.5)
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < 60:
            return 0.0
        
        df_copy = df.copy()
        df_copy['returns'] = df_copy['close'].pct_change()
        df_copy['day'] = df_copy.index.dayofweek
        
        # Today's day of week
        today = df_copy['day'].iloc[-1]
        
        # Historical returns by day of week
        day_returns = df_copy.groupby('day')['returns'].mean()
        
        if today not in day_returns.index:
            return 0.0
        
        today_avg = day_returns[today]
        overall_avg = df_copy['returns'].mean()
        
        # If today is historically a good day
        if today_avg > overall_avg + 0.001:
            return 0.5
        elif today_avg < overall_avg - 0.001:
            return -0.5
        else:
            return 0.0


class PriceAcceleration(QuantSignal):
    """
    Detects acceleration in price movement.
    Second derivative of price - momentum of momentum.
    """
    
    def __init__(self, window: int = 10):
        super().__init__("price_acceleration", weight=0.9)
        self.window = window
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < self.window * 3:
            return 0.0
        
        # First derivative: momentum
        momentum = df['close'].pct_change(self.window)
        
        # Second derivative: change in momentum
        acceleration = momentum.diff(self.window)
        
        current_accel = acceleration.iloc[-1]
        hist_accel_std = acceleration.iloc[:-1].std()
        
        if hist_accel_std == 0:
            return 0.0
        
        z_score = current_accel / (hist_accel_std + 1e-8)
        
        return np.clip(z_score / 2, -1, 1)


class VolumeProfileAnomaly(QuantSignal):
    """
    Detects unusual volume distribution patterns.
    Institutional accumulation/distribution.
    """
    
    def __init__(self, window: int = 20):
        super().__init__("volume_profile", weight=1.0)
        self.window = window
    
    def compute(self, df: pd.DataFrame) -> float:
        if len(df) < self.window + 5:
            return 0.0
        
        recent = df.iloc[-self.window:]
        
        # On-balance volume approach
        obv_changes = []
        for i in range(1, len(recent)):
            if recent['close'].iloc[i] > recent['close'].iloc[i-1]:
                obv_changes.append(recent['volume'].iloc[i])
            elif recent['close'].iloc[i] < recent['close'].iloc[i-1]:
                obv_changes.append(-recent['volume'].iloc[i])
            else:
                obv_changes.append(0)
        
        # Net volume flow
        net_flow = sum(obv_changes)
        avg_volume = recent['volume'].mean()
        
        if avg_volume == 0:
            return 0.0
        
        # Normalized flow
        normalized_flow = net_flow / (avg_volume * self.window)
        
        return np.clip(normalized_flow * 2, -1, 1)


class QuantScreener:
    """
    Main screener that aggregates all signals.
    Outputs ranked list of stocks with converging patterns.
    """
    
    def __init__(self):
        self.signals = [
            MomentumAnomaly(window=5),
            MomentumAnomaly(window=10),
            VolumeSpike(lookback=20),
            VolatilityCompression(short_window=5, long_window=20),
            MeanReversionSetup(window=20),
            TrendConsistency(window=20),
            RelativeStrength(window=20),
            GapPattern(),
            DayOfWeekEffect(),
            PriceAcceleration(window=10),
            VolumeProfileAnomaly(window=20),
        ]
        self.results = []
    
    def set_market_benchmark(self, market_df: pd.DataFrame):
        """Set market data for relative strength calculation"""
        for signal in self.signals:
            if isinstance(signal, RelativeStrength):
                signal.set_market_benchmark(market_df)
    
    def score_stock(self, symbol: str, df: pd.DataFrame) -> dict:
        """Score a single stock across all signals"""
        
        if len(df) < 60:
            return None
        
        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        scores = {}
        total_weighted_score = 0
        total_weight = 0
        active_signals = 0
        
        for signal in self.signals:
            try:
                score = signal.compute(df)
                scores[signal.name] = round(score, 3)
                total_weighted_score += score * signal.weight
                total_weight += signal.weight
                if abs(score) > 0.3:
                    active_signals += 1
            except Exception as e:
                scores[signal.name] = 0.0
        
        if total_weight == 0:
            return None
        
        composite_score = total_weighted_score / total_weight
        
        # Signal convergence bonus
        # Multiple signals agreeing = higher confidence
        if active_signals >= 5:
            convergence_bonus = 0.2
        elif active_signals >= 3:
            convergence_bonus = 0.1
        else:
            convergence_bonus = 0
        
        # Apply convergence bonus only if signals agree
        positive_signals = sum(1 for s in scores.values() if s > 0.3)
        negative_signals = sum(1 for s in scores.values() if s < -0.3)
        
        if positive_signals > negative_signals:
            composite_score += convergence_bonus
        elif negative_signals > positive_signals:
            composite_score -= convergence_bonus
        
        return {
            'symbol': symbol,
            'composite_score': round(composite_score, 4),
            'active_signals': active_signals,
            'signal_breakdown': scores,
            'last_price': round(df['close'].iloc[-1], 2),
            'change_1d': round((df['close'].iloc[-1] / df['close'].iloc[-2] - 1) * 100, 2),
            'change_5d': round((df['close'].iloc[-1] / df['close'].iloc[-5] - 1) * 100, 2) if len(df) >= 5 else 0,
            'avg_volume': int(df['volume'].iloc[-20:].mean()),
            'volatility_20d': round(df['close'].pct_change().iloc[-20:].std() * 100, 2),
            'scan_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def scan_universe(self, stock_data: dict, market_data: pd.DataFrame = None) -> list:
        """
        Scan entire universe of stocks.
        
        Args:
            stock_data: Dict of {symbol: DataFrame}
            market_data: Optional market index data for relative strength
        
        Returns:
            Sorted list of stock results
        """
        
        if market_data is not None:
            self.set_market_benchmark(market_data)
        
        results = []
        
        for symbol, df in stock_data.items():
            try:
                result = self.score_stock(symbol, df)
                if result is not None:
                    results.append(result)
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue
        
        # Sort by composite score (descending)
        results.sort(key=lambda x: x['composite_score'], reverse=True)
        
        self.results = results
        return results
    
    def get_top_stocks(self, n: int = 20, min_score: float = 0.2) -> list:
        """Get top N stocks with minimum score threshold"""
        return [r for r in self.results if r['composite_score'] >= min_score][:n]
    
    def get_bottom_stocks(self, n: int = 10, max_score: float = -0.2) -> list:
        """Get bottom N stocks (short candidates)"""
        bottom = [r for r in self.results if r['composite_score'] <= max_score]
        return sorted(bottom, key=lambda x: x['composite_score'])[:n]
    
    def generate_report(self) -> str:
        """Generate text report of scan results"""
        
        report = []
        report.append("=" * 70)
        report.append("QUANTITATIVE STOCK SCREENER - DAILY SCAN REPORT")
        report.append(f"Scan Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 70)
        report.append("")
        
        # Top picks
        top = self.get_top_stocks(15, min_score=0.15)
        report.append("TOP STOCKS (Long Candidates)")
        report.append("-" * 70)
        report.append(f"{'Symbol':<15} {'Score':>8} {'Signals':>8} {'1D%':>8} {'5D%':>8} {'Vol':>12}")
        report.append("-" * 70)
        
        for stock in top:
            report.append(
                f"{stock['symbol']:<15} "
                f"{stock['composite_score']:>8.3f} "
                f"{stock['active_signals']:>8} "
                f"{stock['change_1d']:>7.2f}% "
                f"{stock['change_5d']:>7.2f}% "
                f"{stock['avg_volume']:>12,}"
            )
        
        report.append("")
        
        # Short candidates
        bottom = self.get_bottom_stocks(10, max_score=-0.15)
        if bottom:
            report.append("BOTTOM STOCKS (Short/Avoid)")
            report.append("-" * 70)
            report.append(f"{'Symbol':<15} {'Score':>8} {'Signals':>8} {'1D%':>8} {'5D%':>8} {'Vol':>12}")
            report.append("-" * 70)
            
            for stock in bottom:
                report.append(
                    f"{stock['symbol']:<15} "
                    f"{stock['composite_score']:>8.3f} "
                    f"{stock['active_signals']:>8} "
                    f"{stock['change_1d']:>7.2f}% "
                    f"{stock['change_5d']:>7.2f}% "
                    f"{stock['avg_volume']:>12,}"
                )
        
        report.append("")
        report.append("=" * 70)
        report.append("SIGNAL LEGEND:")
        report.append("  Score: Composite weighted score (-1 to +1)")
        report.append("  Signals: Number of active signals (>0.3 strength)")
        report.append("  Higher score = stronger buy signal")
        report.append("  Lower score = stronger sell/avoid signal")
        report.append("=" * 70)
        
        return "\n".join(report)
    
    def save_results(self, filepath: str):
        """Save results to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.results, f, indent=2)
    
    def load_results(self, filepath: str):
        """Load results from JSON file"""
        with open(filepath, 'r') as f:
            self.results = json.load(f)


# Utility function for detailed stock analysis
def analyze_stock_detail(screener: QuantScreener, symbol: str, df: pd.DataFrame) -> str:
    """Generate detailed analysis for a single stock"""
    
    result = screener.score_stock(symbol, df)
    if result is None:
        return f"Insufficient data for {symbol}"
    
    analysis = []
    analysis.append(f"\n{'='*50}")
    analysis.append(f"DETAILED ANALYSIS: {symbol}")
    analysis.append(f"{'='*50}")
    analysis.append(f"Composite Score: {result['composite_score']:.4f}")
    analysis.append(f"Active Signals: {result['active_signals']}")
    analysis.append(f"Last Price: {result['last_price']}")
    analysis.append(f"1-Day Change: {result['change_1d']:.2f}%")
    analysis.append(f"5-Day Change: {result['change_5d']:.2f}%")
    analysis.append(f"20-Day Volatility: {result['volatility_20d']:.2f}%")
    analysis.append("")
    analysis.append("Signal Breakdown:")
    analysis.append("-" * 40)
    
    for signal_name, score in result['signal_breakdown'].items():
        strength = "STRONG" if abs(score) > 0.5 else "MODERATE" if abs(score) > 0.3 else "WEAK"
        direction = "BULLISH" if score > 0 else "BEARISH" if score < 0 else "NEUTRAL"
        analysis.append(f"  {signal_name:<25} {score:>7.3f}  [{direction} - {strength}]")
    
    return "\n".join(analysis)


if __name__ == "__main__":
    print("Quantitative Screener Module Loaded")
    print("Use with data_fetcher.py to run scans")
