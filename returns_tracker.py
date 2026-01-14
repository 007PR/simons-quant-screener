#!/usr/bin/env python3
"""
Returns Tracker Module
======================

Tracks actual daily returns from the screener picks and compounds them over time.
Stores history in JSON for accurate performance measurement.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd


class ReturnsTracker:
    """
    Tracks daily returns from screener picks and market benchmark.
    Compounds returns over time for accurate performance measurement.
    """
    
    def __init__(self, history_file: str = "./output/returns_history.json", 
                 initial_capital: float = 1000000):
        self.history_file = history_file
        self.initial_capital = initial_capital
        self.history = self._load_history()
    
    def _load_history(self) -> dict:
        """Load existing returns history from file"""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        
        # Initialize new history
        return {
            "initial_capital": self.initial_capital,
            "start_date": datetime.now().strftime("%Y-%m-%d"),
            "daily_returns": [],  # List of {date, strategy_return_pct, market_return_pct, picks}
            "strategy_equity": [self.initial_capital],  # Compounded equity curve
            "market_equity": [self.initial_capital],    # Market benchmark equity
            "total_strategy_return_pct": 0,
            "total_market_return_pct": 0
        }
    
    def _save_history(self):
        """Save history to file"""
        os.makedirs(os.path.dirname(self.history_file), exist_ok=True)
        with open(self.history_file, 'w') as f:
            json.dump(self.history, f, indent=2)
    
    def record_daily_return(self, date: str, strategy_return_pct: float, 
                           market_return_pct: float, picks: List[dict] = None):
        """
        Record a single day's return.
        
        Args:
            date: Date string (YYYY-MM-DD)
            strategy_return_pct: Portfolio return percentage (e.g., 0.5 for +0.5%)
            market_return_pct: Market (Nifty) return percentage
            picks: List of stock picks with their individual returns
        """
        # Check if date already exists
        existing_dates = [r['date'] for r in self.history['daily_returns']]
        if date in existing_dates:
            print(f"Return for {date} already recorded. Skipping.")
            return
        
        # Record the daily return
        daily_record = {
            "date": date,
            "strategy_return_pct": round(strategy_return_pct, 4),
            "market_return_pct": round(market_return_pct, 4),
            "picks_count": len(picks) if picks else 0,
            "winning_picks": sum(1 for p in picks if p.get('return_pct', 0) > 0) if picks else 0,
            "top_performer": max(picks, key=lambda x: x.get('return_pct', 0))['symbol'] if picks else None,
            "worst_performer": min(picks, key=lambda x: x.get('return_pct', 0))['symbol'] if picks else None
        }
        
        self.history['daily_returns'].append(daily_record)
        
        # Compound the equity
        last_strategy_equity = self.history['strategy_equity'][-1]
        last_market_equity = self.history['market_equity'][-1]
        
        new_strategy_equity = last_strategy_equity * (1 + strategy_return_pct / 100)
        new_market_equity = last_market_equity * (1 + market_return_pct / 100)
        
        self.history['strategy_equity'].append(round(new_strategy_equity, 2))
        self.history['market_equity'].append(round(new_market_equity, 2))
        
        # Update total returns
        self.history['total_strategy_return_pct'] = round(
            (new_strategy_equity / self.initial_capital - 1) * 100, 2
        )
        self.history['total_market_return_pct'] = round(
            (new_market_equity / self.initial_capital - 1) * 100, 2
        )
        
        self._save_history()
        
        print(f"✓ Recorded {date}: Strategy {strategy_return_pct:+.2f}% | Market {market_return_pct:+.2f}%")
    
    def calculate_picks_return(self, picks: List[dict], current_prices: dict, 
                               investment_per_stock: float = None) -> tuple:
        """
        Calculate total return from a list of picks.
        
        Args:
            picks: List of stock picks with 'symbol' and 'last_price' (buy price)
            current_prices: Dict of {symbol: current_price}
            investment_per_stock: Amount invested per stock
        
        Returns:
            (total_return_pct, picks_with_returns)
        """
        if not picks:
            return 0, []
        
        if investment_per_stock is None:
            investment_per_stock = self.initial_capital / len(picks)
        
        total_invested = 0
        total_current = 0
        picks_with_returns = []
        
        for pick in picks:
            symbol = pick['symbol']
            buy_price = pick.get('last_price', 0)
            
            if buy_price <= 0 or symbol not in current_prices:
                continue
            
            current_price = current_prices[symbol]
            qty = int(investment_per_stock / buy_price)
            invested = qty * buy_price
            current_value = qty * current_price
            
            return_pct = ((current_price / buy_price) - 1) * 100
            pnl = current_value - invested
            
            total_invested += invested
            total_current += current_value
            
            picks_with_returns.append({
                'symbol': symbol,
                'buy_price': buy_price,
                'current_price': current_price,
                'qty': qty,
                'invested': invested,
                'current_value': current_value,
                'pnl': pnl,
                'return_pct': round(return_pct, 2)
            })
        
        total_return_pct = ((total_current / total_invested) - 1) * 100 if total_invested > 0 else 0
        
        return round(total_return_pct, 4), picks_with_returns
    
    def get_equity_curve(self, period_months: int = None) -> dict:
        """
        Get equity curve data for charting.
        
        Args:
            period_months: Number of months to include (None = all)
        
        Returns:
            Dict with labels, strategy_equity, market_equity
        """
        strategy_equity = self.history['strategy_equity']
        market_equity = self.history['market_equity']
        
        # If we have daily returns, use dates as labels
        if self.history['daily_returns']:
            labels = ['Start'] + [r['date'] for r in self.history['daily_returns']]
        else:
            labels = [f'Day {i}' for i in range(len(strategy_equity))]
        
        # Slice if period specified
        if period_months:
            # Approximate: 22 trading days per month
            max_days = period_months * 22
            strategy_equity = strategy_equity[:max_days + 1]
            market_equity = market_equity[:max_days + 1]
            labels = labels[:max_days + 1]
        
        return {
            'labels': labels,
            'strategy': strategy_equity,
            'market': market_equity
        }
    
    def get_performance_summary(self) -> dict:
        """Get overall performance summary"""
        days_tracked = len(self.history['daily_returns'])
        
        if days_tracked == 0:
            return {
                'days_tracked': 0,
                'strategy_return_pct': 0,
                'market_return_pct': 0,
                'alpha_pct': 0,
                'current_equity': self.initial_capital,
                'market_equity': self.initial_capital,
                'win_rate': 0,
                'avg_daily_return': 0
            }
        
        strategy_returns = [r['strategy_return_pct'] for r in self.history['daily_returns']]
        winning_days = sum(1 for r in strategy_returns if r > 0)
        
        return {
            'days_tracked': days_tracked,
            'strategy_return_pct': self.history['total_strategy_return_pct'],
            'market_return_pct': self.history['total_market_return_pct'],
            'alpha_pct': round(self.history['total_strategy_return_pct'] - self.history['total_market_return_pct'], 2),
            'current_equity': self.history['strategy_equity'][-1],
            'market_equity': self.history['market_equity'][-1],
            'win_rate': round(winning_days / days_tracked * 100, 1),
            'avg_daily_return': round(sum(strategy_returns) / days_tracked, 3),
            'best_day': max(strategy_returns),
            'worst_day': min(strategy_returns),
            'max_equity': max(self.history['strategy_equity']),
            'min_equity': min(self.history['strategy_equity'])
        }
    
    def get_period_returns(self) -> dict:
        """Calculate returns for different periods (1D, 1W, 1M, 1Y, etc.)"""
        equity = self.history['strategy_equity']
        market = self.history['market_equity']
        n = len(equity)
        
        def calc_return(data, days_ago):
            if n <= days_ago:
                return 0
            return round((data[-1] / data[-(days_ago + 1)] - 1) * 100, 2)
        
        return {
            '1D': calc_return(equity, 1) if n > 1 else 0,
            '1W': calc_return(equity, 5) if n > 5 else 0,
            '1M': calc_return(equity, 22) if n > 22 else 0,
            '3M': calc_return(equity, 66) if n > 66 else 0,
            '6M': calc_return(equity, 132) if n > 132 else 0,
            '1Y': calc_return(equity, 252) if n > 252 else 0,
            '3Y': calc_return(equity, 756) if n > 756 else 0,
            '5Y': calc_return(equity, 1260) if n > 1260 else 0,
        }
    
    def reset_history(self):
        """Reset all history (use with caution!)"""
        self.history = {
            "initial_capital": self.initial_capital,
            "start_date": datetime.now().strftime("%Y-%m-%d"),
            "daily_returns": [],
            "strategy_equity": [self.initial_capital],
            "market_equity": [self.initial_capital],
            "total_strategy_return_pct": 0,
            "total_market_return_pct": 0
        }
        self._save_history()
        print("✓ History reset")


def generate_sample_history(tracker: ReturnsTracker, days: int = 60):
    """Generate sample history for demo/testing"""
    import random
    
    print(f"Generating {days} days of sample return history...")
    
    base_date = datetime.now() - timedelta(days=days)
    
    for i in range(days):
        date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
        
        # Skip weekends
        day_of_week = (base_date + timedelta(days=i)).weekday()
        if day_of_week >= 5:
            continue
        
        # Generate realistic daily returns
        # Strategy has slight edge over market
        market_return = random.gauss(0.05, 0.8)  # Mean 0.05%, std 0.8%
        strategy_edge = random.gauss(0.08, 0.3)   # Additional edge
        strategy_return = market_return + strategy_edge
        
        # Simulate picks
        picks = [{'symbol': f'STOCK{j}', 'return_pct': random.gauss(strategy_return, 1.5)} 
                 for j in range(25)]
        
        tracker.record_daily_return(date, strategy_return, market_return, picks)
    
    print(f"✓ Generated {len(tracker.history['daily_returns'])} trading days of history")


if __name__ == "__main__":
    # Test the tracker
    tracker = ReturnsTracker()
    
    # Generate sample data for testing
    generate_sample_history(tracker, days=90)
    
    # Print summary
    summary = tracker.get_performance_summary()
    print("\n" + "=" * 50)
    print("PERFORMANCE SUMMARY")
    print("=" * 50)
    print(f"Days Tracked: {summary['days_tracked']}")
    print(f"Strategy Return: {summary['strategy_return_pct']:+.2f}%")
    print(f"Market Return: {summary['market_return_pct']:+.2f}%")
    print(f"Alpha: {summary['alpha_pct']:+.2f}%")
    print(f"Win Rate: {summary['win_rate']:.1f}%")
    print(f"Current Equity: ₹{summary['current_equity']:,.0f}")
