"""
Portfolio Tracker
==================

Tracks a ₹10 Lakh investment equally distributed across screened stocks.
Calculates daily returns, absolute P&L, and percentage returns.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os


class PortfolioTracker:
    """
    Tracks portfolio performance with equal-weight allocation.
    """
    
    def __init__(self, initial_capital: float = 1000000, portfolio_file: str = "./portfolio/holdings.json"):
        self.initial_capital = initial_capital  # ₹10 Lakh default
        self.portfolio_file = portfolio_file
        self.holdings = {}
        self.history = []
        
        # Ensure portfolio directory exists
        os.makedirs(os.path.dirname(portfolio_file), exist_ok=True)
        
        # Load existing portfolio if exists
        self.load_portfolio()
    
    def load_portfolio(self):
        """Load existing portfolio from file"""
        if os.path.exists(self.portfolio_file):
            try:
                with open(self.portfolio_file, 'r') as f:
                    data = json.load(f)
                    self.holdings = data.get('holdings', {})
                    self.history = data.get('history', [])
                    self.initial_capital = data.get('initial_capital', self.initial_capital)
                print(f"Loaded portfolio: {len(self.holdings)} holdings")
            except Exception as e:
                print(f"Error loading portfolio: {e}")
    
    def save_portfolio(self):
        """Save portfolio to file"""
        data = {
            'initial_capital': self.initial_capital,
            'holdings': self.holdings,
            'history': self.history,
            'last_updated': datetime.now().isoformat()
        }
        with open(self.portfolio_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def create_portfolio(self, stocks: list, stock_prices: dict):
        """
        Create new portfolio with equal-weight allocation.
        
        Args:
            stocks: List of stock symbols to buy
            stock_prices: Dict of {symbol: current_price}
        """
        if not stocks:
            print("No stocks to invest in!")
            return
        
        # Equal allocation
        allocation_per_stock = self.initial_capital / len(stocks)
        
        self.holdings = {}
        total_invested = 0
        
        print(f"\n{'='*60}")
        print(f"CREATING PORTFOLIO - ₹{self.initial_capital:,.0f} Investment")
        print(f"{'='*60}")
        print(f"Allocating ₹{allocation_per_stock:,.2f} per stock across {len(stocks)} stocks\n")
        
        for symbol in stocks:
            if symbol in stock_prices and stock_prices[symbol] > 0:
                price = stock_prices[symbol]
                quantity = int(allocation_per_stock / price)  # Full shares only
                
                if quantity > 0:
                    invested = quantity * price
                    self.holdings[symbol] = {
                        'quantity': quantity,
                        'buy_price': price,
                        'buy_date': datetime.now().strftime('%Y-%m-%d'),
                        'invested_amount': invested
                    }
                    total_invested += invested
                    print(f"  {symbol:<15} | Qty: {quantity:>6} | Price: ₹{price:>10,.2f} | Invested: ₹{invested:>12,.2f}")
        
        # Track initial state
        self.history.append({
            'date': datetime.now().strftime('%Y-%m-%d'),
            'portfolio_value': total_invested,
            'absolute_return': 0,
            'percentage_return': 0,
            'holdings_count': len(self.holdings)
        })
        
        print(f"\n{'='*60}")
        print(f"Total Invested: ₹{total_invested:,.2f}")
        print(f"Cash Remaining: ₹{self.initial_capital - total_invested:,.2f}")
        print(f"Stocks Bought: {len(self.holdings)}")
        print(f"{'='*60}\n")
        
        self.save_portfolio()
    
    def update_portfolio(self, current_prices: dict):
        """
        Update portfolio with current prices and calculate returns.
        
        Args:
            current_prices: Dict of {symbol: current_price}
        
        Returns:
            Dict with portfolio summary
        """
        if not self.holdings:
            print("No holdings to update!")
            return None
        
        total_current_value = 0
        total_invested = 0
        stock_returns = []
        
        for symbol, holding in self.holdings.items():
            if symbol in current_prices:
                current_price = current_prices[symbol]
                current_value = holding['quantity'] * current_price
                invested = holding['invested_amount']
                
                stock_return = (current_price - holding['buy_price']) / holding['buy_price'] * 100
                stock_pnl = current_value - invested
                
                total_current_value += current_value
                total_invested += invested
                
                stock_returns.append({
                    'symbol': symbol,
                    'quantity': holding['quantity'],
                    'buy_price': holding['buy_price'],
                    'current_price': current_price,
                    'invested': invested,
                    'current_value': current_value,
                    'pnl': stock_pnl,
                    'return_pct': stock_return
                })
        
        # Calculate portfolio totals
        absolute_return = total_current_value - total_invested
        percentage_return = (absolute_return / total_invested * 100) if total_invested > 0 else 0
        
        # Add to history
        self.history.append({
            'date': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'portfolio_value': total_current_value,
            'absolute_return': absolute_return,
            'percentage_return': percentage_return,
            'holdings_count': len(self.holdings)
        })
        
        self.save_portfolio()
        
        summary = {
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'initial_investment': self.initial_capital,
            'total_invested': total_invested,
            'current_value': total_current_value,
            'absolute_return': absolute_return,
            'percentage_return': percentage_return,
            'holdings': stock_returns,
            'winners': len([s for s in stock_returns if s['pnl'] > 0]),
            'losers': len([s for s in stock_returns if s['pnl'] < 0]),
            'best_performer': max(stock_returns, key=lambda x: x['return_pct']) if stock_returns else None,
            'worst_performer': min(stock_returns, key=lambda x: x['return_pct']) if stock_returns else None
        }
        
        return summary
    
    def get_daily_report(self, current_prices: dict) -> str:
        """Generate daily portfolio report"""
        
        summary = self.update_portfolio(current_prices)
        if not summary:
            return "No portfolio data available"
        
        report = []
        report.append("\n" + "=" * 70)
        report.append("📊 DAILY PORTFOLIO REPORT")
        report.append(f"Date: {summary['date']}")
        report.append("=" * 70)
        
        # Overall Performance
        pnl_symbol = "🟢" if summary['absolute_return'] >= 0 else "🔴"
        report.append(f"\n{pnl_symbol} OVERALL PERFORMANCE")
        report.append("-" * 70)
        report.append(f"Initial Investment:  ₹{summary['initial_investment']:>15,.2f}")
        report.append(f"Total Invested:      ₹{summary['total_invested']:>15,.2f}")
        report.append(f"Current Value:       ₹{summary['current_value']:>15,.2f}")
        report.append(f"Absolute Return:     ₹{summary['absolute_return']:>15,.2f}")
        report.append(f"Percentage Return:   {summary['percentage_return']:>15.2f}%")
        report.append(f"Win/Loss Ratio:      {summary['winners']}W / {summary['losers']}L")
        
        # Best and Worst
        if summary['best_performer']:
            best = summary['best_performer']
            report.append(f"\n🏆 Best:  {best['symbol']} (+{best['return_pct']:.2f}%)")
        if summary['worst_performer']:
            worst = summary['worst_performer']
            report.append(f"📉 Worst: {worst['symbol']} ({worst['return_pct']:.2f}%)")
        
        # Holdings Detail
        report.append(f"\n📈 HOLDINGS DETAIL")
        report.append("-" * 70)
        report.append(f"{'Symbol':<12} {'Qty':>6} {'Buy':>10} {'Current':>10} {'P&L':>12} {'Return':>8}")
        report.append("-" * 70)
        
        # Sort by return
        sorted_holdings = sorted(summary['holdings'], key=lambda x: x['return_pct'], reverse=True)
        
        for h in sorted_holdings:
            pnl_str = f"₹{h['pnl']:+,.0f}"
            return_str = f"{h['return_pct']:+.2f}%"
            report.append(
                f"{h['symbol']:<12} {h['quantity']:>6} "
                f"₹{h['buy_price']:>9,.2f} ₹{h['current_price']:>9,.2f} "
                f"{pnl_str:>12} {return_str:>8}"
            )
        
        report.append("-" * 70)
        report.append(f"{'TOTAL':<12} {'':<6} {'':<10} {'':<10} "
                     f"₹{summary['absolute_return']:>+11,.0f} {summary['percentage_return']:>+7.2f}%")
        report.append("=" * 70)
        
        return "\n".join(report)
    
    def get_history_summary(self) -> str:
        """Get historical performance summary"""
        
        if len(self.history) < 2:
            return "Not enough history data"
        
        report = []
        report.append("\n" + "=" * 70)
        report.append("📈 HISTORICAL PERFORMANCE")
        report.append("=" * 70)
        report.append(f"{'Date':<20} {'Value':>15} {'Absolute':>15} {'Return %':>10}")
        report.append("-" * 70)
        
        for entry in self.history[-30:]:  # Last 30 entries
            report.append(
                f"{entry['date']:<20} "
                f"₹{entry['portfolio_value']:>14,.0f} "
                f"₹{entry['absolute_return']:>+14,.0f} "
                f"{entry['percentage_return']:>+9.2f}%"
            )
        
        report.append("=" * 70)
        
        return "\n".join(report)
    
    def reset_portfolio(self):
        """Reset portfolio to start fresh"""
        self.holdings = {}
        self.history = []
        self.save_portfolio()
        print("Portfolio reset successfully")


def get_portfolio_html_section(summary: dict) -> str:
    """Generate HTML section for portfolio display"""
    
    if not summary:
        return "<div class='section'><h2>No Portfolio Data</h2></div>"
    
    pnl_class = "positive" if summary['absolute_return'] >= 0 else "negative"
    pnl_icon = "📈" if summary['absolute_return'] >= 0 else "📉"
    
    html = f"""
    <div class="section portfolio-section">
        <h2>💰 Portfolio Performance (₹10 Lakh Investment)</h2>
        
        <div class="portfolio-summary">
            <div class="summary-card">
                <span class="label">Initial Investment</span>
                <span class="value">₹{summary['initial_investment']:,.0f}</span>
            </div>
            <div class="summary-card">
                <span class="label">Current Value</span>
                <span class="value">₹{summary['current_value']:,.2f}</span>
            </div>
            <div class="summary-card {pnl_class}">
                <span class="label">Absolute Return</span>
                <span class="value">₹{summary['absolute_return']:+,.2f}</span>
            </div>
            <div class="summary-card {pnl_class}">
                <span class="label">Percentage Return</span>
                <span class="value">{summary['percentage_return']:+.2f}%</span>
            </div>
            <div class="summary-card">
                <span class="label">Win/Loss</span>
                <span class="value">{summary['winners']}W / {summary['losers']}L</span>
            </div>
        </div>
        
        <h3>Holdings Detail</h3>
        <table class="holdings-table">
            <tr>
                <th>Symbol</th>
                <th>Quantity</th>
                <th>Buy Price</th>
                <th>Current Price</th>
                <th>P&L</th>
                <th>Return %</th>
            </tr>
    """
    
    sorted_holdings = sorted(summary['holdings'], key=lambda x: x['return_pct'], reverse=True)
    
    for h in sorted_holdings:
        row_class = "winner" if h['pnl'] >= 0 else "loser"
        html += f"""
            <tr class="{row_class}">
                <td><strong>{h['symbol']}</strong></td>
                <td>{h['quantity']}</td>
                <td>₹{h['buy_price']:,.2f}</td>
                <td>₹{h['current_price']:,.2f}</td>
                <td class="{'positive' if h['pnl'] >= 0 else 'negative'}">₹{h['pnl']:+,.0f}</td>
                <td class="{'positive' if h['return_pct'] >= 0 else 'negative'}">{h['return_pct']:+.2f}%</td>
            </tr>
        """
    
    html += f"""
            <tr class="total-row">
                <td colspan="4"><strong>TOTAL</strong></td>
                <td class="{pnl_class}"><strong>₹{summary['absolute_return']:+,.0f}</strong></td>
                <td class="{pnl_class}"><strong>{summary['percentage_return']:+.2f}%</strong></td>
            </tr>
        </table>
    </div>
    """
    
    return html


if __name__ == "__main__":
    # Test portfolio tracker
    tracker = PortfolioTracker(initial_capital=1000000)
    
    # Sample stocks and prices
    test_stocks = ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK']
    test_prices = {
        'RELIANCE': 2450.50,
        'TCS': 3890.25,
        'INFY': 1567.80,
        'HDFCBANK': 1678.90,
        'ICICIBANK': 1023.45
    }
    
    # Create portfolio
    tracker.create_portfolio(test_stocks, test_prices)
    
    # Simulate price change
    updated_prices = {
        'RELIANCE': 2480.00,  # +1.2%
        'TCS': 3850.00,       # -1.0%
        'INFY': 1590.00,      # +1.4%
        'HDFCBANK': 1700.00,  # +1.3%
        'ICICIBANK': 1010.00  # -1.3%
    }
    
    # Get report
    print(tracker.get_daily_report(updated_prices))
