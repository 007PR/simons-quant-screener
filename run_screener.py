#!/usr/bin/env python3
"""
Jim Simons Quantitative Stock Screener
======================================

Runs daily via GitHub Actions to:
1. Fetch real stock data from Yahoo Finance
2. Run 11 quantitative signals
3. Generate beautiful HTML report
4. Publish to GitHub Pages
"""

import os
import json
from datetime import datetime, timedelta
from calendar import monthrange

from quant_screener import QuantScreener
from data_fetcher import DataFetcher, generate_sample_data
from returns_tracker import ReturnsTracker
from trading_calendar import is_trading_day, get_previous_trading_day


def run_screener():
    """Run the full screening process"""
    
    print("=" * 70)
    print("JIM SIMONS QUANTITATIVE SCREENER")
    print(f"Run Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 70)
    
    # Adjust for IST
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    ist_date = ist_now.date()
    scan_date = ist_date.strftime('%Y%m%d')
    
    print(f"IST Date: {ist_date}")
    print(f"Is Trading Day: {is_trading_day(ist_date)}")
    
    # Initialize
    fetcher = DataFetcher(cache_dir="./data_cache")
    screener = QuantScreener()
    output_dir = "./output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Fetch real data
    print("\nFetching data from Yahoo Finance...")
    
    try:
        stock_data = fetcher.fetch_universe(
            universe='nifty200',
            source='yfinance',
            days=365,
            delay=0.2
        )
        
        if not stock_data or len(stock_data) < 10:
            print("WARNING: Limited data fetched, using available stocks")
        
        print(f"Fetched data for {len(stock_data)} stocks")
        
        # Fetch market benchmark
        print("Fetching Nifty 50 index...")
        market_data = fetcher.fetch_index("NIFTY50", source='yfinance')
        
        if len(market_data) > 0:
            screener.set_market_benchmark(market_data)
            print(f"Nifty 50 loaded: {len(market_data)} days")
        
        # Run screener
        print(f"\nScanning {len(stock_data)} stocks...")
        results = screener.scan_universe(stock_data, market_data)
        
        # Sort by composite score
        results.sort(key=lambda x: x.get('composite_score', 0), reverse=True)
        
        # Get top 25
        top_picks = [r for r in results if r.get('composite_score', 0) >= 0.05][:25]
        
        print(f"Found {len(top_picks)} stocks passing filters")
        
        # Print top 10
        print("\nTOP 10 PICKS:")
        print("-" * 50)
        for i, pick in enumerate(top_picks[:10], 1):
            print(f"{i:2}. {pick['symbol']:15} Score: {pick['composite_score']:.3f}")
        
        # Save JSON
        json_path = os.path.join(output_dir, f"scan_{scan_date}.json")
        with open(json_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nSaved: {json_path}")
        
        # Load/update returns tracker
        returns_tracker = ReturnsTracker(
            history_file=os.path.join(output_dir, "returns_history.json")
        )
        
        # Calculate returns from previous picks
        calculate_returns(output_dir, stock_data, market_data, returns_tracker, ist_date)
        
        # Get performance data
        performance = returns_tracker.get_performance_summary()
        period_returns = returns_tracker.get_period_returns()
        equity_curve = returns_tracker.get_equity_curve()
        
        # Generate HTML
        html = generate_html_report(
            top_picks=top_picks,
            results=results,
            scan_time=ist_now,
            performance=performance,
            period_returns=period_returns,
            equity_curve=equity_curve,
            total_screened=len(stock_data)
        )
        
        # Save as index.html for GitHub Pages
        html_path = os.path.join(output_dir, "index.html")
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"Saved: {html_path}")
        
        print("\n" + "=" * 70)
        print("SCREENER COMPLETED SUCCESSFULLY")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def calculate_returns(output_dir, stock_data, market_data, returns_tracker, ist_date):
    """Calculate returns from previous day's picks"""
    
    scan_files = sorted([f for f in os.listdir(output_dir) 
                        if f.startswith('scan_') and f.endswith('.json')], 
                       reverse=True)
    
    if len(scan_files) < 2:
        return
    
    try:
        prev_file = os.path.join(output_dir, scan_files[1])
        with open(prev_file, 'r') as f:
            prev_picks = json.load(f)
        
        prev_picks = [p for p in prev_picks if p.get('composite_score', 0) >= 0.05][:25]
        
        total_return = 0
        count = 0
        
        for pick in prev_picks:
            symbol = pick['symbol']
            if symbol in stock_data:
                df = stock_data[symbol]
                if len(df) > 1:
                    ret = (df['close'].iloc[-1] / df['close'].iloc[-2] - 1) * 100
                    total_return += ret
                    count += 1
        
        if count > 0:
            strategy_return = total_return / count
            
            market_return = 0
            if len(market_data) > 1:
                market_return = (market_data['close'].iloc[-1] / market_data['close'].iloc[-2] - 1) * 100
            
            returns_tracker.record_daily_return(
                date=ist_date.strftime("%Y-%m-%d"),
                strategy_return_pct=round(strategy_return, 2),
                market_return_pct=round(market_return, 2)
            )
            
            print(f"\nRecorded: Strategy {strategy_return:+.2f}% | Market {market_return:+.2f}%")
    
    except Exception as e:
        print(f"Warning: Could not calculate returns: {e}")


def generate_html_report(top_picks, results, scan_time, performance, period_returns, equity_curve, total_screened):
    """Generate beautiful HTML report"""
    
    today_date = scan_time.strftime('%d %b %Y')
    
    # Portfolio calculations
    initial_capital = 1000000
    if equity_curve and equity_curve.get('strategy'):
        current_value = equity_curve['strategy'][-1]
    else:
        current_value = initial_capital
    
    absolute_return = current_value - initial_capital
    
    # Period returns
    if not period_returns:
        period_returns = {'1D': 0, '1W': 0, '1M': 0, '1Y': 0, '3Y': 0, '5Y': 0}
    
    portfolio_returns = {
        '1D': period_returns.get('1D', 0),
        '1W': period_returns.get('1W', 0),
        '1M': period_returns.get('1M', 0),
        '1Y': period_returns.get('1Y', 0),
        '3Y': period_returns.get('3Y', 0),
        '5Y': period_returns.get('5Y', 0),
    }
    
    # Performance metrics
    if not performance:
        performance = {}
    
    strategy_return = performance.get('strategy_return_pct', 0)
    market_return = performance.get('market_return_pct', 0)
    alpha = strategy_return - market_return
    win_rate = performance.get('win_rate', 50)
    days_tracked = performance.get('days_tracked', 0)
    
    # Equity curve data
    if equity_curve and equity_curve.get('strategy'):
        strategy_data = equity_curve['strategy']
        market_data_eq = equity_curve.get('market', [initial_capital] * len(strategy_data))
        chart_labels = [f"Day {i}" for i in range(len(strategy_data))]
    else:
        strategy_data = [initial_capital]
        market_data_eq = [initial_capital]
        chart_labels = ["Start"]
    
    # Calendar heatmap data
    calendar_data = []
    if equity_curve and equity_curve.get('daily_returns'):
        for day in equity_curve['daily_returns'][-90:]:
            calendar_data.append({
                'date': day.get('date', ''),
                'strategy': day.get('strategy_return_pct', 0),
                'market': day.get('market_return_pct', 0)
            })
    
    # Generate stock rows
    stock_rows = ""
    for i, stock in enumerate(top_picks[:25], 1):
        change_1d = stock.get('change_1d', 0) or 0
        change_5d = stock.get('change_5d', 0) or 0
        price = stock.get('last_price', 0) or 0
        
        stock_rows += f"""
        <tr data-score="{stock.get('composite_score', 0)}" data-1d="{change_1d}" data-1w="{change_5d}">
            <td class="row-num">{i}</td>
            <td class="stock-symbol">{stock['symbol']}</td>
            <td><span class="score-badge">{stock.get('composite_score', 0):.3f}</span></td>
            <td><span class="signal-badge">{stock.get('active_signals', 0)}</span></td>
            <td>&#8377;{price:,.0f}</td>
            <td class="{'positive' if change_1d >= 0 else 'negative'}">{change_1d:+.1f}%</td>
            <td class="{'positive' if change_5d >= 0 else 'negative'}">{change_5d:+.1f}%</td>
        </tr>"""
    
    # Generate calendar HTML
    calendar_html = ""
    for day in calendar_data[-30:]:
        ret = day['strategy']
        if ret > 1:
            color_class = 'profit-high'
        elif ret > 0:
            color_class = 'profit'
        elif ret < -1:
            color_class = 'loss-high'
        elif ret < 0:
            color_class = 'loss'
        else:
            color_class = 'empty'
        
        calendar_html += f'<div class="calendar-day {color_class}" title="{day["date"]}: {ret:+.2f}%"></div>'
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Jim Simons Quant Screener - {today_date}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a0f; color: #e0e0e0; }}
        
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        
        /* Header */
        .header {{
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            padding: 30px;
            border-radius: 15px;
            margin-bottom: 20px;
            text-align: center;
        }}
        .header h1 {{ color: #4fc3f7; font-size: 2em; margin-bottom: 10px; }}
        .header .subtitle {{ color: #888; font-size: 1em; }}
        .header .date {{ color: #ffd700; font-size: 0.9em; margin-top: 10px; }}
        
        /* Stats Grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            border: 1px solid #2a2a4e;
            border-radius: 12px;
            padding: 20px;
            text-align: center;
        }}
        .stat-card .label {{ color: #888; font-size: 0.75em; text-transform: uppercase; letter-spacing: 1px; }}
        .stat-card .value {{ font-size: 1.6em; font-weight: bold; margin-top: 5px; }}
        .stat-card .value.positive {{ color: #4caf50; }}
        .stat-card .value.negative {{ color: #f44336; }}
        .stat-card .value.neutral {{ color: #4fc3f7; }}
        .stat-card .sub {{ color: #666; font-size: 0.7em; margin-top: 5px; }}
        
        /* Chart Section */
        .chart-section {{
            background: #1a1a2e;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid #2a2a4e;
        }}
        .chart-section h2 {{ color: #4fc3f7; font-size: 1em; margin-bottom: 15px; }}
        .chart-container {{ height: 300px; }}
        
        /* Calendar */
        .calendar-section {{
            background: #1a1a2e;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid #2a2a4e;
        }}
        .calendar-section h2 {{ color: #4fc3f7; font-size: 1em; margin-bottom: 15px; }}
        .calendar-grid {{ display: flex; gap: 4px; flex-wrap: wrap; }}
        .calendar-day {{
            width: 16px; height: 16px; border-radius: 3px;
            cursor: pointer; transition: transform 0.1s;
        }}
        .calendar-day:hover {{ transform: scale(1.5); }}
        .calendar-day.profit {{ background: #4caf50; }}
        .calendar-day.profit-high {{ background: #2e7d32; }}
        .calendar-day.loss {{ background: #f44336; }}
        .calendar-day.loss-high {{ background: #c62828; }}
        .calendar-day.empty {{ background: #2a2a3e; }}
        
        /* Stock Table */
        .table-section {{
            background: #1a1a2e;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid #2a2a4e;
        }}
        .table-section h2 {{ color: #4fc3f7; font-size: 1em; margin-bottom: 5px; }}
        .table-section .subtitle {{ color: #666; font-size: 0.8em; margin-bottom: 15px; }}
        
        .stock-table {{ width: 100%; border-collapse: collapse; }}
        .stock-table th {{
            background: #0f0f1a; color: #ffd700; padding: 12px 10px;
            text-align: left; font-size: 0.7em; text-transform: uppercase;
            letter-spacing: 1px; border-bottom: 2px solid #2a2a4e;
            cursor: pointer;
        }}
        .stock-table th:hover {{ background: #1a1a2e; }}
        .stock-table td {{ padding: 12px 10px; border-bottom: 1px solid #2a2a4e; font-size: 0.85em; }}
        .stock-table tr:hover {{ background: rgba(79, 195, 247, 0.05); }}
        .stock-symbol {{ font-weight: 600; color: #4fc3f7; }}
        .score-badge {{
            background: rgba(79, 195, 247, 0.2); color: #4fc3f7;
            padding: 4px 10px; border-radius: 20px; font-size: 0.85em; font-weight: 600;
        }}
        .signal-badge {{
            background: rgba(255, 215, 0, 0.2); color: #ffd700;
            padding: 4px 10px; border-radius: 20px; font-size: 0.85em;
        }}
        .positive {{ color: #4caf50; }}
        .negative {{ color: #f44336; }}
        
        /* Disclaimer */
        .disclaimer {{
            background: rgba(244, 67, 54, 0.1);
            border: 1px solid rgba(244, 67, 54, 0.3);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .disclaimer h3 {{ color: #f44336; font-size: 0.9em; margin-bottom: 10px; }}
        .disclaimer p {{ color: #888; font-size: 0.8em; line-height: 1.6; }}
        
        /* Footer */
        .footer {{ text-align: center; padding: 30px; color: #555; font-size: 0.8em; }}
        .footer a {{ color: #4fc3f7; text-decoration: none; }}
        
        @media (max-width: 768px) {{
            .stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .stock-table {{ font-size: 0.75em; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <h1>&#128202; Jim Simons Quant Screener</h1>
            <p class="subtitle">Renaissance-Style Pattern Analysis for Indian Markets</p>
            <p class="date">Last Updated: {today_date} | Screening {total_screened} Stocks</p>
        </div>
        
        <!-- Stats Grid -->
        <div class="stats-grid">
            <div class="stat-card">
                <div class="label">Portfolio Value</div>
                <div class="value neutral">&#8377;{current_value/100000:.2f}L</div>
                <div class="sub">Started: &#8377;10.00L</div>
            </div>
            <div class="stat-card">
                <div class="label">Total P&L</div>
                <div class="value {'positive' if absolute_return >= 0 else 'negative'}">&#8377;{absolute_return:+,.0f}</div>
                <div class="sub">{(absolute_return/initial_capital)*100:+.2f}% overall</div>
            </div>
            <div class="stat-card">
                <div class="label">Strategy Return</div>
                <div class="value {'positive' if strategy_return >= 0 else 'negative'}">{strategy_return:+.2f}%</div>
                <div class="sub">vs Market: {market_return:+.2f}%</div>
            </div>
            <div class="stat-card">
                <div class="label">Alpha (Edge)</div>
                <div class="value {'positive' if alpha >= 0 else 'negative'}">{alpha:+.2f}%</div>
                <div class="sub">{days_tracked} days tracked</div>
            </div>
            <div class="stat-card">
                <div class="label">Win Rate</div>
                <div class="value neutral">{win_rate:.0f}%</div>
                <div class="sub">Profitable days</div>
            </div>
            <div class="stat-card">
                <div class="label">Today (1D)</div>
                <div class="value {'positive' if portfolio_returns['1D'] >= 0 else 'negative'}">{portfolio_returns['1D']:+.2f}%</div>
                <div class="sub">Latest return</div>
            </div>
        </div>
        
        <!-- Chart -->
        <div class="chart-section">
            <h2>&#128200; Compounding: Strategy vs Market</h2>
            <div class="chart-container">
                <canvas id="equityChart"></canvas>
            </div>
        </div>
        
        <!-- Calendar -->
        <div class="calendar-section">
            <h2>&#128197; Daily Returns (Last 30 Days)</h2>
            <div class="calendar-grid">
                {calendar_html}
            </div>
        </div>
        
        <!-- Stock Table -->
        <div class="table-section">
            <h2>&#127381; Today's Top 25 Stock Picks</h2>
            <p class="subtitle">Stocks passing all quantitative filters</p>
            
            <table class="stock-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Stock</th>
                        <th>Score</th>
                        <th>Signals</th>
                        <th>Price</th>
                        <th>1D %</th>
                        <th>1W %</th>
                    </tr>
                </thead>
                <tbody>
                    {stock_rows}
                </tbody>
            </table>
        </div>
        
        <!-- Disclaimer -->
        <div class="disclaimer">
            <h3>&#9888; Important Disclaimer</h3>
            <p>
                <strong>FOR EDUCATIONAL PURPOSES ONLY.</strong> This is not financial advice. 
                This screener is not registered with SEBI. Past performance does not guarantee future results. 
                All investments carry risk. Please consult a registered financial advisor before making any investment decisions.
            </p>
        </div>
        
        <!-- Footer -->
        <div class="footer">
            <p>Inspired by <a href="https://en.wikipedia.org/wiki/Jim_Simons_(mathematician)" target="_blank">Jim Simons</a> &amp; Renaissance Technologies</p>
            <p>Data Source: Yahoo Finance | Auto-updated daily at 7:30 PM IST via GitHub Actions</p>
        </div>
    </div>
    
    <!-- Chart Script -->
    <script>
        const strategyData = {json.dumps(strategy_data)};
        const marketData = {json.dumps(market_data_eq)};
        const labels = {json.dumps(chart_labels)};
        
        const ctx = document.getElementById('equityChart').getContext('2d');
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: labels,
                datasets: [
                    {{
                        label: 'Strategy',
                        data: strategyData,
                        borderColor: '#4caf50',
                        backgroundColor: 'rgba(76, 175, 80, 0.1)',
                        fill: true,
                        tension: 0.3
                    }},
                    {{
                        label: 'Market (Nifty)',
                        data: marketData,
                        borderColor: '#888',
                        backgroundColor: 'rgba(136, 136, 136, 0.1)',
                        fill: true,
                        tension: 0.3
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ labels: {{ color: '#888' }} }}
                }},
                scales: {{
                    x: {{
                        grid: {{ color: 'rgba(255,255,255,0.05)' }},
                        ticks: {{ color: '#666', maxTicksLimit: 10 }}
                    }},
                    y: {{
                        grid: {{ color: 'rgba(255,255,255,0.05)' }},
                        ticks: {{
                            color: '#666',
                            callback: function(v) {{ return '₹' + (v/100000).toFixed(1) + 'L'; }}
                        }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""
    
    return html


if __name__ == "__main__":
    success = run_screener()
    exit(0 if success else 1)
