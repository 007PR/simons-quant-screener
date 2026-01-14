#!/usr/bin/env python3
"""
NSE Trading Calendar
====================

Checks if a given date is a trading day on NSE (National Stock Exchange of India).
Excludes weekends and NSE holidays.
"""

from datetime import datetime, date
from typing import List, Optional

# NSE Holidays for 2025 and 2026
# Source: NSE official holiday list
NSE_HOLIDAYS_2025 = [
    "2025-01-26",  # Republic Day
    "2025-02-26",  # Mahashivratri
    "2025-03-14",  # Holi
    "2025-03-31",  # Id-Ul-Fitr (Ramadan Eid)
    "2025-04-10",  # Shri Mahavir Jayanti
    "2025-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2025-04-18",  # Good Friday
    "2025-05-01",  # Maharashtra Day
    "2025-06-07",  # Bakri Id (Eid ul-Adha)
    "2025-08-15",  # Independence Day
    "2025-08-16",  # Parsi New Year
    "2025-08-27",  # Ganesh Chaturthi
    "2025-10-02",  # Mahatma Gandhi Jayanti
    "2025-10-21",  # Diwali Laxmi Pujan
    "2025-10-22",  # Diwali Balipratipada
    "2025-11-05",  # Prakash Gurpurab Sri Guru Nanak Dev
    "2025-12-25",  # Christmas
]

NSE_HOLIDAYS_2026 = [
    "2026-01-26",  # Republic Day
    "2026-02-17",  # Mahashivratri
    "2026-03-03",  # Holi
    "2026-03-20",  # Id-Ul-Fitr (Ramadan Eid)
    "2026-03-30",  # Shri Mahavir Jayanti
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-05-27",  # Bakri Id (Eid ul-Adha)
    "2026-06-25",  # Muharram
    "2026-08-15",  # Independence Day
    "2026-08-17",  # Ganesh Chaturthi
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-10-20",  # Dussehra
    "2026-11-09",  # Diwali Laxmi Pujan
    "2026-11-10",  # Diwali Balipratipada
    "2026-11-25",  # Prakash Gurpurab Sri Guru Nanak Dev
    "2026-12-25",  # Christmas
]

# Combine all holidays
NSE_HOLIDAYS = set(NSE_HOLIDAYS_2025 + NSE_HOLIDAYS_2026)


def is_weekend(d: date) -> bool:
    """Check if date is Saturday (5) or Sunday (6)"""
    return d.weekday() >= 5


def is_nse_holiday(d: date) -> bool:
    """Check if date is an NSE holiday"""
    date_str = d.strftime("%Y-%m-%d")
    return date_str in NSE_HOLIDAYS


def is_trading_day(d: Optional[date] = None) -> bool:
    """
    Check if the given date is a trading day on NSE.
    
    Args:
        d: Date to check (default: today)
    
    Returns:
        True if it's a trading day, False otherwise
    """
    if d is None:
        d = date.today()
    
    if isinstance(d, datetime):
        d = d.date()
    
    # Check weekend
    if is_weekend(d):
        return False
    
    # Check holiday
    if is_nse_holiday(d):
        return False
    
    return True


def get_next_trading_day(d: Optional[date] = None) -> date:
    """
    Get the next trading day from given date.
    
    Args:
        d: Starting date (default: today)
    
    Returns:
        Next trading day
    """
    if d is None:
        d = date.today()
    
    if isinstance(d, datetime):
        d = d.date()
    
    from datetime import timedelta
    
    next_day = d + timedelta(days=1)
    while not is_trading_day(next_day):
        next_day += timedelta(days=1)
    
    return next_day


def get_previous_trading_day(d: Optional[date] = None) -> date:
    """
    Get the previous trading day from given date.
    
    Args:
        d: Starting date (default: today)
    
    Returns:
        Previous trading day
    """
    if d is None:
        d = date.today()
    
    if isinstance(d, datetime):
        d = d.date()
    
    from datetime import timedelta
    
    prev_day = d - timedelta(days=1)
    while not is_trading_day(prev_day):
        prev_day -= timedelta(days=1)
    
    return prev_day


def get_trading_day_reason(d: Optional[date] = None) -> str:
    """
    Get the reason why a date is not a trading day.
    
    Args:
        d: Date to check (default: today)
    
    Returns:
        Reason string or "Trading Day" if it is one
    """
    if d is None:
        d = date.today()
    
    if isinstance(d, datetime):
        d = d.date()
    
    if is_weekend(d):
        day_name = d.strftime("%A")
        return f"Weekend ({day_name})"
    
    if is_nse_holiday(d):
        return "NSE Holiday"
    
    return "Trading Day"


def is_market_hours(dt: Optional[datetime] = None) -> bool:
    """
    Check if current time is during market hours (9:15 AM - 3:30 PM IST).
    
    Args:
        dt: Datetime to check (default: now)
    
    Returns:
        True if during market hours
    """
    if dt is None:
        dt = datetime.now()
    
    # NSE market hours: 9:15 AM to 3:30 PM IST
    market_open = dt.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = dt.replace(hour=15, minute=30, second=0, microsecond=0)
    
    return market_open <= dt <= market_close


def is_after_market_close(dt: Optional[datetime] = None) -> bool:
    """
    Check if current time is after market close (after 3:30 PM IST).
    Best time to run screener.
    
    Args:
        dt: Datetime to check (default: now)
    
    Returns:
        True if after market close
    """
    if dt is None:
        dt = datetime.now()
    
    market_close = dt.replace(hour=15, minute=30, second=0, microsecond=0)
    return dt > market_close


def get_market_status() -> dict:
    """
    Get current market status.
    
    Returns:
        Dict with status info
    """
    now = datetime.now()
    today = now.date()
    
    is_trading = is_trading_day(today)
    reason = get_trading_day_reason(today)
    
    if not is_trading:
        return {
            'is_open': False,
            'is_trading_day': False,
            'reason': reason,
            'next_trading_day': get_next_trading_day(today).strftime("%Y-%m-%d"),
            'message': f"Market closed: {reason}. Next trading day: {get_next_trading_day(today).strftime('%d %b %Y')}"
        }
    
    if is_market_hours(now):
        return {
            'is_open': True,
            'is_trading_day': True,
            'reason': 'Market Open',
            'message': "Market is currently open (9:15 AM - 3:30 PM IST)"
        }
    elif is_after_market_close(now):
        return {
            'is_open': False,
            'is_trading_day': True,
            'reason': 'After Hours',
            'message': "Market closed for today. Good time to run screener!"
        }
    else:
        return {
            'is_open': False,
            'is_trading_day': True,
            'reason': 'Pre-Market',
            'message': "Market opens at 9:15 AM IST"
        }


if __name__ == "__main__":
    print("NSE Trading Calendar")
    print("=" * 50)
    
    today = date.today()
    print(f"\nToday: {today.strftime('%d %b %Y (%A)')}")
    print(f"Is Trading Day: {is_trading_day(today)}")
    print(f"Reason: {get_trading_day_reason(today)}")
    
    status = get_market_status()
    print(f"\nMarket Status: {status['message']}")
    
    if not is_trading_day(today):
        next_td = get_next_trading_day(today)
        print(f"Next Trading Day: {next_td.strftime('%d %b %Y (%A)')}")
    
    prev_td = get_previous_trading_day(today)
    print(f"Previous Trading Day: {prev_td.strftime('%d %b %Y (%A)')}")
    
    print("\n" + "=" * 50)
    print("Upcoming NSE Holidays:")
    for h in sorted(NSE_HOLIDAYS):
        h_date = datetime.strptime(h, "%Y-%m-%d").date()
        if h_date >= today:
            print(f"  {h_date.strftime('%d %b %Y (%A)')}")
