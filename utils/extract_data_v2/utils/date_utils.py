# -*- coding: utf-8 -*-
import datetime as dt
from typing import Tuple
import pytz
from dateutil.relativedelta import relativedelta
import calendar

TZ_LIMA = pytz.timezone('America/Lima')

def get_current_lima_time() -> dt.datetime:
    """Get current time in Lima timezone"""
    return dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

def get_date_parts() -> Tuple[str, str, str]:
    """Get current year, month, day in Lima timezone"""
    now = get_current_lima_time()
    return now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')

def transform_to_datetime(date_str: str) -> dt.datetime:
    """Convert string date to datetime object"""
    return dt.datetime(
        year=int(date_str[:4]),
        month=int(date_str[5:7]),
        day=int(date_str[8:10]),
        hour=int(date_str[11:13]),
        minute=int(date_str[14:16]),
        second=int(date_str[17:19])
    )

def get_date_limits(month_diff: str, data_type: str) -> Tuple[str, str]:
    """Get lower and upper limits for date filters"""
    month_diff = month_diff.strip().replace("'", "")
    
    upper_limit = get_current_lima_time()
    lower_limit = upper_limit - relativedelta(months=(-1 * int(month_diff)))
    
    if data_type == "aje_period":
        return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')
    
    elif data_type in ["aje_date", "aje_processperiod"]:
        # Get last day of month for upper limit
        _, last_day = calendar.monthrange(upper_limit.year, upper_limit.month)
        upper_limit = upper_limit.replace(day=last_day, tzinfo=TZ_LIMA)
        lower_limit = lower_limit.replace(day=1, tzinfo=TZ_LIMA)
        
        # Convert to Excel date format
        upper_limit_days = (upper_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
        lower_limit_days = (lower_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
        
        return str(lower_limit_days), str(upper_limit_days)
    
    return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')

def format_date_for_db(date_str: str, date_type: str) -> str:
    """Format date string for specific database types"""
    if date_type == 'smalldatetime':
        return f"CONVERT(smalldatetime, '{date_str}', 120)"
    elif date_type == 'DATE':
        return f"TO_DATE('{date_str[:19]}', 'YYYY-MM-DD HH24:MI:SS')"
    elif date_type == 'TIMESTAMP(6)':
        return f"TO_TIMESTAMP('{date_str}', 'YYYY-MM-DD HH24:MI:SS.FF')"
    elif date_type == 'SQL_DATETIME':
        return f"CONVERT(DATETIME, '{date_str}', 102)"
    elif date_type == 'BIGINT':
        end_dt = dt.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return str(int(end_dt.timestamp()))
    else:
        return f"'{date_str}'"