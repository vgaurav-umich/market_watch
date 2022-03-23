from datetime import datetime, timedelta
import pandas as pd


# +
def gdelt_date_format(utc_date):
    
    return utc_date.strftime("%Y%m%d%H%M00")

def general_date_format(utc_date):
    
    return utc_date.strftime("%Y-%m-%d")

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def get_start_date(window):
    # get today's UTC datetime
    today = datetime.utcnow()
    # round datetime to nearest 15min slot
    pd_ts = pd.Timestamp(today)
    today_ts = pd_ts.round('15T')
    # how far back we want to go?
    how_far_back = timedelta(days=window)
    start_date = today_ts - how_far_back
    
    return start_date
