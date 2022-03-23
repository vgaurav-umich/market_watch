# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = None

# This is a placeholder, leave it as None
product = None


# %%
# your code here...
import yfinance as yf
import pandas as pd
from src import utils
import altair as alt

alt.data_transformers.disable_max_rows()
alt.themes.enable('fivethirtyeight')
# alt.renderers.enable('mimetype')
pd.set_option('display.float_format',  '{:,.2f}'.format)


# %%
tickers = query_params['tickers']
rolling_window = query_params['rolling_window']
# temp override
rolling_window = 1800 if rolling_window == 1 else rolling_window

start_date = utils.get_start_date(rolling_window)
start_date = utils.general_date_format(start_date)

# %%
ticker = yf.Ticker(tickers[0])

# %%
ticker_history = ticker.history(start=start_date)

# %%
history = ticker_history.reset_index()
base = alt.Chart(history).configure(
    font='monospace'
)

# %%
line_chart = base.mark_line().transform_calculate(
    validation = 'if(datum.Date > datetime("2021-03-01"), "True", "False")'
).encode(
    alt.X('Date:T'),
    alt.Y('Close'),
    alt.Color(
        'validation:O', 
        scale=alt.Scale(range=['green', 'orange']),
        legend=alt.Legend(
            values=['Traning', 'Validation'], 
            title='legend')
    )
).properties(
    width=900
)
line_chart

# %%
hist_chart = base.mark_bar().transform_bin(
    'Stock Price (Binned)', 
    'Close',
    bin=alt.Bin(maxbins=25)
).encode(
        alt.Y('count()', title='Number of Days'),
    alt.X('Stock Price (Binned)', type='nominal'),
)
hist_chart


# %%
ticker.institutional_holders

# %%
ticker.mutualfund_holders

# %%
ticker.splits

# %%
ticker.info

# %%
ticker.cashflow

# %%
ticker.balancesheet

# %%
ticker.analysis

# %%
ticker.calendar

# %%
ticker.earnings

# %%
ticker.financials

# %%
ticker.news

# %%
ticker.recommendations

# %%
