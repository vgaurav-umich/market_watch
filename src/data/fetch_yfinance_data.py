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
pd.set_option('display.float_format',  '{:,.2f}'.format)


# %%
tickers = query_params['tickers']
rolling_window = query_params['rolling_window']

start_date = utils.get_start_date(rolling_window)
start_date = utils.general_date_format(start_date)

# %%
ticker = yf.Ticker(tickers[0])

# %%
ticker_history = ticker.history(start=start_date)

# %%
history = ticker_history.reset_index()
output_file_path = product['data']
history.to_csv(output_file_path)