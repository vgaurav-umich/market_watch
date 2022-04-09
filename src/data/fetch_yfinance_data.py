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
from tqdm import tqdm
import time

alt.data_transformers.disable_max_rows()
alt.themes.enable('fivethirtyeight')
pd.set_option('display.float_format',  '{:,.2f}'.format)


# %%
ticker_path= query_params['sp_500_path']
rolling_window = query_params['fin_ec_rolling_window']

# C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\sp500_list.xlsx
ticker_names= pd.read_excel(ticker_path + 'sp500_list.xlsx')['Symbol']

start_date = utils.get_start_date(rolling_window)
start_date = utils.general_date_format(start_date)

# %%
l1= list(ticker_names[:250]) 
l2= list(ticker_names[250:])
df= yf.download(l1, start= start_date)
# time.sleep(61)
# df2= yf.download(l2, start= start_date)
# df= df.concat([df,df2])


# %%

# %%
df = df.reset_index()
output_file_path = product['data']
df.to_csv(output_file_path)
