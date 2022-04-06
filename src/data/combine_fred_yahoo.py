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

# %%
# Add description here
#
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)


# %%
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# %% tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = ['fetch_fred', 'fetch_yfinance_data']

# This is a placeholder, leave it as None
product = None


# %%
# your code here...

# %%
import pandas as pd

# %%
fred_df= pd.read_csv(upstream['fetch_fred']['data'], parse_dates=["DATE"])
fred_df.drop(columns= 'Unnamed: 0', inplace=True)
fred_df

# %%
yahoo_df= pd.read_csv(upstream['fetch_yfinance_data']['data'], parse_dates=['Date'])
yahoo_df

# %%
df= fred_df.merge(yahoo_df, how='inner', left_on= 'DATE', right_on='Date')
df

# %%
path= product['data']
df.to_csv(path, index=False)
