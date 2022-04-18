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
upstream = ['total_org_count']

# This is a placeholder, leave it as None
product = None


# %%
# your code here...
import yfinance as yf
import pandas as pd
from src import utils
from tqdm import tqdm
import time
from pathlib import Path

pd.set_option('display.float_format',  '{:,.2f}'.format)


# %%
securities_file_path = upstream['total_org_count']['data']

securities_df = pd.read_csv(securities_file_path, index_col=0)
securities_df.info()

# %%
ticker_names = securities_df.ticker.to_list()

# %%
start_date = utils.get_start_date(rolling_window)
start_date = utils.general_date_format(start_date)

# %%
df= yf.download(ticker_names, start=start_date, threads=True, progress=True, timeout=1)


# %%
df = df.sort_index()

# %%
output_file_path = product['data']
file_prefix = "snp500" if snp_filter == True else "all"

parent_path = Path(output_file_path).parent

for field in yahoo_fields:
    file_path = str(parent_path) + f"/yfinance_{file_prefix}_ticker_data_{field}-raw.csv"
    df[field].to_csv(file_path)
    print(f"Saved file {file_path}")

df.to_csv(output_file_path)
print(f"Saved file {output_file_path}")  

# %%
with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    for field in yahoo_fields:
        df[field].to_excel(writer, sheet_name=field)
        print(f"Saved sheet {field}")
    
print(f"Saved file {output_file_path}")

# %%
del df, securities_df

# %%
