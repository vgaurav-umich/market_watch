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
upstream = ['fetch_fred', 'fetch_yfinance_data']

# This is a placeholder, leave it as None
product = None


# %%
# your code here...

# %%
import pandas as pd

# %%
fred_df= pd.read_csv(upstream['fetch_fred']['data'], parse_dates=["date"], index_col=0)
fred_df.info()

# %%
fred_df.index.name = 'Date'

# %%
yahoo_path = upstream['fetch_yfinance_data']['data']
df_dict= {}
lv1= ['Open','Close','High','Low', 'Volume']
for table_name in lv1:
    df_dict.update({table_name : pd.read_excel(yahoo_path, sheet_name=table_name, index_col= 0)})

# %%
df_dict['Open'].info()

# %%
df_dict['Open'].index.name

# %%
output_file_path = product['data']

# %%
with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:  
    print(f"Saved file with {output_file_path}")
    for name, dataframe in df_dict.items(): 
        dataframe = fred_df.merge(df_dict[name], how='inner', left_on='Date', right_on='Date')
        dataframe.to_excel(writer, sheet_name=name)
        print(f"Saved file with {len(dataframe)} records.")

# %%
