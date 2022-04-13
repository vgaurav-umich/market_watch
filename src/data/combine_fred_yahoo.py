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
# %%time
yahoo_path = upstream['fetch_yfinance_data']['data']
df_dict= {}
lv1= ['Open','Close','High','Low']
for table_name in lv1:
    df_dict.update({table_name : pd.read_excel(yahoo_path, sheet_name=table_name, index_col= 0)})

# %%
# t=df_dict['Open']
i=0
for n in t.columns:
    if '.' in n:
        i+=1
i

# %%
output_file_path = product['data']
i=False
for name, dataframe in df_dict.items():
    if i == False:
        dataframe= fred_df.merge(df_dict[name],how='inner', left_on='DATE', right_on='Date')
        dataframe.to_excel(output_file_path, sheet_name= name)
    else:
        with pd.ExcelWriter(output_file_path, engine='openpyxl', mode='a') as writer:  
            dataframe.to_excel(writer, sheet_name=name)
    i= True

# %%
# fred_df.merge(df_dict['Open'],how='inner', left_on='DATE', right_on='Date')

# %%
