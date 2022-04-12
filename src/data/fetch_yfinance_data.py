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
upstream = ['clean_gdelt_data']

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
import json
import os
from src import utils

alt.data_transformers.disable_max_rows()
alt.themes.enable('fivethirtyeight')
pd.set_option('display.float_format',  '{:,.2f}'.format)


# %%
def normalize_company_names(path):
    '''
    Helper function that takes the stock names and adds variations to the name. 
    Example: American Airlines Group Inc. becomes a dict that includes: 
    American, American Airlines, American Airlines Group, & American Airlines Group Incorporated.
    '''
    rel_company = pd.read_csv(path)
    rel_company = rel_company[rel_company['Market Cap'] >= 100000000]
    ticker= rel_company['Symbol']
    rel_company = rel_company['Name']
    rel_company = rel_company.str.lower()
    rel_company = rel_company[rel_company.str.contains('common|ordinary', regex=True)]
    rel_company = rel_company.str.split('(corp|ltd|inc|corporation|limited|incorporation|incorporated)',regex=True)
    rel_company = rel_company.map(lambda x: ''.join(x[:2]))
    expand_rel_company = {}
    # stop_words= ['unit', 'common', 'class', 'warrants', 'warrant', 'depository']
    for i, company in enumerate(rel_company):
        # company = company.lower()
        company_name_list = []
        company_name_list.append(company)
        if 'inc' in company_name_list[0]:
            company_name_list.append(company_name_list[0] + 'orporation')
            company_name_list.append(company_name_list[0] + 'orporated')
        elif 'corp' in company_name_list[0]:
            company_name_list.append(company_name_list[0] + 'oration')
        elif 'ltd' in company_name_list[0]:
            w_name= company_name_list[0].split('ltd')[0]
            company_name_list.append(w_name + 'limited ')
            company_name_list.append(w_name + 'limited company')
        elif 'corporation' in company_name_list[0]:
            w_name= company_name_list[0].split('corporation')[0]
            company_name_list.append(w_name + 'corp')
        words = company.split(' ')
        for n in range(1, len(words)):
            if words[0:n] not in company_name_list:
                company_name_list.append(' '.join(words[0:n]))
        expand_rel_company.update({ticker[i]: company_name_list})
    return expand_rel_company


# %%
# %%time
# ticker_path= query_params['sp_500_path']
# ticker_names= pd.read_excel(ticker_path + 'sp500_list.xlsx')['Symbol']
rolling_window = query_params['fin_ec_rolling_window']
start_date = utils.get_start_date(rolling_window)
start_date = utils.general_date_format(start_date)


exchange_path= query_params['exchanges_path']
if not os.path.exists(exchange_path):
    os.chdir('..')
    os.chdir('..')
exchange_dict= normalize_company_names(exchange_path)
exchange_company_list= {val for list_ in exchange_dict.values() for val in list_}

    
file_path= upstream['clean_gdelt_data']['data']
gdelt_df= pd.read_parquet(file_path)
gdelt_df.dropna(subset= 'Org Count', inplace=True)
gdelt_df= gdelt_df['Org Count'].map(lambda x : {k.lower() for k,v in x.items() if v != None})
companies=set()
for n, dict_ in gdelt_df.iteritems():
    companies.update(dict_)
del gdelt_df
filter_list= companies.intersection(exchange_company_list)


# %%
for v in filter_list:
    if 'honda' in v:
        print(v)

# %%
tickers=pd.read_csv(exchange_path)
# tickers= tickers[tickers['Name'].isin(filter_list)]
# tickers.shape
print(tickers.shape)
print(len(exchange_dict))

# %%
l1= list(ticker_names[:250]) 
l2= list(ticker_names[250:])
df1= yf.download(l1, start= start_date)
# time.sleep(61)
# df2= yf.download(l2, start= start_date)
# df= df.concat([df,df2])


# %%
# df1= pd.read_csv(product['data'], header= [0,1], infer_datetime_format=True, parse_dates= [('Date', 'Unnamed: 2_level_1')])
# df1= df1.set_index(('Date', 'Unnamed: 2_level_1'))

# %%
df= pd.concat((df1['Open'], df2['Open']),axis=1)
# c= sorted(df.columns)
# df= df[c]

# %%
df

# %%
# df = df.reset_index()
output_file_path = product['data']
df.to_csv(output_file_path)
