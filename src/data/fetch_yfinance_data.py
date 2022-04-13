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
    for i, company in rel_company.iteritems():
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
tickers= []
for comp in filter_list:
    for t, names in exchange_dict.items():
        if comp in names:
            tickers.append(t)


# %%
len(tickers)

# %%
l1= list(tickers[:100]) 
l2= list(tickers[100:200])
l3= list(tickers[300:400])
l4=list(tickers[500:600])
l5= list(tickers[600:700])
l6= list(tickers[700:])
df1= yf.download(l1, start= start_date)
time.sleep(70)
df2= yf.download(l2, start= start_date)
time.sleep(70)
df3= yf.download(l3, start= start_date)
time.sleep(70)
df4= yf.download(l4, start= start_date)
time.sleep(70)
df5= yf.download(l5, start= start_date)
time.sleep(70)
df6= yf.download(l6, start= start_date)


# %%
##for pulling parts of the file back into RAM if the yfinance pull does not finish
# output_file_path = product['data']
# df_dict= {}
# lv1= ['Open','Close','High','Low']
# for table_name in lv1:
#     df_dict.update({table_name : pd.read_excel(output_file_path, sheet_name=table_name, index_col= 0)})

# %%

# %%

lv1= ['Open','Close','High','Low']
df_dict= {}
for table_name in lv1:
    df_dict.update({table_name : df1[table_name]})
for dataframe in [df1,df2,df3,df4,df5,df6]:
    for table_name in lv1:
        df_dict[table_name]= pd.concat((df_dict[table_name], dataframe[table_name]), axis=1)




# %%
output_file_path = product['data']
i=False
for name, dataframe in df_dict.items():
    if i == False:
        dataframe.to_excel(output_file_path, sheet_name= name)
    else:
        with pd.ExcelWriter(output_file_path, engine='openpyxl', mode='a') as writer:  
            dataframe.to_excel(writer, sheet_name=name)
    i= True
