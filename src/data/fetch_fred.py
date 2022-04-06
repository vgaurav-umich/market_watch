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
upstream = None

# This is a placeholder, leave it as None
product = None


# %%
# your code here...

import requests
import pandas as pd
import numpy as np
import warnings

pd.options.display.max_rows=500
pd.options.display.max_columns=100
warnings.filterwarnings("ignore")

# %%
r=requests.get('https://api.stlouisfed.org/fred/series', {
    'series_id' : 'VIXCLS',
    'api_key' : 'ebc6d771a26e9b8009c65cb0ab76ba3d', 
    'file_type':'json'
})

r.json()


# %%
# r=requests.get('https://api.stlouisfed.org/fred/series/observations', {
#     'series_id' : 'SP500',
#     'api_key' : 'ebc6d771a26e9b8009c65cb0ab76ba3d',
#     'file_type': 'json',
#     'observation_start': '2022-01-01',
#     'observation_end' : '2022-01-05',
#     'units' : 'lin',
#     'frequencies': 'D'   
# })

# r= r.json()

# %%
def get_series_metainfo(series_id, api_key):
    '''Returns the metadata for the series_id'''
    r= requests.get('https://api.stlouisfed.org/fred/series', {
    'series_id' : series_id,
    'file_type': 'json',
    'api_key' : api_key})
    json= r.json()
    json=json['seriess'][0]
    
    try:
        result= pd.Series([json['title'],
                     json['id'],
                     json['frequency_short'],
                     json['seasonal_adjustment'],
                     json['popularity'],
                     json['observation_start'],
                     json['observation_end'],
                     json['notes']], index= ['Title','ID','Frequency','Seasonally Adjusted', 'Popularity','Observation Start', 'Observation End', 'Notes'])
    except:
        result= pd.Series([json['title'],
                     json['id'],
                     json['frequency_short'],
                     json['seasonal_adjustment'],
                     json['popularity'],
                     json['observation_start'],
                     json['observation_end'],
                     None], index= ['Title','ID','Frequency','Seasonally Adjusted', 'Popularity','Observation Start', 'Observation End', 'Notes'])
    return result

def get_series_obs(series_id, api_key, obs_start, obs_end, freq, units= 'lin'):
    '''Returns the observations for series_id'''
    
    json= requests.get('https://api.stlouisfed.org/fred/series/observations',{
        'series_id' : series_id,
        'file_type': 'json',
        'api_key' : api_key,
        'observation_start': obs_start,
        'observation_end' : obs_end,
        'units' : units,
        'frequencies': freq 
    }).json()
    json= json['observations']
    df= pd.DataFrame(json)
    df= df[['date','value']]
    df.columns= ['DATE',series_id]
    return df

def convert_dfs_to_daily(df):
    '''
    Takes FRED data and returns a df convertion to daily timeframe
    using ffill. Example... a monthly frequency will be converted to daily (weekdays only) where the last recorded value
    will be propagated forward.
    '''

    cols= df.columns
    df.iloc[:,1]= df.iloc[:,1].replace('.', np.nan)
    df.dropna(inplace=True)
    df.reset_index(inplace=True, drop=True)
    all_days_df= pd.DataFrame()
    for i, row in df.iterrows():
        all_days_df= all_days_df.append(row)
        if i+1 < df.shape[0]:
            time_delta= df.loc[i+1, 'DATE'] - row['DATE']
            if time_delta > pd.Timedelta(1,'D'):
                range_= pd.date_range(row['DATE'],df.loc[i+1,'DATE'])[1:-1]
                for n, d in enumerate(range_, 1):
                    all_days_df= all_days_df.append({df.columns[0] : pd.to_datetime(d),
                                                    df.columns[1]: np.nan}, ignore_index=True)
                all_days_df.fillna(method='ffill',inplace=True)  
    return all_days_df


# %%

# %%
# %%time
series_ids= api_params['series_list'].split(' ')
key= api_params['api_key']
obs_start= api_params['obs_start']
obs_stop= api_params['obs_stop']
unit= api_params['units']


meta_df= pd.DataFrame()
obs_df= pd.DataFrame(columns= ['DATE'])

for id_ in series_ids:
    meta_df= meta_df.append(get_series_metainfo(id_, key), ignore_index=True)

for id_ in series_ids:
    print(id_)
    freq= meta_df[meta_df['ID'] == id_]['Frequency']
    series_df= get_series_obs(id_, key, obs_start, obs_stop, freq)
    series_df['DATE']= series_df['DATE'].astype('datetime64')
    series_df= convert_dfs_to_daily(series_df)
    obs_df= obs_df.merge(series_df, how='outer', on='DATE')
obs_df.dropna(axis=0, inplace=True)
obs_df.reset_index(drop=True, inplace=True)

# %%
meta_df

# %%
dt= "<class 'pandas._libs.tslibs.timestamps.Timestamp'>"

def fix_value(cell):
    '''Corrects issue where floats were becoming pd.datetimes from the API'''
    if str(type(cell)) == dt:
        if int(cell.month) < 10:
            return f'{cell.year}.0{cell.month}'
        else:
            return f'{cell.year}.{cell.month}'
    else:
        return cell
obs_df.iloc[:,1:]= obs_df.iloc[:,1:].applymap(fix_value)



obs_df.iloc[:, 1:]= obs_df.iloc[:, 1:].astype(float)
obs_df.to_csv(product['data'])
meta_df.to_csv('C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\meta\\fred_series_meta.csv')

