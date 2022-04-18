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

import requests
import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm
import json
from pathlib import Path

pd.options.display.max_rows=500
pd.options.display.max_columns=100
warnings.filterwarnings("ignore")


# %%
def fetch_fred_data(fred_url, series_list, api_key, obs_start, obs_stop, record_path, meta_columns=None):
    
    session = requests.Session()
    query_params =  {
        'file_type': 'json',
        'api_key' : api_key,
        'observation_start': obs_start,
        'observation_end' : obs_stop,
    }
    
    df_lst = []
    for series_id in tqdm(series_list):
        query_params['series_id'] = series_id
        response = session.get(
           fred_url,
           params=query_params
        )
        data = json.loads(response.text)
        df = pd.json_normalize(
            data, 
            record_path=record_path, 
            meta=meta_columns,
        )
        df['id'] = series_id
        df_lst.append(df)

    df = pd.concat(df_lst)
    
    return df


# %%
def get_series_info(fred_series_info_url, series_list, api_key, obs_start, obs_stop):
    
    record_path = ['seriess']
    df = fetch_fred_data(fred_series_info_url, series_list, api_key, obs_start, obs_stop, record_path)
    
    return df


# %%
def get_series_observations(fred_series_observations_url, series_list, api_key, obs_start, obs_stop):
    
    record_path = ['observations']
    df = fetch_fred_data(fred_series_observations_url, series_list, api_key, obs_start, obs_stop, record_path)
    df = df[['id','date','value']]
    return df


# %%
def clean_series_data(series_df):
    
    series_df.date = pd.to_datetime(series_df.date)
    # holidays are represented as . in dataset, let's replace them by null values
    series_df.value = series_df.value.replace('.', np.nan)
    # convert value to float as it comes as object
    series_df.value = series_df.value.astype(float)
    
    return series_df


# %%
def resample(series_df, method='ffill'):
    series_df = series_df.set_index('date')
    sampled = series_df.resample('B')
    sampled_df = sampled.interpolate(method=method)
    sampled_df = sampled_df.reset_index()
    return sampled_df


# %%
def convert_to_wide_format(series_obs_df):
    df_lst = []
    #  we need to resample each series individually becuase of date collission
    series_list = series_obs_df.id.unique()
    for series_id in series_list:
        # filter for given series id    
        filter_cond = series_obs_df['id'] == series_id
        # get individual series dataframe    
        series_df = series_obs_df[filter_cond]
        series_df = resample(series_df)
        series_df = series_df.pivot(index='date', columns='id', values='value')
        df_lst.append(series_df)
    
    df = pd.concat(df_lst, axis=1).sort_index()
    
    return df


# %%
series_info_df = get_series_info(fred_series_info_url, series_list, api_key, obs_start, obs_stop)
series_info_df.columns

# %%
# need to call below functions in order
series_obs_df = get_series_observations(fred_series_observations_url, series_list, api_key, obs_start, obs_stop)
series_obs_df.columns

# %%
series_obs_df = clean_series_data(series_obs_df)
series_obs_df.info()

# %%
series_wide_df = convert_to_wide_format(series_obs_df)
series_wide_df = series_wide_df.fillna(method='ffill')
series_wide_df.columns

# %%
series_wide_df.tail(25)

# %%
output_file_path = product['data']
parent_file_path = Path(output_file_path).parent
series_info_file_path = str(parent_file_path) + "/fred_series_info.csv"
print(f"Writing to {output_file_path} \n {series_info_file_path}")

# %%
series_wide_df.to_csv(output_file_path)
series_info_df.to_csv(series_info_file_path)


# %%
