# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# + tags=["injected-parameters"]
# This cell was injected automatically based on your stated upstream dependencies (cell above) and pipeline.yaml preferences. It is temporary and will be removed when you save this notebook
query_params = {
    "rolling_window": 90,
    "search_term": "tesla",
    "bq_table_name": "`gdelt-bq.gdeltv2.gkg`",
    "sp500_path": "data/external/",
    "exchanges_path": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\nasdaq_nyse_amex.csv",
}
product = {
    "nb": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\notebooks\\fetch_n_filter_gdelt_bq.ipynb",
    "data": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\data\\raw",
}


# + tags=[]
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from google.cloud import bigquery
import pandas as pd
import datetime
from tqdm import tqdm
import re
import collections
import json

import os
import sys
import warnings
from src import utils

# import numpy as np
# import altair as alt
# from altair import datum
# alt.data_transformers.disable_max_rows()
# alt.themes.enable('fivethirtyeight')

warnings.filterwarnings('ignore')

# + tags=[]
load_dotenv(find_dotenv('market_watch.env'))

# + tags=[]
print(sys.executable)
api_key_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
print(api_key_file)


# + tags=[]
def build_gdelt_query(table_name, search_term, start_date):
    query_string = f"""
        SELECT
          GKGRECORDID,
          DATE,
          SourceCollectionIdentifier,
          DocumentIdentifier,
          V2Locations AS Locations,
          V2Persons AS Persons,
          V2Organizations AS Organizations,
          V2Tone AS Tone
        FROM
          {table_name}
        WHERE
          LOWER(Organizations) LIKE "%{search_term}%"
          AND DATE > {start_date}
    """
    return query_string


# + tags=[]
def build_gdelt_query_for_counter(table_name, start_date, end_date):
    query_string= f'''
        Select
            V2Organizations As Organizations
        From
            {table_name}
        Where
            date between {start_date} and {end_date}
    '''
    return query_string


# + tags=[]
def fetch_data(bqclient, query_string):
    df = (
        bqclient.query(query_string)
            .result()
            .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    return df


# + tags=[]
rolling_window = query_params["rolling_window"]
table_name = query_params["bq_table_name"]
search_term = query_params["search_term"]
file_path = product["data"] + 'gdelt_gkg_bqdata-raw.csv'
# file_path2= product["data"] + 'gdelt_gkg_bqdata-counter.csv'
start_date = utils.get_start_date(rolling_window)
start_date = utils.gdelt_date_format(start_date)
query = build_gdelt_query(table_name, search_term, start_date)
# counter_query= build_gdelt_query_for_counter(table_name, start_date)
# print(query)

# + tags=[]
client = bigquery.Client()
data_df = fetch_data(client, query)
print(f"Processed merged file with {len(data_df)} records")

# + tags=[]
# Save records
Path(file_path).parent.mkdir(exist_ok=True, parents=True)
data_df.to_csv(file_path)
print(f"Saved file {file_path}")
del data_df


# + tags=[]
def create_increments(start_date):
    dt= pd.to_datetime(start_date)
    range_= pd.date_range(dt, end= datetime.date.today(), freq='2D')
    string_range= []
    for date in range_:
        # print(date)
        string_range.append(utils.gdelt_date_format(date))
        # string_range.append(f'{date.year}{date.month}{date.day}000000')
    end_range= string_range[1:]
    date_range= zip(string_range, end_range)
    return list(date_range)

def get_rel_company_names(path):
    rel_company = pd.read_csv(path)
    rel_company = rel_company['Name']
    rel_company = rel_company.str.lower()
    rel_company = rel_company[rel_company.str.contains('common|ordinary', regex=True)]
    rel_company = rel_company.str.split('(corp|ltd|inc|corporation|limited|incorporation|incorporated)',regex=True)
    rel_company = rel_company.map(lambda x: ''.join(x[:2]))
    expand_rel_company = {}
    # stop_words= ['unit', 'common', 'class', 'warrants', 'warrant', 'depository']
    for company in rel_company:
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
        expand_rel_company.update({company: company_name_list})
    return expand_rel_company



# +
# exchanges_path= query_params['exchanges_path']
# exchanges_dict= get_rel_company_names(exchanges_path)
# exchanges_dict

# + tags=[]
# %%time
# C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\

# sp_500= query_params['sp500_path'] + 'sp500_list.xlsx'
# sp500_dict= get_rel_company_names(sp_500)
exchanges_path= query_params['exchanges_path']
exchanges_dict= get_rel_company_names(exchanges_path)

def fetch_counter_data(bqclient, table_name, start, sp500_dict):
    word_list= []
    date_range= create_increments(start)
    sp500_names= [val for list_ in sp500_dict.values() for val in list_]
    if len(date_range) == 0:
        end_date= utils.gdelt_date_format(datetime.date.today())
        # end_date= '20220330153000'
        date_range= [(start, end_date)]
    for start_dates, end_date in tqdm(date_range):

        query_string= build_gdelt_query_for_counter(table_name, start_dates, end_date)
        text = (
            bqclient.query(query_string)
                .result()
                .to_dataframe(
                # Optionally, explicitly request to use the BigQuery Storage API. As of
                # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
                # API is used by default.
                create_bqstorage_client=True,
                ))
        print('Size: ', len(text))
        # print('Query: ', datetime.datetime.now())
        # text= re.sub('\\n\d+' , ' ', series.to_string())
        text= text.replace('Organizations', '', limit= 1)
        text.dropna(inplace=True)
        text= text['Organizations'].map(lambda x: re.sub('\s{2,}', '', x))
        text= text.str.lower()
        text= text.map(lambda x: re.split(',\d+;?', x))
        # print('Mapping: ', datetime.datetime.now())
        
        sp_count=0
        for n, item in text.iteritems():
            for word in item:
                if word in sp500_names:
                    for key, names_list in sp500_dict.items():
                        if word in names_list:
                            word_list.append(key)
                            sp_count+=1
                else:
                    word_list.append(word)
    result= collections.Counter(word_list)
    result= {k:v for k,v in sorted(result.items(), key= lambda x: x[1], reverse=True)}
    print(sp_count)
    
    return result

counts= fetch_counter_data(client, table_name, start_date, exchanges_dict)


# + tags=[]
with open(product['data'] + '\\org_totals.txt', 'w') as convert_file:
     convert_file.write(json.dumps(counts))

# + tags=[]
# start_date, end_date= ('20220101231500', '20220101233000')
# query_string= build_gdelt_query_for_counter(table_name, start_date, end_date)
# text= (
#             client.query(query_string)
#                 .result()
#                 .to_dataframe(
#                 # Optionally, explicitly request to use the BigQuery Storage API. As of
#                 # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
#                 # API is used by default.
#                 create_bqstorage_client=True,
#                 ))

# + tags=[]
# x= text.copy().replace('Organizations', '', limit= 1)
# x= x.dropna()
# x= x['Organizations'].map(lambda x: re.sub('\s{2,}', '', x))
# x=x.map(lambda x: re.split(',\d+;?', x))


# + tags=[]
# with open(product['data'] + '\\org_totals.txt', 'w') as convert_file:
#      convert_file.write(json.dumps({'a':'b'}))
