# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# +
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
# -

load_dotenv(find_dotenv('market_watch.env'))

print(sys.executable)
api_key_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
print(api_key_file)


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

client = bigquery.Client()
data_df = fetch_data(client, query)
print(f"Processed merged file with {len(data_df)} records")

# Save records
Path(file_path).parent.mkdir(exist_ok=True, parents=True)
data_df.to_csv(file_path)
print(f"Saved file {file_path}")
del data_df


# +
# data_df= fetch_data(client, counter_query)
# print(f"Processed merged file with {len(data_df)} records")

# #Save the counter record
# Path(file_path).parent.mkdir(exist_ok=True, parents=True)
# data_df.to_csv(file_path2)
# print(f"Saved file {file_path2}")

# +
# Going to refactor code for counter pull to perform api calls in chunks and perform filter and count on chunks

# +
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
    rel_company = pd.read_excel(path)
    rel_company = rel_company['Security']
    expand_rel_company = {}
    for company in rel_company:
        company = company.lower()
        company_name_list = []
        company_name_list.append(company)
        for postfix in ['inc', 'inc.', 'incorporation', 'corp.', 'corp', 'corporation']:
            if postfix in company_name_list[0]:
                break
        for postfix in ['inc', 'incorporation', 'corp', 'corporation']:
            company_name_list.append(company_name_list[0] + ' ' + postfix)
        words = company.split(' ')
        for n in range(1, len(words)):
            if words[0:n] not in company_name_list:
                company_name_list.append(' '.join(words[0:n]))
        expand_rel_company.update({company: company_name_list})
    return expand_rel_company



# +
# %%time
print('Start time: ', datetime.datetime.now())
# C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\
sp_500= query_params['sp500_path'] + 'sp500_list.xlsx'
sp500_dict= get_rel_company_names(sp_500)
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

counts= fetch_counter_data(client, table_name, '20220404000000', sp500_dict)


# -


with open(product['data'] + '\\org_totals.txt', 'w') as convert_file:
     convert_file.write(json.dumps(counts))

start_date, end_date= ('20220101231500', '20220101233000')
query_string= build_gdelt_query_for_counter(table_name, start_date, end_date)
text= (
            client.query(query_string)
                .result()
                .to_dataframe(
                # Optionally, explicitly request to use the BigQuery Storage API. As of
                # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
                # API is used by default.
                create_bqstorage_client=True,
                ))

x= text.copy().replace('Organizations', '', limit= 1)
x= x.dropna()
x= x['Organizations'].map(lambda x: re.sub('\s{2,}', '', x))
x=x.map(lambda x: re.split(',\d+;?', x))


with open(product['data'] + '\\org_totals.txt', 'w') as convert_file:
     convert_file.write(json.dumps({'a':'b'}))
