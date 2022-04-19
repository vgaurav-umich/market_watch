# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# +
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from google.cloud import bigquery
from tqdm import tqdm
from src import utils

import re
import collections
import pandas as pd
import datetime
import json
import os
import sys
import warnings

warnings.filterwarnings('ignore')
# -

load_dotenv(find_dotenv('market_watch.env'))

# Load Google API key. This will enable us to call GDELT's BigQuery Dataset

# debug code to see on which env our executable is running
print(sys.executable)
api_key_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
# debug code to see if we have loaded Google API key
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


def fetch_data(bqclient, query_string):
    df = (
        bqclient.query(query_string).result().to_dataframe(
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
gkg_file_path = product["data"]

start_date = utils.get_start_date(rolling_window)
start_date = utils.gdelt_date_format(start_date)
gkg_query = build_gdelt_query(table_name, search_term, start_date)

client = bigquery.Client()
data_df = fetch_data(client, gkg_query)
print(f"Processed file with {len(data_df)} records")

data_df.head()

# Save GKG records
Path(gkg_file_path).parent.mkdir(exist_ok=True, parents=True)
data_df.to_csv(gkg_file_path)
print(f"Saved file {gkg_file_path}")

del data_df


