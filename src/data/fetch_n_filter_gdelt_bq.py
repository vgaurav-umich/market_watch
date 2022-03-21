# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# +
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import bigquery

import pandas as pd
import os
import sys
import warnings

# import numpy as np
# import altair as alt
# from altair import datum
# alt.data_transformers.disable_max_rows()
# alt.themes.enable('fivethirtyeight')

load_dotenv("big-query.env")
warnings.filterwarnings('ignore')
# -

print(sys.executable)
api_key_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
print(api_key_file)


def get_start_date(window):
    # get today's UTC datetime
    today = datetime.utcnow()
    # round datetime to nearest 15min slot
    pd_ts = pd.Timestamp(today)
    today_ts = pd_ts.round('15T')
    # how far back we want to go?
    how_far_back = timedelta(days=window)
    start_date = today_ts - how_far_back
    
    return start_date.strftime("%Y%m%d%H%M00")


def build_gdelt_query(table_name, search_term, start_date):

    query_string = f"""
        SELECT
          GKGRECORDID,
          DATE,
          SourceCollectionIdentifier,
          DocumentIdentifier,
          V2Counts AS Counts,
          V2Themes AS Themes,
          V2Locations AS Locations,
          V2Persons AS Persons,
          V2Organizations AS Organizations,
          V2Tone AS Tone,
          GCAM,
          AllNames,
          Amounts,
          TranslationInfo,
          Extras
        FROM
          {table_name}
        WHERE
          LOWER(Organizations) LIKE "%{search_term}%"
          AND DATE > {start_date}
    """
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
file_path = product["data"]
start_date = get_start_date(rolling_window)
query = build_gdelt_query(table_name, search_term, start_date)
print(query)

client = bigquery.Client()
data_df = fetch_data(client, query)
print(f"Processed merged file with {len(data_df)} records")

# Save records
Path(file_path).parent.mkdir(exist_ok=True, parents=True)
data_df.to_csv(file_path)
print(f"Saved file {file_path}")
