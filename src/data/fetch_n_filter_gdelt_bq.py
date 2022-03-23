# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# +
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from google.cloud import bigquery

import os
import sys
import warnings
from src import utils

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
start_date = utils.get_start_date(rolling_window)
start_date = utils.gdelt_date_format(start_date)
query = build_gdelt_query(table_name, search_term, start_date)
print(query)

client = bigquery.Client()
data_df = fetch_data(client, query)
print(f"Processed merged file with {len(data_df)} records")

# Save records
Path(file_path).parent.mkdir(exist_ok=True, parents=True)
data_df.to_csv(file_path)
print(f"Saved file {file_path}")
