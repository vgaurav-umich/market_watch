# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# +
from pathlib import Path
from urllib.error import HTTPError, URLError
from datetime import datetime, timedelta

import pandas as pd
import os
import sys
import warnings

# import numpy as np
# import altair as alt
# from altair import datum
# alt.data_transformers.disable_max_rows()
# alt.themes.enable('fivethirtyeight')

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


# +
col_names = ['GKGRECORDID', 'DATE', 'SourceCollectionIdentifier', 'SourceCommonName',
             'DocumentIdentifier', 'Counts', 'V2Counts', 'Themes', 'V2Themes',
             'Locations', 'V2Locations', 'Persons', 'V2Persons', 'Organizations',
             'V2Organizations', 'V2Tone', 'Dates', 'GCAM', 'SharingImage',
             'RelatedImages', 'SocialImageEmbeds', 'SocialVideoEmbeds', 'Quotations',
             'AllNames', 'Amounts', 'TranslationInfo', 'Extras']

filter_cols = [
    'GKGRECORDID',
    'DATE',
    'SourceCollectionIdentifier',
    'SourceCommonName',
    'DocumentIdentifier',
    'V2Counts',
    'V2Themes',
    'V2Locations',
    'V2Persons',
    'V2Organizations',
    'V2Tone',
    'GCAM',
    'SharingImage',
    'RelatedImages',
    'SocialImageEmbeds',
    'SocialVideoEmbeds',
    'Quotations',
    'AllNames',
    'Amounts',
    'TranslationInfo',
    'Extras'
]
#  config parameter that tells us rolling window for getting data 
# i.e. a value of 30 in env.yaml means 30 days rolling window. We will fetch data from today to 30 days back.
rolling_window = query_params["rolling_window"]
gdelt_gkg_base_url = query_params["gdelt_gkg_base_url"]
# -

gd = gdelt.gdelt(version=2)


# +


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


def get_gkg_file_url(rolling_window, base_url):
    today = datetime.utcnow()

    pd_ts = pd.Timestamp(today)
    today_ts = pd_ts.round('15T')

    how_far_back = timedelta(days=rolling_window)
    start_date = today_ts - how_far_back
    for dt in datetime_range(start_date, today_ts, timedelta(minutes=15)):
        yield base_url + f'{dt.strftime("%Y%m%d%H%M00")}.gkg.csv.zip'


# -

def read_and_filter_files():
    data = []

    for url in get_gkg_file_url(rolling_window, base_url=gdelt_gkg_base_url):

        file_name = os.path.basename(url).split(".")[0]

        try:
            search_results = pd.read_csv(url, sep='\t', names=col_names, on_bad_lines='skip', encoding_errors='ignore')
        except HTTPError as http_err:
            print(f"Unable to fetch {url}. Encountered {http_err.code}")
            pass
        except URLError as url_err:
            print(f"Unable to fetch {url}. Encountered {url_err.code}")
            pass
        except:
            print(f"Unable to fetch {url}. Some Generic Error.")
            pass

        # Add search date to file name     
        file_path = product["data"]

        Path(file_path).parent.mkdir(exist_ok=True, parents=True)
        # Form a query friendly search terms     
        search_terms = "|".join(query_params['search_terms'])
        organizations = search_results['V2Organizations'].str.lower()
        filter_cond = organizations.str.contains(search_terms, regex=True, case=False, na=False)
        df = search_results[filter_cond]

        if len(df) > 0:
            data.append(df[filter_cols])

    data_df = pd.concat(data)
    data_df.to_csv(file_path)
    print(f"Processed merged file with {len(data_df)} records")


read_and_filter_files()
