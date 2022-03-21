# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# +
import datetime
from pathlib import Path
import gdelt
import pandas as pd
import os

import sys

import warnings

warnings.filterwarnings('ignore')
# -

print(sys.executable)

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
rolling_window = query_params["rolling_window"]
gdelt_gkg_base_url = query_params["gdelt_gkg_base_url"]
# -

gd = gdelt.gdelt(version=2)

# +
from datetime import datetime, timedelta


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


def get_urls(rolling_window, base_url):
    today = datetime.utcnow()

    pd_ts = pd.Timestamp(today)
    today_ts = pd_ts.round('15T')

    how_far_back = timedelta(days=rolling_window)
    start_date = today_ts - how_far_back
    urls = [
        base_url + f'{dt.strftime("%Y%m%d%H%M00")}.gkg.csv.zip'
        for dt in
        datetime_range(
            start_date,
            today_ts,
            timedelta(minutes=15)
        )
    ]
    return urls


# -

urls = get_urls(
    rolling_window=rolling_window,
    base_url='http://data.gdeltproject.org/gdeltv2/'
)

# +
#  config parameter that tells us rolling window for getting data 
# i.e. a value of 30 in env.yaml means 30 days rolling window. We will fetch data from today to 30 days back.
rolling_window = query_params["rolling_window"]

# fetch data for each day and then filter it on search terms
for url in urls:
    file_name = os.path.basename(url).split(".")[0]
    search_results = pd.read_csv(url, sep='\t', names=col_names, on_bad_lines='skip', encoding_errors='ignore')
    # Add search date to file name     
    file_path = f'{product["data"]}/{str(file_name)}'
    Path(file_path).parent.mkdir(exist_ok=True, parents=True)

    # Form a query friendly search terms     
    search_terms = "|".join(query_params['search_terms'])
    organizations = search_results['V2Organizations'].str.lower()
    filter_cond = organizations.str.contains(search_terms, regex=True, case=False, na=False)
    df = search_results[filter_cond]

    if len(df) > 0:
        print(f"==> Found {len(df)} matchign records out of {len(search_results)} for {search_terms} on {file_name}")
        df[filter_cols].to_csv(file_path + '.csv')

# +
# #  config parameter that tells us rolling window for getting data 
# # i.e. a value of 30 in env.yaml means 30 days rolling window. We will fetch data from today to 30 days back.
# rolling_window = query_params["rolling_window"]

# # fetch data for each day and then filter it on search terms
# for day_num in range(rolling_window):

#     today = datetime.today()
#     how_far_back = timedelta(days=day_num)
#     search_date = today - how_far_back

#     print(f"Searching for {search_date}")
#     # try:
#     search_results = gd.Search(
#         str(search_date),
#         table='gkg', 
#         coverage=False,
#         translation=True
#     )
#     # except:
#     #     print(f"=> No records returnd for date {search_date}")
#     #     continue;
#     print(search_results.columns)
#     break
#     print(f"=> Returned {len(search_results)} records")

#     # Add search date to file name     
#     file_path = f'{product["data"]}/{str(search_date)}'
#     Path(file_path).parent.mkdir(exist_ok=True, parents=True)

#     # Form a query friendly search terms     
#     search_terms = "|".join(query_params['search_terms'])
#     organizations = search_results['V2Organizations'].str.lower()
#     filter_cond = organizations.str.contains(search_terms, regex=True, case=False, na=False)
#     df = search_results[filter_cond]

#     if len(df) > 0 :
#         print(f"==> Found {len(df)} matchign records for {search_terms} on {search_date}")
#         df[filter_cols].to_csv(file_path + '.csv')
# -
