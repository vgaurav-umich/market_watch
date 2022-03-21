# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None


# +
import datetime
from pathlib import Path
import gdelt
import pandas as pd

import sys

import warnings
warnings.filterwarnings('ignore')
# -

print(sys.executable)

gd = gdelt.gdelt(version=2)

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

# +
#  config parameter that tells us rolling window for getting data 
# i.e. a value of 30 in env.yaml means 30 days rolling window. We will fetch data from today to 30 days back.
rolling_window = query_params["rolling_window"]

# fetch data for each day and then filter it on search terms
for day_num in range(rolling_window):
    
    today = datetime.date.today()
    how_far_back = datetime.timedelta(days=day_num)
    search_date = today - how_far_back
    
    print(f"Searching for {search_date}")
    try:
        search_results = gd.Search(
            str(search_date),
            table='gkg'
        )
    except:
        print(f"=> No records returnd for date {search_date}")
        continue;

    print(f"=> Returned {len(search_results)} records")
    
    # Add search date to file name     
    file_path = f'{product["data"]}/{str(search_date)}'
    Path(file_path).parent.mkdir(exist_ok=True, parents=True)
    
    # Form a query friendly search terms     
    search_terms = "|".join(query_params['search_terms'])
    filter_cond = search_results['V2Organizations'].str.contains(search_terms, regex=True, case=False, na=False)
    df = search_results[filter_cond]
    
    if len(df) > 0 :
        print(f"==> Found {len(df)} matchign records for {search_terms} on {search_date}")
        df[filter_cols].to_csv(file_path + '.csv')
# -


