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

for day_num in range(query_params["rolling_window"]):
    
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
        continue;
        print(f"=> No records returnd for date {search_date}")
    print(f"=> Returned {len(search_results)} records")
    
    file_path = f'{product["data"]}/{str(search_date)}'
    Path(file_path).parent.mkdir(exist_ok=True, parents=True)
    
    search_terms = "|".join(query_params['search_terms'])
    filter_cond = search_results['V2Organizations'].str.contains(search_terms, regex=True, case=False, na=False)
    df = search_results[filter_cond]
    
    if len(df) > 0 :
        print(f"==> Found {len(df)} matchign records for {search_terms} on {search_date}")
        df[filter_cols].to_csv(file_path + '.csv')


