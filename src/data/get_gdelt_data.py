# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None
# -


import datetime
from pathlib import Path
import shutil
import gdelt
import pandas as pd
import sys
print(sys.executable)


def get_themes(v2themes):
    return v2themes.map(lambda x: [theme_tuple.split(',')[0] for theme_tuple in x.split(';')])


gdelt_codes = {}
for key in cameo_codefile_urls:
    url = cameo_codefile_urls[key]
    gdelt_codes[key] = pd.read_csv(url, sep='\t')

# +
# gdelt_codes['cameo_event_codes'][gdelt_codes['cameo_event_codes'].CAMEOEVENTCODE == 100]
# -

gd = gdelt.gdelt(version=2)

for day_num in range(query_params["rolling_window"]):
    
    today = datetime.date.today()
    how_far_back = datetime.timedelta(days=day_num)
    search_date = today - how_far_back
    
    search_results = gd.Search(
        str(search_date), 
        table='gkg'
    )
    file_path = f'{product["data"]}/{str(search_date)}.parquet'
    Path(file_path).parent.mkdir(exist_ok=True, parents=True)

    search_results.to_parquet(file_path)

