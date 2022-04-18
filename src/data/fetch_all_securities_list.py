# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None
# -

import pandas as pd
import json
import requests
import time 
from pathlib import Path

# Get list of publicaly traded companies, ticker and associated cik ID. We will use cik ID to pull data from SEC's [DATA APIS](https://www.sec.gov/edgar/sec-api-documentation)

# +
counter = 0
while True:
    try:
        traded_company_df = pd.read_json(
            sec_ticker_url, 
            orient='index', 
            encoding_errors='ignore', 
            storage_options = {'User-Agent': 'vgaurav@umich.edu'}
        )
        break
    except:
        print(f"Error in file load. Proceeding retry after 10 second for {retry_count} times.")
        counter += 1
        if counter == retry_count:
            break
        time.sleep(10)
        
# traded_company_df['cik'] = traded_company_df.cik_str
# traded_company_df.cik_str = traded_company_df.cik_str.astype(str).str.zfill(10)
# -
traded_company_df.drop_duplicates(subset=['cik_str'], inplace=True)

traded_company_df.info()

traded_company_df.sample(5)

file_path = product["data"]
Path(file_path).parent.mkdir(exist_ok=True, parents=True)
traded_company_df.to_csv(file_path)
print(f"Saved file {file_path}")

del traded_company_df


