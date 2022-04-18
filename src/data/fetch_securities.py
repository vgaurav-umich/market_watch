# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['fetch_all_securities_list', 'fetch_snp500_securities_list']
# -

import pandas as pd
import json
import requests
import time 
from pathlib import Path
from tqdm import tqdm
from src.utils import preprocess_text

# Get list of publicaly traded companies, ticker and associated cik ID. We will use cik ID to pull data from SEC's [DATA APIS](https://www.sec.gov/edgar/sec-api-documentation)

# +
traded_company_df = pd.read_csv(upstream["fetch_all_securities_list"]["data"], index_col=0)
snp500_company_df = pd.read_csv(upstream["fetch_snp500_securities_list"]["data"], index_col=0)

# SEC data API needs cik number to be padded by 0's to make it a length of 10 string
traded_company_df['cik'] = traded_company_df.cik_str
traded_company_df.cik_str = traded_company_df.cik_str.astype(str).str.zfill(10)
snp500_company_df['cik_str'] = snp500_company_df.CIK.astype(str).str.zfill(10)
# -

# Download dataset for publicly traded companies incl. metadata like previous name etc. See the [DEVELOPER FAQ](https://www.sec.gov/os/webmaster-faq#developers) to get more infomration about rate limit and API call requirements.

# snp_filter parameter controls if we want to fetch all master publicly traded securities or just the snp500 ones
if snp_filter == True:
    traded_companies = snp500_company_df.cik_str
else:
    # sample_pct is helpful parameter to help speed up development by selecting only a fraction of securities out of 12,227 master securities       
    traded_companies = traded_company_df.cik_str.sample(frac=sample_pct)

# Let's Fetch SEC submission data. This API has 10 request per second limit. so it will take about 1 hour to finish.preprocess_text

# +
# SEC data API requires explicit setting of User-agent as email address, otehrwise we will get 404 error
headers = {
  'User-Agent': 'vgaurav@umich.edu'
}

df_lst = []
counter = 0
#  let's reuse connection pool by using same session.
session = requests.Session()

for cik_str in tqdm(traded_companies):
    url = sec_base_url.format(cik_str = cik_str)
    # we could have used following code, but it does not work due to nested json format of payload
    # pd.read_json(url, stosample_pctptions = {'User-Agent': 'vgaurav@umich.edu'})
    response = session.get(url, headers=headers)
    data = json.loads(response.text)
    df_lst.append(pd.json_normalize(data))
    counter += 1
    # SEC API restricts 10 API request per second, so we will use counter and time to meet this restriction
    if counter % 10 == 0:
        time.sleep(1)
        
securities_df = pd.concat(df_lst)
# -

securities_df.cik = securities_df.cik.astype(int)
securities_df.info()

traded_company_df.info()

merged_df = securities_df.merge(traded_company_df, on='cik', how='inner')
# merged_df = merged_df.dropna(subset=['tickers'])
merged_df.info()

snp500_company_df.info()

# left join ensures we populate fields from snp 500 df where applicable
merged_securities_df = merged_df.merge(snp500_company_df, left_on='cik', right_on='CIK', how='left')
merged_securities_df = merged_securities_df[['cik','ticker', 'exchanges', 'ein', 'name','formerNames', 'Security', 'GICS Sector', 'GICS Sub-Industry', 'sic', 'sicDescription', 'Headquarters Location']]
merged_securities_df.columns = ['cik','ticker', 'exchanges', 'ein', 'full_name','former_names', 'short_name', 'gics_sector', 'gics_sub_industry', 'sic','sic_description', 'headquarters_location']
merged_securities_df.info()

output_file_path = product['data']
print(len(merged_securities_df))

merged_securities_df.to_csv(output_file_path)
print(f"Saved file {output_file_path}")

merged_securities_df.sample(10)

del securities_df, traded_company_df, traded_companies, snp500_company_df, merged_securities_df


