# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['clean_gdelt_data', 'normalize_security_names']

# -
import pandas as pd
from collections import Counter
import json
import ast
from pathlib import Path

output_file_path = product['data']
gdelt_file_path = upstream['clean_gdelt_data']['data']
security_file_path = upstream['normalize_security_names']['data']


gdelt_df = pd.read_csv(gdelt_file_path, index_col=0)
security_df = pd.read_csv(security_file_path, index_col=0)

gdelt_df.info()

security_df.info()

# +
c = Counter()

def update_counter(string):
    if len(string.strip()) > 5:
        string = ast.literal_eval(string)
        string = json.dumps(string)
        dictionary = json.loads(string)
        c.update(dictionary)
    
gdelt_df['Organizations'].apply(update_counter)
# -

total_org_count_df = pd.DataFrame.from_dict(dict(c), orient='index', columns=['count'])

full_name_lst = security_df.full_name.to_list()
cik_lst = []
for security in total_org_count_df.index:
    if security in full_name_lst:
        matching_rows = security_df[security_df['full_name'] == security]
        # if len(matching_rows > 0):
        cik_lst.append(matching_rows['ticker'].to_list()[0])

assert len(cik_lst) == len(total_org_count_df.index)

total_org_count_df['ticker'] = cik_lst

output_file_path = product['data']
Path(output_file_path).parent.mkdir(exist_ok=True, parents=True)
total_org_count_df.to_csv(output_file_path)
print(f"Saved file {output_file_path}")


