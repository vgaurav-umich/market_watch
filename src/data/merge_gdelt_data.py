# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['fetch_gdelt_data']
# -


import pandas as pd
from pathlib import Path
import os
import sys

print(sys.executable)

# +
# def get_themes(v2themes):
#     return v2themes.map(lambda x: [theme_tuple.split(',')[0] for theme_tuple in x.split(';')])

# +
# gdelt_codes = {}
# for key in cameo_codefile_urls:
#     url = cameo_codefile_urls[key]
#     gdelt_codes[key] = pd.read_csv(url, sep='\t')

# +
# Read filenames from the given path
data_dir = upstream['fetch_gdelt_data']['data']
data_files = os.listdir(data_dir)

def load_files(data_dir, filenames):
    for filename in filenames:
        file_path = os.path.join(data_dir, filename)
        print(file_path)
        yield pd.read_csv(file_path)
		

data = pd.concat(load_files(data_dir, data_files))

# +
output_file = product['data']

data.to_csv(output_file)
# -



