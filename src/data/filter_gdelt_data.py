# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['get_gdlet_data']
# -


import pandas as pd
import sys

print(sys.executable)


def get_themes(v2themes):
    return v2themes.map(lambda x: [theme_tuple.split(',')[0] for theme_tuple in x.split(';')])


gdelt_codes = {}
for key in cameo_codefile_urls:
    url = cameo_codefile_urls[key]
    gdelt_codes[key] = pd.read_csv(url, sep='\t')




