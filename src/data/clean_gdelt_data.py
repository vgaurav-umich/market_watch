# + tags=["parameters"]
# your parameters here...
upstream = ['fetch_n_filter_gdelt_bq', 'normalize_security_names']
# -

# your code here...
import pandas as pd
import numpy as np
from pathlib import Path
import nltk
import time
import re
from time import time
import json
import ast
from src.utils import preprocess_text
from collections import Counter
from src.utils import fuzz_similarity, preprocess_text
from nltk.corpus import stopwords

nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('omw-1.4')
stops = set(stopwords.words('english'))
pd.options.display.max_colwidth = 200

gdelt_file_path = upstream['fetch_n_filter_gdelt_bq']['data']
securities_file_path = upstream['normalize_security_names']['data']
gdelt_df = pd.read_csv(gdelt_file_path, index_col=0)
securities_df = pd.read_csv(securities_file_path, index_col=0)
securities_df.dropna(subset=['former_name'], inplace=True)
securities_df.info()

gdelt_df.columns

securities_df.head()

#  drop rows where no organizations names were extracted
gdelt_df = gdelt_df.dropna(subset=['Organizations'])
gdelt_df = gdelt_df.replace(to_replace= ['Google', 'Facebook', 'YouTube', 'Youtube'], value=['alphabet', 'meta platforms', 'alphabet', 'alphabet'], regex=True)

#  get rid of numeric position of org name mention by only extracting alpha names
# Extract only names not the index
gdelt_df.Organizations = gdelt_df.Organizations.map(lambda x: re.split(r',\d+;?', x))


# +
def preprocess_orgs(x):
    return preprocess_text(x, eng=True)

gdelt_df.Organizations = gdelt_df.Organizations.apply(preprocess_orgs)
# -

gdelt_df.Organizations = gdelt_df.Organizations.replace(to_replace=securities_df.former_name.to_list(), value=securities_df.full_name.to_list())

gdelt_df.info()


# +
def count_orgs(x):
    return x if type(x) is float or x is None or len(x) == 0 else Counter(x)

gdelt_df.Organizations = gdelt_df.Organizations.apply(count_orgs)
gdelt_df.dropna(subset=['Organizations'], inplace=True)
gdelt_df.info()
# -
securities_names = preprocess_text(securities_df.full_name)
gdelt_df.Organizations = gdelt_df.Organizations.apply(lambda x: { key:x[key] for key in x.keys() if key in securities_names and key != 'Tooshorttext'})
gdelt_df = gdelt_df[gdelt_df.Organizations.str.len() != 0]

gdelt_df.info()


def split_locations(location_list):
    location_names = []
    if type(location_list) is not float:
        for location_string in location_list:
            loc_parts = location_string.split('#')
            # We are only interested in full location name which is second entry in location string           
            location_names.append(loc_parts[1]) if len(loc_parts) > 1 else np.nan
            
    return location_names
# Locations are semi-colons(;) seperated and each location string is further seperated by hash(#)
gdelt_df.Locations = gdelt_df.Locations.str.split(';').apply(split_locations)
#  Remove duplicates
gdelt_df.Locations = gdelt_df.Locations.map(set)


#  Clean some data elements
gdelt_df.Persons = gdelt_df.Persons.str.findall(pat="[A-Z][a-z]+ [A-Z][a-z]+")
#  Remove duplicates
gdelt_df.Persons = gdelt_df.Persons.map(set, na_action='ignore')

gdelt_df.Tone = gdelt_df.Tone.str.split(',')

# Clean Tone
gdelt_df['AvgTone'] = gdelt_df.Tone.apply(lambda x: x[0])
gdelt_df['PosScore'] = gdelt_df.Tone.apply(lambda x: x[1])
gdelt_df['NegScore'] = gdelt_df.Tone.apply(lambda x: x[2])
gdelt_df['Polarity'] = gdelt_df.Tone.apply(lambda x: x[3])

gdelt_df.drop(["Tone", "DATE", "SourceCollectionIdentifier", "DocumentIdentifier"], axis = 1, inplace=True)


gdelt_df.sample(10)

output_file_path = product['data']
Path(output_file_path).parent.mkdir(exist_ok=True, parents=True)
gdelt_df.to_csv(output_file_path)
print(f"Saved file {output_file_path}")

del gdelt_df, securities_df


