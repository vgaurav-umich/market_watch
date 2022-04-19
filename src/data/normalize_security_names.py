# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['fetch_securities']

# +
import ast
import pandas as pd
import numpy as np
import json
from pathlib import Path
from src.utils import preprocess_text

import nltk
nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('omw-1.4')
# -

securities_file_path = upstream['fetch_securities']['data']
securities_df = pd.read_csv(securities_file_path, index_col=0)

securities_df.drop_duplicates(subset=['cik'], inplace=True)

securities_df.info()

securities_df.former_names.replace('[]', np.nan, inplace=True)
securities_df[securities_df.former_names.isnull()]


def nomalize_names(securities_df):
    def extract_name(string):
        if len(str(string).strip()) > 5:
            string = ast.literal_eval(string)
            string = json.dumps(string)
            lst = json.loads(string)
            name = lst[0]["name"]
            return name
        else:
            return np.nan
            
    securities_df['former_name'] = securities_df.former_names.apply(extract_name)
    securities_df = securities_df.replace(to_replace=["/[A-Za-z]+/?"], value=[''], regex=True)
    securities_df = securities_df.replace(to_replace=[r'\\DE\\'], value=[''], regex=True)
    
    not_null_cond = ~securities_df['former_name'].isnull()
    
    securities_df.loc[not_null_cond,'former_name'] = preprocess_text(securities_df.loc[not_null_cond,'former_name'])
    
    securities_df['full_name'] = preprocess_text(securities_df.full_name)
    securities_df.replace('[]', np.nan, inplace=True)

    columns_to_select = ['cik','ticker','full_name', 'former_name']
    
    return securities_df[columns_to_select]


securities_df = nomalize_names(securities_df)

output_file_path = product['data']
Path(output_file_path).parent.mkdir(exist_ok=True, parents=True)
securities_df.to_csv(output_file_path)
print(f"Saved file {output_file_path}")
