# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = ['clean_gdelt_data', 'total_org_count']

# This is a placeholder, leave it as None
product = None


# %%
import pandas as pd
import numpy as np
import warnings
import json
import ast
from pathlib import Path

warnings.simplefilter("ignore")

# %%
gdelt_file_path = upstream['clean_gdelt_data']['data']
total_count_path = upstream['total_org_count']['data']

# %%
gdelt_df = pd.read_csv(gdelt_file_path, index_col=0)
total_org_count_df = pd.read_csv(total_count_path, index_col=0)

# %%
gdelt_df.info()

# %%
total_org_count_df.info()


# %%

# %%
def convert_to_dict(string):
    string = ast.literal_eval(string)
    string = json.dumps(string)
    dictionary = json.loads(string)
    return pd.Series(dictionary)

tf_df = gdelt_df['Organizations'].apply(convert_to_dict)
ticker_name_dict = {values['index']: values['ticker'] for row_num, values in total_org_count_df.reset_index().iterrows()}
tf_df.rename(columns=ticker_name_dict, inplace = True)

# %%
tf_df = tf_df.div(tf_df.sum(axis=1), axis=0)
tf_df

# %%
idf = np.log(len(tf_df) / tf_df.count(axis=0))
idf

# %%
tf_idf_df = tf_df * idf
tf_idf_df = tf_idf_df.fillna(0)
tf_idf_df

# %%
output_file_path = product['data']
Path(output_file_path).parent.mkdir(exist_ok=True, parents=True)
tf_idf_df.to_csv(output_file_path)
print(f"Saved file {output_file_path}")

# %%
del tf_df, tf_idf_df

# %%
