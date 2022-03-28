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

# %%
# Add description here
#
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)


# %%
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# %% tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = ['clean_gdelt_data']

# This is a placeholder, leave it as None
product = None


# %%
# your code here...

# %%
import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm

warnings.simplefilter("ignore")

# %%
file= upstream['clean_gdelt_data']['data']
gdelt_df= pd.read_parquet(file)

gdelt_df.head(2)

# %%
gdelt_df.shape

# %%
path= path_params['sp_500_path']
# C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\sp500_list.xlsx
sp500= pd.read_excel(path + 'sp500_list.xlsx')

# %%
# def tfidf_scores(df, sp500):
#     comp_names= list(sp500['Security'])
#     org_counts= list(df['Org Count'])
#     result= []
#     doc_freq= {}
#     n= len(org_counts)
#     for article in tqdm(org_counts):
#         if article != None:
#             for entity, val in article.items():
#                 if entity in comp_names:
#                     if entity in doc_freq.keys():
#                         doc_freq.update({entity : doc_freq[entity] + 1})
#                     else:
#                         doc_freq.update({entity : 1})
#     for article in tqdm(org_counts):
#         tf= {}
#         article_list= []
#         if article != None:
#             for entity, val in article.items():
#                 if entity in comp_names:
#                     tf.update({entity : val})
#                     tfidf= None
#                     if tf[entity] != None:
#                         tfidf= tf[entity] * np.log((n) / (1 + doc_freq[entity]))
#                     article_list.append([entity, tfidf])
            
#             result= result.append(article_list)
#         else:
#             result= result.append([])
            
#     return pd.DataFrame(result, columns=['Company','TFIDF'])

# tfidf= tfidf_scores(gdelt_df.iloc[:100], sp500)  
# tfidf.shape

# %%

# %%
