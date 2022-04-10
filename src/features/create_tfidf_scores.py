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

# %% tags=[]
# Add description here
#
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)


# %% tags=[]
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# %% tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = ['clean_gdelt_data', 'fetch_n_filter_gdelt_bq']

# This is a placeholder, leave it as None
product = None


# %% tags=["injected-parameters"]
# This cell was injected automatically based on your stated upstream dependencies (cell above) and pipeline.yaml preferences. It is temporary and will be removed when you save this notebook
path_params = {
    "sp_500_path": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\",
    "exchanges_path": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\",
}
upstream = {
    "clean_gdelt_data": {
        "nb": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\notebooks\\clean_gdelt_data.ipynb",
        "data": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\data\\interim\\gdelt_gkg_data-cleaned.pq",
    },
    "fetch_n_filter_gdelt_bq": {
        "nb": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\notebooks\\fetch_n_filter_gdelt_bq.ipynb",
        "data": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\data\\raw",
    },
}
product = {
    "nb": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\notebooks\\create_tfidf.ipynb",
    "data": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\data\\tfid_vals.csv",
}


# %% tags=[]
# your code here...

# %% tags=[]
import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm
import json
from collections import Counter
import os

warnings.simplefilter("ignore")

# %%
# os.chdir('C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\')

# %% tags=[]
# %%time
file= upstream['clean_gdelt_data']['data']
gdelt_df= pd.read_parquet(file)
gdelt_df.dropna(subset= 'Org Count', inplace=True)
gdelt_df['Org Count']= gdelt_df['Org Count'].map(lambda x : {k.lower() : int(v) for k,v in x.items() if v != None})

print(f'Filtered GDELT contains { gdelt_df.shape[0]} articles.')
gdelt_df.head(2)

# %% tags=[]
counter_file= upstream['fetch_n_filter_gdelt_bq']['data'] + '\\org_totals.txt'
f= open(counter_file)
counter= json.load(f)
f.close()
print(f'Organization Counter has {len(counter)} orgs')
{k: counter[k] for k in list(counter)[1:5]}

# %% tags=[]
path= path_params['sp_500_path']
# C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\
# sp500= pd.read_excel(path + 'sp500_list.xlsx')
exchanges_list= pd.read_csv(path)


# %% tags=[]
# %%time
def tfidf_scores(df, org_counter, sp500):
    # comp_names= list(sp500['Security'])
    # comp_names= [val.lower() for val in comp_names]
    tf_org_counts= list(df['Org Count'])
    doc_freq= Counter()
    print(len(org_counter))
    n= sum(org_counter.values())
    #== get tf ==
    print(len(tf_org_counts))
    for article in tqdm(tf_org_counts):
        if article != None:
            for entity, val in article.items():
                # if entity in comp_names:
                doc_freq.update({entity : 1})
    #====
    result= {}
    for entity, count in doc_freq.items():
        try:
            idf= np.log(n / (1 + org_counter[entity]))
            tfidf= count * idf
            result.update({entity : tfidf})      
        except:
            None
    return result
                
                
                
#     for article in tqdm(tf_org_counts):
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

tfidf= tfidf_scores(gdelt_df, counter, sp500)  

# %% tags=[]
comp_names= list(exchanges_list['Name'])
comp_names= [val.lower() for val in comp_names]
{k : v for k,v in sorted(tfidf.items(), key= lambda x: x[1], reverse=True) if k in comp_names}



# %% tags=[]

# {k : v for k,v in sorted(tfidf.items(), key= lambda x: x[1], reverse=True)}
