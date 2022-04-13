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
upstream = ['clean_gdelt_data', 'fetch_n_filter_gdelt_bq']

# This is a placeholder, leave it as None
product = None


# %%
# your code here...

# %%
import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm
import json
from collections import Counter
import os
from src import utils

warnings.simplefilter("ignore")

# %%
# os.chdir('C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\')

# %%
# %%time
file= upstream['clean_gdelt_data']['data']
gdelt_df= pd.read_parquet(file)
gdelt_df.dropna(subset= 'Org Count', inplace=True)
gdelt_df['Org Count']= gdelt_df['Org Count'].map(lambda x : {k.lower() : int(v) for k,v in x.items() if v != None})

print(f'Filtered GDELT contains { gdelt_df.shape[0]} articles.')
gdelt_df.head(2)

# %%
counter_file= upstream['fetch_n_filter_gdelt_bq']['data'] + '\\org_totals.txt'
f= open(counter_file)
counter= json.load(f)
f.close()
print(f'Organization Counter has {len(counter)} orgs')
{k: counter[k] for k in list(counter)[1:5]}

# %%
path= path_params['exchanges_path']
if not os.path.exists(path):
    os.chdir('..')
    os.chdir('..')
# C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\
# sp500= pd.read_excel(path + 'sp500_list.xlsx')
exchanges_dict= utils.get_rel_company_names(path)
exchanges_list= list(exchanges_dict.keys())


# %%
# %%time
def tfidf_scores(df, org_counter):
    # comp_names= list(sp500['Security'])
    # comp_names= [val.lower() for val in comp_names]
    tf_org_counts= list(df['Org Count'])
    doc_freq= Counter()
    print(f'Number of unique from all articles: {len(org_counter)}')
    n= sum(org_counter.values())
    #== get tf ==
    print(f'Number of unique orgs from articles mentioning Tesla: {len(tf_org_counts)}')
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

tfidf= tfidf_scores(gdelt_df, counter)  


# %%
def normalize_company_names(path):
    '''
    Helper function that takes the stock names and adds variations to the name. 
    Example: American Airlines Group Inc. becomes a dict that includes: 
    American, American Airlines, American Airlines Group, & American Airlines Group Incorporated.
    '''
    rel_company = pd.read_csv(path)
    rel_company = rel_company[rel_company['Market Cap'] >= 100000000]
    ticker= rel_company['Symbol']
    rel_company = rel_company['Name']
    rel_company = rel_company.str.lower()
    rel_company = rel_company[rel_company.str.contains('common|ordinary', regex=True)]
    rel_company = rel_company.str.split('(corp|ltd|inc|corporation|limited|incorporation|incorporated)',regex=True)
    rel_company = rel_company.map(lambda x: ''.join(x[:2]))
    expand_rel_company = {}
    # stop_words= ['unit', 'common', 'class', 'warrants', 'warrant', 'depository']
    for i, company in rel_company.iteritems():
        expand_rel_company.update({company : ticker[i]})
    return expand_rel_company
ticker_dict= normalize_company_names(path)

# %%

# %% jupyter={"outputs_hidden": true}
comp_names= exchanges_list
comp_names= [val.lower() for val in comp_names]
result_names= {k : v for k,v in sorted(tfidf.items(), key= lambda x: x[1], reverse=True) if k in comp_names}
result_ticker= {ticker_dict[k] : v for k,v in sorted(tfidf.items(), key= lambda x: x[1], reverse=True) if k in comp_names}


result_names

# %%
output_path= product['data']
with open(output_path,'w') as op:
    json.dump(result_ticker, op)

# %%
