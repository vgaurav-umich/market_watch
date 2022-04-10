# + tags=["parameters"]
# your parameters here...
upstream = ['fetch_n_filter_gdelt_bq']

# + tags=["injected-parameters"]
# This cell was injected automatically based on your stated upstream dependencies (cell above) and pipeline.yaml preferences. It is temporary and will be removed when you save this notebook
path_params = {
    "sp_500_path": "data/external/",
    "exchanges_path": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\nasdaq_nyse_amex.csv",
}
upstream = {
    "fetch_n_filter_gdelt_bq": {
        "nb": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\notebooks\\fetch_n_filter_gdelt_bq.ipynb",
        "data": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\data\\raw",
    }
}
product = {
    "nb": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\notebooks\\clean_gdelt_data.ipynb",
    "data": "C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\output\\data\\interim\\gdelt_gkg_data-cleaned.pq",
}


# + tags=[]
# your code here...
import pandas as pd
import numpy as np
from pathlib import Path

pd.options.display.max_colwidth = 200

# + tags=[]
input_file_path = upstream['fetch_n_filter_gdelt_bq']['data'] + '\\gdelt_gkg_bqdata-raw.csv'

data_df = pd.read_csv(input_file_path, index_col=0)
print(data_df.columns)


# + tags=[]
def split_locations(location_list):
    location_names = []
    if type(location_list) is not float:
        for location_string in location_list:
            loc_parts = location_string.split('#')
            location_names.append(loc_parts[1]) if len(loc_parts) > 1 else np.nan
            
    return location_names

data_df.Locations = data_df.Locations.str.split(';').apply(split_locations)


# + tags=[]
#  Clean some data elements
data_df.Persons = data_df.Persons.str.findall(pat="[A-Z][a-z]+ [A-Z][a-z]+")

# + tags=[]
# data_df.Organizations = data_df.Organizations.str.replace("Tesla|Tesla Motors|Tesla Inc", '').str.findall(pat="[A-Z][a-z]+ [A-Z][a-z]+")

# + tags=[]
data_df.Tone = data_df.Tone.str.split(',')

# + tags=[]
# Clean Tone
data_df['AvgTone'] = data_df.Tone.apply(lambda x: x[0])
data_df['PosScore'] = data_df.Tone.apply(lambda x: x[1])
data_df['NegScore'] = data_df.Tone.apply(lambda x: x[2])
data_df['Polarity'] = data_df.Tone.apply(lambda x: x[3])

# + tags=[]
threshold = 1
# C:\\Users\\mattd\\OneDrive\\Masters\\SIADS-697 Capstone Project III\\market_watch2\\data\\external\\
path = path_params['exchanges_path']


def get_rel_company_names(path):
    rel_company = pd.read_csv(path)
    rel_company = rel_company['Name']
    rel_company = rel_company.str.lower()
    rel_company = rel_company[rel_company.str.contains('common|ordinary', regex=True)]
    rel_company = rel_company.str.split('(corp|ltd|inc|corporation|limited|incorporation|incorporated)',regex=True)
    rel_company = rel_company.map(lambda x: ''.join(x[:2]))
    expand_rel_company = {}
    # stop_words= ['unit', 'common', 'class', 'warrants', 'warrant', 'depository']
    for company in rel_company:
        # company = company.lower()
        company_name_list = []
        company_name_list.append(company)
        if 'inc' in company_name_list[0]:
            company_name_list.append(company_name_list[0] + 'orporation')
            company_name_list.append(company_name_list[0] + 'orporated')
        elif 'corp' in company_name_list[0]:
            company_name_list.append(company_name_list[0] + 'oration')
        elif 'ltd' in company_name_list[0]:
            w_name= company_name_list[0].split('ltd')[0]
            company_name_list.append(w_name + 'limited ')
            company_name_list.append(w_name + 'limited company')
        elif 'corporation' in company_name_list[0]:
            w_name= company_name_list[0].split('corporation')[0]
            company_name_list.append(w_name + 'corp')
        words = company.split(' ')
        for n in range(1, len(words)):
            if words[0:n] not in company_name_list:
                company_name_list.append(' '.join(words[0:n]))
        expand_rel_company.update({company: company_name_list})
    return expand_rel_company


def filter_org_col(org_cell, rel_comp_dict, threshold):
    result_dict = {}
    if org_cell != None:
        try:
            org_cell.split(';')
        except:
            print(org_cell)
        for item in org_cell.split(';'):
            word = item.split(',')[0]
            rel_company_names = [val for list_ in rel_comp_dict.values() for val in list_]
            if word in rel_company_names:
                for key, names_list in rel_comp_dict.items():
                    if word in names_list:
                        if key in result_dict.keys():
                            result_dict.update({key: result_dict[key] + 1})
                        else:
                            result_dict.update({key : 1})
            else:
                if word in result_dict.keys():
                    result_dict.update({word : result_dict[word] +1})
                else:
                    result_dict.update({word : 1})
    result_dict = {key: val for key, val in result_dict.items() if val >= threshold}
    return result_dict


def extract_company_articles(df, path, threshold):
    '''
    Ectract all article that contain company_name from csv files.
    Parameters:
        Company_name= main company to filter for
        path= path to GDELT csv's
        path2= path to list to SP500 csv
        threshold= minimum count for related company to be included
    '''
    rel_company_dict = get_rel_company_names(path)
    df['Org Count'] = df['Organizations'].map(lambda x: filter_org_col(x, rel_company_dict, threshold), na_action='ignore')

    return df


# + tags=[]
# %%time
data_df = extract_company_articles(data_df, path, threshold)

# + tags=[]
data_df

# + tags=[]
data_df.drop(["Tone", "DATE", "SourceCollectionIdentifier", "DocumentIdentifier"], axis = 1, inplace=True)


# + tags=[]
output_file_path = product['data']
# Path(output_file_path).parent.mkdir(exist_ok=True, parents=True)
# data_df.to_csv(output_file_path)
# print(f"Saved file {output_file_path}")

# + tags=[]


# + tags=[]

data_df.to_parquet(output_file_path)
# + tags=[]
data_df.loc[0,'Org Count']

