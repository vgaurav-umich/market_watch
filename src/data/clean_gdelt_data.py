# + tags=["parameters"]
# your parameters here...
upstream = ['fetch_n_filter_gdelt_bq']

# +
# your code here...
import pandas as pd
import numpy as np
from pathlib import Path

pd.options.display.max_colwidth = 200

# +
input_file_path = upstream['fetch_n_filter_gdelt_bq']['data']

data_df = pd.read_csv(input_file_path, index_col=0)
print(data_df.columns)


# +
def split_locations(location_list):
    location_names = []
    if type(location_list) is not float:
        for location_string in location_list:
            loc_parts = location_string.split('#')
            location_names.append(loc_parts[1]) if len(loc_parts) > 1 else np.nan
            
    return location_names

data_df.Locations = data_df.Locations.str.split(';').apply(split_locations)
# -


#  Clean some data elements
data_df.Persons = data_df.Persons.str.findall(pat="[A-Z][a-z]+ [A-Z][a-z]+")

# +
# data_df.Organizations = data_df.Organizations.str.replace("Tesla|Tesla Motors|Tesla Inc", '').str.findall(pat="[A-Z][a-z]+ [A-Z][a-z]+")
# -

data_df.Tone = data_df.Tone.str.split(',')

# Clean Tone
data_df['AvgTone'] = data_df.Tone.apply(lambda x: x[0])
data_df['PosScore'] = data_df.Tone.apply(lambda x: x[1])
data_df['NegScore'] = data_df.Tone.apply(lambda x: x[2])
data_df['Polarity'] = data_df.Tone.apply(lambda x: x[3])

data_df.drop(["Tone","SourceCollectionIdentifier", "DocumentIdentifier"], axis = 1, inplace=True)

# +
threshold = 1
path = 'sp500_list.xlsx'


def get_rel_company_names(path):
    rel_company = pd.read_excel(path)
    rel_company = rel_company['Security']
    expand_rel_company = {}
    for company in rel_company:
        company = company.lower()
        company_name_list = []
        company_name_list.append(company)
        for postfix in ['inc', 'inc.', 'incorporation', 'corp.', 'corp', 'corporation']:
            if postfix in company_name_list[0]:
                break
        for postfix in ['inc', 'incorporation', 'corp', 'corporation']:
            company_name_list.append(company_name_list[0] + ' ' + postfix)
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
    df['related_companies'] = df['Organizations'].map(lambda x: filter_org_col(x, rel_company_dict, threshold), na_action='ignore')

    return df


# -

# %%time
data_df = extract_company_articles(data_df, path, threshold)

# +
print(data_df.loc[4155,'Organizations'])

print('=')
print(data_df.loc[4155,'related_companies'])
# -



output_file_path = product['data']
Path(output_file_path).parent.mkdir(exist_ok=True, parents=True)
data_df.to_csv(output_file_path)
print(f"Saved file {output_file_path}")


