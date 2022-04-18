from datetime import datetime, timedelta
import pandas as pd


# +
def gdelt_date_format(utc_date):
    
    return utc_date.strftime("%Y%m%d%H%M00")

def general_date_format(utc_date):
    
    return utc_date.strftime("%Y-%m-%d")

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def get_start_date(window):
    # get today's UTC datetime
    today = datetime.utcnow()
    # round datetime to nearest 15min slot
    pd_ts = pd.Timestamp(today)
    today_ts = pd_ts.round('15T')
    # how far back we want to go?
    how_far_back = timedelta(days=window)
    start_date = today_ts - how_far_back
    
    return start_date

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


def filter_org_col(org_cell, rel_comp_dict, rel_company_names, threshold):
    result_dict = {}
    if org_cell != None:
        try:
            org_cell.split(';')
        except:
            print(org_cell)
        for item in org_cell.split(';'):
            word = item.split(',')[0]
            # rel_company_names = [val for list_ in rel_comp_dict.values() for val in list_]
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



# +
# Importing the necessary libraries
import unicodedata
import re
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import nltk

# List of keywords to help identify stop_words
vendor_stopwords=['biz', 'bv', 'company', 'comp', 'charity', 'company',
                'corporation','corp','co', 'dba', 'market', 'holdings', 'holding',
                'incorporated', 'incorporation','incorporate', 'japan', 
                'incorporat', 'incorp', 'inc', 'museum', 'trust', 'center',
                'intl', 'intnl', 'nicholas', 'club', 'science', 
                'limited' ,'llc', 'ltd', 'llp', 'institute', 'federal', 'reserve', 
                'pvt', 'pte', 'private', 'unknown', 'plc', 'group', 
                'industries', 'investment', 'fund', 'motors', 'motor', 'battery',
                'department of justice','department','industry','electronics','association','systems',
                'technologies' ,'batteries', 'energy', 'india', 'china', 'indian',
                'chinese', 'europe', 'securities end exchange commission', 'securities and exchange commission',
                'federal bureau', 'investigation', 'united states', 'shanghai', 'alliance'
                'california','austin','texas', 'russian', 'ministry', 'administration', 'white house',
                'internal revenue service', 'european union', 'giga', 'division', 'university', 
                'party','exchange commission','board', 'supervisory', 'justice', 'council', 'buraeu',
                 'democrate', 'senate', 'committiee', 'generation', 'modi']

# Text data encoder function
def filter_ascii(text):
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')

# Remove spl characters & digits (optional) function
def remove_special_characters(text, remove_digits=False):
    pattern = r'[^a-zA-z0-9\s]' if not remove_digits else r'[^a-zA-z\s]'
    text = re.sub(pattern, '', text)
    return text

# Remove vendor specific stop words
def clean_stopwords(text,eng=False):
    if eng == False:
        custom = vendor_stopwords
    else:
        custom = vendor_stopwords + list(ENGLISH_STOP_WORDS)
    for x in custom:
        pattern2 = r'\b'+x+r'\b'
        text=re.sub(pattern2,'',text)
    return text

# Trim the text to remove spaces
def clean_spaces(text):
    text=text.replace('  ', ' ')
    text=text.strip()
    if len(text) < 1:
        text='Tooshorttext'
    return text

# Function to Preprocess Textual data. Provide input as df['Column Name'] to this function
def preprocess_text(column, remove_digits=True, lemm=True, eng=False):
    try:
        column = [filter_ascii(text) for text in column]
        column = [remove_special_characters(text, remove_digits) for text in column]
        column = [text.lower() for text in column]
        column = [clean_stopwords(text, eng) for text in column]
        column = [clean_spaces(text) for text in column]
        ## Lemmatisation (convert the word into root word)
        if lemm == True:
            lem = nltk.stem.wordnet.WordNetLemmatizer()
            column = [lem.lemmatize(text) for text in column]
        return column
    except Exception as e:
        return print(e)


# -

# %pip install fuzzywuzzy
# %pip install python-Levenshtein

# Function to merge dataframes
def df_merger(df_left, df_right, how, on):
    try:
        final=df_left.merge(df_right, how=how, on=on)
    except Exception as e:
        return print(e)  
    return final


# +
from fuzzywuzzy import fuzz
import numpy as np

# Function to generate similarity matrix. Provide input as df['Column Name'] to this function
def fuzz_similarity(column):
    similarity_array = np.ones((len(column), (len(column))))*100
    for i in range(1, len(column)):
        for j in range(i):
            s1 = fuzz.token_set_ratio(column[i],column[j]) + 0.00000000001
            s2 = fuzz.partial_ratio(column[i],column[j]) + 0.00000000001
            similarity_array[i][j] = 2*s1*s2 / (s1+s2)

    for i in range(len(column)):
        for j in range(i+1,len(column)):
            similarity_array[i][j] = similarity_array[j][i]
            np.fill_diagonal(similarity_array, 100)
    return similarity_array


# +

# Importing the necessary libraries
from sklearn.cluster import AffinityPropagation
import pandas as pd

def company_clusters(dataframe, matrix):
    cust_ids = dataframe['Supplier Code'].to_list()
    clusters = AffinityPropagation(affinity='precomputed').fit_predict(matrix)
    df_clusters = pd.DataFrame(list(zip(cust_ids, clusters)), columns=['Supplier Code','Cluster'])
    new = df_merger(dataframe, df_clusters, 'inner', 'Supplier Code')
    return new
# -


