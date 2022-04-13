from datetime import datetime, timedelta
import pandas as pd
import os


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
    '''
    Helper function that takes the stock names and adds variations to the name. 
    Example: American Airlines Group Inc. becomes a dict that includes: 
    American, American Airlines, American Airlines Group, & American Airlines Group Incorporated.
    '''
    rel_company = pd.read_csv(path)
    rel_company = rel_company[rel_company['Market Cap'] >= 100000000]
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
        expand_rel_company.update({ticker[i]: company_name_list})
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

