# + tags=["parameters"]
# your parameters here...
upstream = ['fetch_n_filter_gdelt_bq']
# -

# your code here...
import pandas as pd
import numpy as np
from pathlib import Path

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

data_df.Organizations = data_df.Organizations.str.replace("Tesla|Tesla Motors|Tesla Inc", '').str.findall(pat="[A-Z][a-z]+ [A-Z][a-z]+")

data_df.Tone = data_df.Tone.str.split(',')

# Clean Tone
data_df['AvgTone'] = data_df.Tone.apply(lambda x: x[0])
data_df['PosScore'] = data_df.Tone.apply(lambda x: x[1])
data_df['NegScore'] = data_df.Tone.apply(lambda x: x[2])
data_df['Polarity'] = data_df.Tone.apply(lambda x: x[3])

data_df.drop(["Tone", "DATE", "SourceCollectionIdentifier", "DocumentIdentifier"], axis = 1, inplace=True)

output_file_path = product['data']
Path(output_file_path).parent.mkdir(exist_ok=True, parents=True)
data_df.to_csv(output_file_path)
print(f"Saved file {output_file_path}")


