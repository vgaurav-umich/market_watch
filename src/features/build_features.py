import pandas as pd

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['clean_gdelt_data']

# -


input_file_path = str(upstream['clean_gdelt_data']['data'])


def read_data(input_file_path):
    print(f"reading {input_file_path}")
    data = pd.read_csv(input_file_path, index_col=0)
    return data


data_df = read_data(input_file_path)

data_df.info()

data_df.sample(10)

cond = (data_df['NegScore'] > 10) | (data_df['PosScore'] > 10)
data_df[cond]




