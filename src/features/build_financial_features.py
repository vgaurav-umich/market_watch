import pandas as pd

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['fetch_yfinance_data']

# -


input_file_path = str(upstream['fetch_yfinance_data']['data'])


def read_data(input_file_path):
    print(f"reading {input_file_path}")
    data = pd.read_csv(input_file_path, index_col=0)
    return data


data_df = read_data(input_file_path)

data_df.info()

data_df.sample(10)




