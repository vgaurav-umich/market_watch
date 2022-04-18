# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None
# -

import pandas as pd

# +
# There are 2 tables on the Wikipedia page
# we want the first table

payload=pd.read_html(source_url)
first_table = payload[0]
second_table = payload[1]

df = first_table
# -

df.drop_duplicates(subset=['CIK'], inplace=True)

df.info()

df.sample(10)

output_file_path = product['data']
df.to_csv(output_file_path)
print(f"Saved file {output_file_path}")  

del df, first_table, second_table


