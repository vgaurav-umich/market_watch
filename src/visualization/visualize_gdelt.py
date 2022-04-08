# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['clean_gdelt_data']
# -

import pandas as pd
import altair as alt
from altair import datum
alt.data_transformers.disable_max_rows()
alt.themes.enable('fivethirtyeight')


# ### Load Data File to Visualize
#
# The file path to data file is inserted automatically by Ploomber using upstream task data dictionary

input_file_path = str(upstream['clean_gdelt_data']['data'])

def read_data(input_file_path):
    print(f"reading {input_file_path}")
    data = pd.read_csv(input_file_path, index_col=0)
    return data


data_df = read_data(input_file_path)

dates = data_df.GKGRECORDID.str.extract(pat="([0-9]+)")
data_df['Date'] = pd.to_datetime(dates[0], format="%Y%m%d%H%M%S")

data_df.info()

# ### Line chart to see any obvious trends with sentiments on Tesla related news
# Note that we do not see any major shift in trend. It is however trending positive (trending to be more negative here in context of NegScore)

base = alt.Chart(data_df)
line_chart = base.mark_line().transform_window(
    # The field to average
    rolling_mean='mean(NegScore)',
    # The number of values before and after the current value to include.
    frame=[360, 0]
).encode(
    alt.X('Date:T'),
    alt.Y('rolling_mean:Q', )
).properties(
    width = 900
).configure(
    font='monospace'
)
line_chart



