import pandas as pd

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['make_dataset']

# -


def build_features(product):
    data = pd.read_parquet(str(upstream['make_dataset']['data']))
    data['sepal area (cm2)'] = data['sepal length (cm)'] * data['sepal width (cm)']
    data.to_parquet(str(product['data']))
    print("Hello")


build_features(product)
