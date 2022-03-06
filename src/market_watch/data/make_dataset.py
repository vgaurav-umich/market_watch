# import click
# import logging
import pandas as pd
from sklearn import datasets
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# -


def make_dataset(product):

    d = datasets.load_iris()
    df = pd.DataFrame(d['data'])
    df.columns = d['feature_names']
    df['target'] = d['target']

    Path(str(product['data'])).parent.mkdir(exist_ok=True, parents=True)

    df.to_parquet(str(product['data']))


make_dataset(product)

#
# @click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
# def main(input_filepath, output_filepath):
#     """ Runs data processing scripts to turn raw data from (../raw) into
#         cleaned data ready to be analyzed (saved in ../processed).
#     """
#     logger = logging.getLogger(__name__)
#     logger.info('making final data set from raw data')
#
#
# if __name__ == '__main__':
#     log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     logging.basicConfig(level=logging.INFO, format=log_fmt)
#
#     # not used in this stub but often useful for finding various files
#     project_dir = Path(__file__).resolve().parents[2]
#
#     # find .env automagically by walking up directories until it's found, then
#     # load up the .env entries as environment variables
#     load_dotenv(find_dotenv())
#     make_dataset(product)
#
#     main()
