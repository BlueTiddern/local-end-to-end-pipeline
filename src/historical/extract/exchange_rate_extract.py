from pathlib import Path
import requests
import pandas as pd
from ...utils import load_yml,make_dir
import json
import argparse
import datetime as dt
import logging
from ...logger import setup_logging

def load_exchange_rates():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bulk',default='config/bulk.yaml')
    args = parser.parse_args()

    bulk_config = load_yml(args.bulk)

    exchange_root = Path(bulk_config['exchange_rate_root'])
    exchange_end_point = bulk_config['frank_exchange_end_point'].format(start_date=bulk_config['start_date'])

    #logger configuration
    setup_logging()
    logger = logging.getLogger('bronze-execution')

    runtime_datetime = dt.datetime.now()
    runtime_year = str(runtime_datetime.year)
    runtime_month = runtime_datetime.strftime("%b")
    runtime_date = runtime_datetime.strftime("%d")
    runtime_time = runtime_datetime.time().strftime("%H-%M-%S")
    try:
        runtime_start = dt.datetime.now()
        logger.info("Starting the data extraction process for exchange rates data....")
        print("creating landing for the historic exchange data....\n\nDownloading the historic exchange data....\n")
        make_dir(exchange_root)

        print("Creating runtime folder")

        runtime_folder = exchange_root / runtime_year / runtime_month / runtime_date

        make_dir(runtime_folder)
        try:
            response = requests.get(exchange_end_point).json()

            exchange_df = pd.DataFrame.from_dict(response['rates'], orient='index') # pandas data parsing from the response

            exchange_df.index.name = 'Date'

            exchange_df.reset_index(inplace=True)

            exchange_df['USD_rate'] = 1

            exchange_df.rename(columns={'INR': 'INR_amount'},inplace=True)

            logger.info("Exchange rates data extracted successfully and data is parsed into usable format...")

            exchange_file_path = runtime_folder / f'exchange_rates_{runtime_time}.csv'

            exchange_df.to_csv(exchange_file_path, index=False)

            logger.info("Successfully extracted the exchange rates and data is staged, process completed...")
            runtime_end = dt.datetime.now()
            logger.info(f"Runtime in - {runtime_end-runtime_start}...")
        except Exception as e:
            logger.exception(f"Error while downloading the exchange rates data from the API {e}")

    except Exception as e:
        logger.exception(f"Error while executing the download for the exchange rate data {e}...")
if __name__ == "__main__":
    load_exchange_rates()











