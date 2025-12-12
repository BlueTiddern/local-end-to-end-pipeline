import os
from ...utils import load_yml,make_dir
from dotenv import load_dotenv
from pathlib import Path
from argparse import ArgumentParser
import requests
import pandas as pd
import datetime as dt
import logging
from ...logger import setup_logging

load_dotenv(dotenv_path='.env')
#load_dotenv()
fmp_key = os.getenv('FMP_KEY')


def load_metadata():

    # logger configuration
    setup_logging()
    logger = logging.getLogger('bronze-execution')

    parser = ArgumentParser()
    parser.add_argument("--bulk", default="config/bulk.yaml")
    args = parser.parse_args()

    runtime_start = dt.datetime.now()

    bulk_config = load_yml(args.bulk) # loading the yaml configs

    ticker_list = bulk_config['tickers'] # list of the select company stock tickers
    meta_path = Path(bulk_config['meta_data_root']) # meta data root directory
    select_keys = bulk_config['meta_keys'] # select dictionary keys
    company_meta_data_list = [] # list to hold the JSON array

    runtime_datetime = dt.datetime.now()
    runtime_year = str(runtime_datetime.year)
    runtime_month = runtime_datetime.strftime("%b")
    runtime_date = runtime_datetime.strftime("%d")
    runtime_time = runtime_datetime.time().strftime("%H-%M-%S")

    try:
        # "https://financialmodelingprep.com/stable/profile?symbol="
        fmp_end_point = bulk_config['fmp_end_point']

        logger.info("Starting the load process for the company meta data...")

        if os.path.isdir(meta_path):
            print("root exists, downloading the company meta data....\n")
        else:
            print("root does not exist, creating the root folder for meta data...\n")
            make_dir(meta_path)

        print("creating the folders for select companies...\nPooling raw data for companies....\n")

        path_dest = meta_path / runtime_year / runtime_month / runtime_date
        make_dir(path_dest)

        try:

            for tick in ticker_list:
                print("\n----------------------------------------------------------------------------------\n**********************************************************************************\n")

                # specific url endpoint for the company
                url = fmp_end_point + tick + '&apikey=' + fmp_key

                # single dict list
                response = requests.get(url).json()

                print(f"Extracting data for  {response[0]['companyName']}")

                # creating a sub dictionary
                select_meta_data = {k : response[0][k] for k in select_keys if k in response[0].keys()}

                company_meta_data_list.append(select_meta_data)


            meta_data_df = pd.DataFrame(company_meta_data_list)

            meta_data_df.to_csv(path_dest / f'company_metadata_{runtime_time}.csv', index=False)

            logger.info("Company meta data extracted successfully for each ticker....")
        except Exception as e:
            logger.info("Error while downloading the company Meta data...")


        runtime_end = dt.datetime.now()
        logger.info("Successfully completed data extract process for company meta...")
        logger.info(f"Runtime finished in - {runtime_end - runtime_start} ...")

    except Exception as e:
        logger.info("Error processing the company meta data extract...")


if __name__ == "__main__":
    load_metadata()





