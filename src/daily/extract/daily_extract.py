##########################
#
#   Imports section
#
##########################

# general library imports

import argparse
import os
import pandas as pd
from pathlib import Path
import logging
from ...utils import make_dir,load_yml
from ...logger import setup_logging
import requests
import datetime as dt
from dotenv import load_dotenv

# Yahoo finance API
import yfinance as yf

##########################
#
#   execution body
#
##########################

def daily_extr( bulk: str = "config/bulk.yaml", dagster_run_id: str | None = None ):

    #loading environment variables
    load_dotenv(dotenv_path='.env')
    fmp_key = os.getenv('FMP_KEY')

    # setup to load the arguments from config
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--bulk",default = 'config/bulk.yaml')
    # args = parser.parse_args()

    # logging configuration call
    setup_logging()
    logger = logging.getLogger('daily-execution')

    # logging Dagster run id and timestamp - correlation log
    if dagster_run_id:
        logger.info(f"**dagster_run_id** : {dagster_run_id} -> starting daily extract orchestration")
        logger.info(f"**dagster_log_ts** : {dt.datetime.now().isoformat()} -> starting daily extract marking")

    # execution encompassing
    try:

        # runtime start
        runtime_start = dt.datetime.now()

        # today's date
        today = dt.datetime.today().strftime("%Y-%m-%d")

        logger.info("Starting the daily extract execution....")

        # config for loading
        bulk_config = load_yml(bulk)

        # config values
        ticker_list = bulk_config['tickers'] # list of the select company stock tickers
        ohclv_root = Path(bulk_config['ohclv_daily_root']) # ohclv daily data landing path
        meta_data_root = Path(bulk_config['meta_data_daily_root']) # Company meta data landing path
        exchange_root = Path(bulk_config['exchange_rate_daily_root']) # daily exchange rate data landing path
        meta_data_keys = bulk_config['meta_keys']
        exchange_api = bulk_config['frank_exchange_latest_endpoint']
        fmp_endpoint = bulk_config['fmp_end_point']

        # <--- ohclv daily load block --->
        try:
            ohclv_runtime_start = dt.datetime.now()
            logger.info("Starting OHCLV execution....")

            # datetime for landing path generation
            ohclv_runtime_year = str(ohclv_runtime_start.year)
            ohclv_runtime_month = ohclv_runtime_start.strftime("%b")
            ohclv_runtime_date_day = ohclv_runtime_start.strftime("%d")
            ohclv_file_ts = ohclv_runtime_start.strftime("%H-%M-%S")
            # each ticker execution
            for ticker in ticker_list:
                logger.info(f"Running for ticker : {ticker}")
                # creating the landing path folder structure
                ticker_landing = ohclv_root / ticker / ohclv_runtime_year / ohclv_runtime_month / ohclv_runtime_date_day

                # creating the landing path folder
                make_dir(ticker_landing)

                # retry block for handling failed attempts
                for i in range(5):

                    # Yfinance API call
                    org_df = yf.download(ticker, period ='1d', rounding=True, keepna=True)

                    # retry trigger
                    if org_df.empty:
                        logger.info(f"Implementing retry for ticker : {ticker}")
                        continue
                    else:
                        break

                # flattening the multi_index columns names
                org_df.columns = [col[0].upper() for col in org_df.columns]

                # Adding company name static column
                org_df['COMPANY_TICKER'] = ticker

                org_df.reset_index(inplace=True)

                # loading the data into the landing path
                ticker_file_path = ticker_landing / f"{ticker}_stock_{ohclv_file_ts}.csv"
                org_df.to_csv(ticker_file_path, index=False)
                logger.info(f"Successfully extracted the ohclv data for : {ticker}")

            logger.info("Ran the extract pipeline for all the tickers")

            ohclv_runtime_end = dt.datetime.now()
            logger.info(f"Extract for ohclv took : {ohclv_runtime_end - ohclv_runtime_start}")
        except Exception as e:
            logger.error(f"OHCLV execution failed, ohclv data load is not completed. \n{e}")

        # <--- company metadata extract for CDC check --->

        try:
            logger.info("Starting the data load for company meta data....")
            meta_runtime_start = dt.datetime.now()
            company_meta_data_list = [] # empty list to hold the responses

            # timestamps for the company metadata
            meta_year = str(meta_runtime_start.year)
            meta_month = meta_runtime_start.strftime("%b")
            meta_day = meta_runtime_start.strftime("%d")
            meta_data_runtime_ts = meta_runtime_start.strftime("%H-%M-%S")

            # meta data landing path generation
            meta_landing_path = meta_data_root / meta_year / meta_month / meta_day
            make_dir(meta_landing_path)

            # going through the ticker list

            for tick in ticker_list:

                # specific url endpoint for the company
                url = fmp_endpoint + tick + '&apikey=' + fmp_key

                # single dict list
                response = requests.get(url).json()

                logger.info(f"Extracting the data for : {response[0]['companyName']}")

                # creating a sub dictionary
                select_meta_data = {k : response[0][k] for k in meta_data_keys if k in response[0].keys()}

                company_meta_data_list.append(select_meta_data)

            meta_data_df = pd.DataFrame(company_meta_data_list)

            meta_data_df.to_csv(meta_landing_path / f'company_metadata_{meta_data_runtime_ts}.csv', index=False)

            logger.info("Ran the company meta data end point for all the tickers....")
            meta_runtime_end = dt.datetime.now()
            logger.info(f"Meta data extract took {meta_runtime_end - meta_runtime_start}")

        except Exception as e:
            logger.error(f"Company Meta Data execution failed, meta data load is not completed. \n{e}")

        # <--- Block for running the exchange rate api endpoint to load the data in landing path

        try:
            logger.info("Starting the data load for the exchange rate....")

            #timestamp section for exhcnage rate API
            exchange_rate_start = dt.datetime.now()
            exchange_rate_year = str(exchange_rate_start.year)
            exchange_rate_month = exchange_rate_start.strftime("%b")
            exchange_rate_day = exchange_rate_start.strftime("%d")
            exchange_rate_runtime_start = exchange_rate_start.strftime("%H-%M-%S")

            # landing zone for exchange rate
            exchange_rate_path = exchange_root / exchange_rate_year / exchange_rate_month / exchange_rate_day
            make_dir(exchange_rate_path)

            # data point extraction for exchange rate
            frank_response = requests.get(exchange_api).json()
            exchange_rate_df = pd.json_normalize(frank_response)
            exchange_rate_df.reset_index(inplace=True)
            exchange_rate_df['USD_rate'] = 1
            exchange_rate_df.rename(columns={'rates.INR': 'inr_rate'},inplace=True)
            exchange_rate_df.drop(columns=['index','amount','base'], inplace=True)

            # storing the df
            exchange_file_path = exchange_rate_path / f'exchange_rate{exchange_rate_runtime_start}.csv'
            exchange_rate_df.to_csv(exchange_file_path, index=False)

            logger.info(f"Exchange rate data point extracted for {str(exchange_rate_start.date())}...")

            exchange_rate_runtime_end = dt.datetime.now()
            logger.info(f"Exchange rate data extracted in : {exchange_rate_runtime_end - exchange_rate_start}")

        except Exception as e:
            logger.error(f"Exchange rate data load is not completed without error. \n{e}")

        logger.info("Ran the extraction execution for ohclv, Exchange rate and meta data....")
        runtime_end = dt.datetime.now()
        logger.info(f"Daily extract took : {runtime_end - runtime_start}")

    except Exception as e:
        logger.error(f"There is an error while processing the daily load : {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bulk", default="config/bulk.yaml")
    args = parser.parse_args()
    daily_extr(bulk=args.bulk)


