import argparse
from ...utils import load_yml,make_dir
import os
import yfinance as yf
from pathlib import Path
import datetime as dt
from ...logger import setup_logging
import logging


def ohclv_load():

    # logger configuration
    setup_logging()
    logger = logging.getLogger('bronze-execution')


    parser = argparse.ArgumentParser()
    parser.add_argument('--bulk', default='config/bulk.yaml')
    args = parser.parse_args()

    bulk_config = load_yml(args.bulk)

    ticker_list = bulk_config['tickers'] # list of the select company stock tickers
    root_path = Path(bulk_config['ohclv_root']) # root folder for the OHCLV data
    start_date = bulk_config['start_date'] # start date
    end_date = bulk_config['end_date'] # end date

    runtime_datetime = dt.datetime.now()
    runtime_year = str(runtime_datetime.year)
    runtime_month = runtime_datetime.strftime("%b")
    runtime_date = runtime_datetime.strftime("%d")
    runtime_time = runtime_datetime.time().strftime("%H-%M-%S")

    try:

        runtime_start = dt.datetime.now()
        logger.info(f"Starting the processing of the OHCLV data extract...")

        if os.path.isdir(root_path):
            print("root exists, downloading the ohclv stock data....\n")
        else:
            print("root does not exist, creating the root folder for ohclv...\n")
            make_dir(root_path)

        print("creating the folders select company ohclv stock data...\n")
        logger.info("creating the landing paths for the extracted data...")

        try:
            # loop through the tickers
            for ticker in ticker_list:

                print("\n----------------------------------------------------------------------------------\n**********************************************************************************\n")


                #folder path for each ticker
                ticker_out_path = root_path / ticker /runtime_year / runtime_month / runtime_date

                # make folders for each of the select company
                make_dir(ticker_out_path)

                # retry block for implementing Yfinance API

                for i in range(5):
                    # downloading the companies data as data frame
                    org_df = yf.download(ticker, start=start_date,end=end_date, interval='1d', rounding=True, keepna=True)
                    if org_df.empty:
                        logger.info("Data Frame is empty, call the Yfinance API...")
                        print("implementing retry")
                        continue
                    else:
                        break

                # flattening the multi_index columns
                org_df.columns = [col[0].upper() for col in org_df.columns]

                # Adding company name static column
                org_df['COMPANY_TICKER'] = ticker

                org_df.reset_index(inplace=True)

                ticker_file_path = ticker_out_path / f"{ticker}_stock_{runtime_time}.csv"
                org_df.to_csv(ticker_file_path, index=False)

                print("\n----------------------------------------------------------------------------------\n**********************************************************************************\n")
            logger.info("Successfully extracted the OHCLV stock data for the respective company ticker values...")
        except Exception as e:
            print(f"OHCLV stock data download failed.......\n\n[ERROR] failed to process the data:  {e}")

        runtime_end = dt.datetime.now()
        logger.info("Successfully completed data extract process for OHCLV")
        logger.info(f"Runtime finished in - {runtime_end - runtime_start} ...")

    except Exception as e:
        logger.info("Error while processing the ohclv data extract....")


if __name__ == '__main__':
 ohclv_load()
