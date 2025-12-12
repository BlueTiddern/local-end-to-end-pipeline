import os
from dotenv import load_dotenv
import pandas as pd
from ...utils import load_yml,get_engine_session,recreate_table,mysql_connect_create_db
from ...models.bronze.ohclv import OHCLVBronze
import argparse
import datetime as dt
from pathlib import Path
import logging
from ...logger import setup_logging

load_dotenv(dotenv_path=".env")
db_pass = os.getenv("DB_PASS")

parser = argparse.ArgumentParser()
parser.add_argument("--bulk",default="config/bulk.yaml")
args = parser.parse_args()
bulk_config = load_yml(args.bulk)

def load_ohclv_bronze():

    # logging configuration
    setup_logging()
    logger = logging.getLogger('bronze-execution')

    runtime_start = dt.datetime.now()

    # landing path
    DATA_DIR = Path(bulk_config["ohclv_root"])

    # config logs
    db_name = bulk_config["dbname"][0]
    user_name = bulk_config["user_name"]
    host = bulk_config["host"]
    port = bulk_config["port"]

    # time variables
    runtime_datetime = dt.datetime.now()
    runtime_year = str(runtime_datetime.year)
    runtime_month = runtime_datetime.strftime("%b")
    runtime_date = runtime_datetime.strftime("%d")
    runtime_time = runtime_datetime.time().strftime("%H-%M-%S")
    logger.info("Starting the Ohclv Bronze Data Load....")
    try:
        try:
            # connect to mysql and create the db if it does not exist
            mysql_connect_create_db(db_name,user_name,host,port,db_pass)
            logger.info("Successfully established connection to the MySQL  database")
        except Exception as e:
            logger.error(e)

        try:
            # creating the engine and session for the select db
            engine, session = get_engine_session(db_name,user_name,host,port,db_pass)
            # dropping and recreating the table
            recreate_table(engine,OHCLVBronze)
            logger.info("Starting the data load from CSV to Database Bronze table")
            for ticker in os.listdir(DATA_DIR):
                ticker_path = DATA_DIR / ticker
                if not os.path.isdir(ticker_path):
                    continue

                # root folder with csv's
                leaf_folder = ticker_path / runtime_year / runtime_month / runtime_date

                # list of csv files in the root folder
                files = list(leaf_folder.glob("*.csv"))
                latest_file = None
                if files:
                    latest_file = max(files, key=os.path.getmtime)
                else:
                    print("error")

                if not os.path.exists(latest_file):
                    print(f"No CSV for {ticker}")
                    continue

                print(f"Loading: {ticker}...")

                df = pd.read_csv(latest_file)

                for _, row in df.iterrows():
                    record = OHCLVBronze(
                        ticker=ticker,
                        date=row["Date"],
                        open=row["OPEN"],
                        high=row["HIGH"],
                        low=row["LOW"],
                        close=row["CLOSE"],
                        volume=row["VOLUME"]
                    )
                    session.add(record)

                session.commit()

            logger.info("Finished loading data for each ticker into the database....")
        except Exception as e:
            logger.exception("OHCLV Bronze load failed, there was an error with data loading....")

        logger.info("Completed OHCLV Data load process into Database....")
        runtime_end = dt.datetime.now()
        logger.info(f"Finished in {runtime_end - runtime_start}...")

    except Exception as e:
        logger.exception("Error while processing the OHCLV data load....")

if __name__ == "__main__":
    load_ohclv_bronze()






