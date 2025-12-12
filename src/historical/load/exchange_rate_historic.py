from ...utils import load_yml,mysql_connect_create_db,get_engine_session,recreate_table
import pandas as pd
import argparse
from dotenv import load_dotenv
import os
from ...models.bronze.exchange_rate_data import ExchangeRateData
from pathlib import Path
import datetime as dt
import logging
from ...logger import setup_logging

load_dotenv(dotenv_path=".env")
db_pass = os.getenv("DB_PASS")

parser = argparse.ArgumentParser()
parser.add_argument("--bulk", default='config/bulk.yaml')
args = parser.parse_args()
bulk_config = load_yml(args.bulk)

def load_exhange_bronze():

    # loading the logging configuration
    setup_logging()
    logger = logging.getLogger('bronze-execution')

    # configure options loading
    DATA_DIR = Path(bulk_config["exchange_rate_root"])

    db_name = bulk_config["dbname"][0]
    user_name = bulk_config["user_name"]
    host = bulk_config["host"]
    port = bulk_config["port"]

    try:
        runtime_start = dt.datetime.now()
        logger.info("starting the exchange rate data load into the bronze layer...")
        # connect to mysql and create the db if it does not exist
        mysql_connect_create_db(db_name,user_name,host,port,db_pass)
        print("Mysql connected.... database exists/created")

        # creating the engine and session for the select db
        engine, session = get_engine_session(db_name,user_name,host,port,db_pass)
        print("Engine to work with DB created and session is activated...")
        # dropping and recreating the table
        recreate_table(engine,ExchangeRateData)

        runtime_datetime = dt.datetime.now()
        runtime_year = str(runtime_datetime.year)
        runtime_month = runtime_datetime.strftime("%b")
        runtime_date = runtime_datetime.strftime("%d")
        logger.info("Successfully connected to the database and the engine has been created....")
        try:
            if not os.path.isdir(DATA_DIR):
                print("Source directory for exchange rate data not found...")

            runtime_folder_path = DATA_DIR / runtime_year / runtime_month / runtime_date

            files = list(runtime_folder_path.glob("*.csv"))
            runtime_file = max(files, key=os.path.getmtime)

            if not os.path.isfile(runtime_file):
                print("Source file for exchange rate data not found...")

            df = pd.read_csv(runtime_file)

            for _, row in df.iterrows():
                record = ExchangeRateData(
                    date=row["Date"],
                    inr_rate=row["INR_amount"],
                    usd_amount=row["USD_rate"],
                )
                session.add(record)
            session.commit()
            print("Exchange rate data loaded...")
            logger.info("Successfully loaded the exchange rate data into the bronze layer....")
            runtime_end = dt.datetime.now()
            logger.info(f"Runtime duration: {runtime_end - runtime_start}")
        except Exception as e:
            print(e)
    except Exception as e:
        logger.exception(e)

if __name__ == "__main__":
    load_exhange_bronze()


