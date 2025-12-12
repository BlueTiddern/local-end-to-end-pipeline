import os
from dotenv import load_dotenv
import pandas as pd
from ...utils import load_yml,recreate_table,mysql_connect_create_db,get_engine_session
from ...models.bronze.company_meta_data import CompanyMetaDataBronze
import argparse
import datetime as dt
from pathlib import Path
from ...logger import setup_logging
import logging

load_dotenv(dotenv_path=".env")
db_pass = os.getenv("DB_PASS")

parser = argparse.ArgumentParser()
parser.add_argument("--bulk",default="config/bulk.yaml")
args = parser.parse_args()
bulk_config = load_yml(args.bulk)


def load_meta_bronze():

    # calling the log configuration
    setup_logging()
    logger = logging.getLogger('bronze-execution')

    # config file loading
    DATA_DIR = Path(bulk_config["meta_data_root"])

    db_name = bulk_config["dbname"][0]
    user_name = bulk_config["user_name"]
    host = bulk_config["host"]
    port = bulk_config["port"]
    try:
        logger.info("Starting the process to load the company metadata into the bronze layer....")
        runtime_start = dt.datetime.now()
        try:
            # connect to mysql and create the db if it does not exist
            mysql_connect_create_db(db_name,user_name,host,port,db_pass)
            logger.info("Sucessfully connected to the MySQL database server")
        except Exception as e:
            logger.exception(f"Error while connecting to MySql.... : {e}")
        # creating the engine and session for the select db
        engine, session = get_engine_session(db_name,user_name,host,port,db_pass)
        # dropping and recreating the table
        recreate_table(engine,CompanyMetaDataBronze)
        logger.info("Table dropped if exists and created new table for fresh load....")
        runtime_datetime = dt.datetime.now()
        runtime_year = str(runtime_datetime.year)
        runtime_month = runtime_datetime.strftime("%b")
        runtime_date = runtime_datetime.strftime("%d")
        runtime_time = runtime_datetime.time().strftime("%H-%M-%S")

        try:
            runtime_file_path = DATA_DIR / runtime_year / runtime_month / runtime_date
            if not os.path.isdir(DATA_DIR):
                logger.info("Path to the data is not a directory....")

            files = list(runtime_file_path.glob("*.csv"))
            latest_file = max(files, key=os.path.getmtime)

            df = pd.read_csv(latest_file)

            print("Loading the company's meta data...\n")
            for _, row in df.iterrows():
                record = CompanyMetaDataBronze(
                    company_name=row['companyName'],
                    ticker=row["symbol"],
                    price=row["price"],
                    market_cap=row["marketCap"],
                    sector=row["sector"],
                    industry=row["industry"]
                )
                session.add(record)

            session.commit()
            print("CompanyMetaData Bronze load complete...")
            logger.info("Successfully loaded the company's meta data into the bronze layer....")
        except Exception as err:
            print(f"CompanyMetaData Bronze load failed...: {err}")

        runtime_end = dt.datetime.now()
        logger.info("Completed the company meta data load process into database....")
        logger.info(f"Finished in {runtime_end - runtime_start}...")
    except Exception as e:
        logger.exception(f"Error while loading the company data into the bronze layer: {e}")
if __name__ == "__main__":
    load_meta_bronze()
