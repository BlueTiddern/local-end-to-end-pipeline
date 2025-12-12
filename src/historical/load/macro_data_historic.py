from ...utils import load_yml,mysql_connect_create_db,get_engine_session,recreate_table,gmd_null_handler
import pandas as pd
import os
from ...models.bronze.macro_economic_data import MacroEconomicData
import argparse
from dotenv import load_dotenv
from pathlib import Path
import datetime as dt
import logging
from ...logger import setup_logging

parser = argparse.ArgumentParser()
parser.add_argument("--bulk", default='config/bulk.yaml')
args = parser.parse_args()
bulk_config = load_yml(args.bulk)

load_dotenv(dotenv_path=".env")
db_pass = os.getenv("DB_PASS")

def load_macro_bronze():

    # loading the logging configuration
    setup_logging()
    logger = logging.getLogger('bronze-execution')

    runtime_start = dt.datetime.now()

    logger.info("Starting Macro Bronze Historic Data load into MySql Server....")

    DATA_DIR = Path(bulk_config["macro_data_root"])

    db_name = bulk_config["dbname"][0]
    user_name = bulk_config["user_name"]
    host = bulk_config["host"]
    port = bulk_config["port"]


    # start a connection to the db
    try:
        mysql_connect_create_db(db_name,user_name,host,port,db_pass)
        logger.info("successfully connected to the MySql Server....")
    except Exception as e:
        logger.exception("Connection to the MySql Server failed...")
        raise RuntimeError("Cannot connect to the MySql Server...")

    try:
        # creating the engine and session for the select db
        engine, session = get_engine_session(db_name,user_name,host,port,db_pass)
        logger.info("Successfully connected to the Bronze Database in MySql Server....")
        # dropping and recreating the table
        recreate_table(engine,MacroEconomicData)
        logger.info("Macro data table successfully dropped and recreated")

        runtime_datetime = dt.datetime.now()
        runtime_year = str(runtime_datetime.year)
        runtime_month = runtime_datetime.strftime("%b")
        runtime_date = runtime_datetime.strftime("%d")


        if not os.path.isdir(DATA_DIR):
            print("Data directory does not exist")

        runtime_filepath = DATA_DIR / runtime_year / runtime_month / runtime_date
        files = runtime_filepath.glob("*.csv")
        runtime_file = max(files, key=os.path.getmtime)

        logger.info(f"latest source file retrieved : {runtime_file}, started null value handling ...")

        if not os.path.isfile(runtime_file):
            print("Macro data file does not exist")

        df = pd.read_csv(runtime_file)

        # data handling for null values
        df_filled = df.groupby('ISO3',group_keys=False).apply(gmd_null_handler, include_groups=False)

        logger.info("Null values handled successfully and data frame is ready for loading into MySQL table")


        # convert the numpy numeric to python object, this allows the nan to be changed as None
        # Using where to convert conditionally False values to None
        #df = df.astype(object).where(pd.notna(df), None)

        for _,row in df_filled.iterrows():
            record = MacroEconomicData(
                country_id = row["id"],
                year = row["year"],
                country_name = row["countryname"],
                nominal_gdp = row["NOMINAL_GDP_FILLED"],
                real_gdp = row["REAL_GDP_FILLED"],
                inflation = row["INFLATION_FILLED"],
                unemployment = row["UNEMPLOYMENT_FILLED"]
            )
            session.add(record)
        session.commit()
        # end time
        runtime_end = dt.datetime.now()

        # closing logs
        logger.info("Finished Bronze Macro Economic Data Load into MySql Server....")
        logger.info(f"Runtime in : {runtime_end - runtime_start}") # time difference for the validation runtime

    except Exception as e :
        print(e)

if __name__ == "__main__":
    load_macro_bronze()
