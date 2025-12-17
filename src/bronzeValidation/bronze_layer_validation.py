import pandas as pd
from pathlib import Path
from ..utils import load_yml,mysql_connect_create_db,get_engine_session
from dotenv import load_dotenv
import argparse
import logging
import os
import datetime as dt
from ..logger import setup_logging

# loading environment variables
load_dotenv(dotenv_path=".env")

#data base password
db_pass = os.getenv("DB_PASS")

# initializing argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--bulk", default = "config/bulk.yaml")
args = parser.parse_args()

def bronze_layer_validation():

    # start time
    runtime_start = dt.datetime.now()

    # logging module setup
    setup_logging()
    logger = logging.getLogger('bronze-validation')

    logger.info("Starting Bronze layer wide validation....")
    logger.info("Starting a connection to Bronze DB for MySql source....")

    # loading the configurations
    bulk_config = load_yml(args.bulk)

    # initializing the config variables
    dbname = bulk_config["dbname"][0]
    username = bulk_config["user_name"]
    host = bulk_config["host"]
    port = bulk_config["port"]

    # start a connection to the db
    try:
        mysql_connect_create_db(dbname, username, host, port, db_pass, create_flag=False)
        logger.info("successfully connected to the MySql Server....")
    except Exception as e:
        logger.exception("Connection to the MySql Server failed...")
        raise RuntimeError("Cannot connect to the MySql Server...")

    # Get the engine and the session
    engine, session = get_engine_session(dbname, username, host, port, db_pass)

    # opening a connection to the database
    conn = engine.connect()

    logger.info("Connection established to Bronze DB for MySql source....")
    logger.info("Running SQL report on the layer....")
    # MySQL query string
    query_string = """
                   SELECT
                       'ohclv_bronze' AS table_name,
                       COUNT(*) AS total_records,
                       (
                           SUM(ticker IS NULL) +
                           SUM(date   IS NULL) +
                           SUM(open   IS NULL) +
                           SUM(high   IS NULL) +
                           SUM(low    IS NULL) +
                           SUM(close  IS NULL) +
                           SUM(volume IS NULL)
                           ) AS total_null_values
                   FROM bronze.ohclv_bronze

                   UNION ALL

                   SELECT
                       'company_meta_data_bronze' AS table_name,
                       COUNT(*) AS total_records,
                       (
                           SUM(company_name IS NULL) +
                           SUM(ticker       IS NULL) +
                           SUM(price        IS NULL) +
                           SUM(market_cap   IS NULL) +
                           SUM(sector       IS NULL) +
                           SUM(industry     IS NULL)
                           ) AS total_null_values
                   FROM bronze.company_meta_data_bronze

                   UNION ALL

                   SELECT
                       'macro_economic_data_bronze' AS table_name,
                       COUNT(*) AS total_records,
                       (
                           SUM(country_id      IS NULL) +
                           SUM(year            IS NULL) +
                           SUM(country_name    IS NULL) +
                           SUM(nominal_gdp     IS NULL) +
                           SUM(real_gdp        IS NULL) +
                           SUM(inflation       IS NULL) +
                           SUM(unemployment    IS NULL) 
                           ) AS total_null_values
                   FROM bronze.macro_economic_data_bronze

                   UNION ALL

                   SELECT
                       'exchange_rates_bronze' AS table_name,
                       COUNT(*) AS total_records,
                       (
                           SUM(date       IS NULL) +
                           SUM(inr_rate   IS NULL) +
                           SUM(usd_amount IS NULL)
                           ) AS total_null_values
                   FROM bronze.exchange_rates_bronze;
                   """
    # building data frames for row count and the null values
    bronze_layer_df = pd.read_sql(query_string, conn)
    logger.info("Finished running SQL report on the layer....")
    report_path = Path('reports/bronzeValidation')
    bronze_layer_df.to_csv(report_path/ "bronze_layer_metrics.csv", index=False)

    conn.close()
    # end time
    runtime_end = dt.datetime.now()

    # closing logs
    logger.info("Finished generation of the report....")
    logger.info(f"Runtime in : {runtime_end - runtime_start}") # time difference for the validation runtime

if __name__ == "__main__":
    bronze_layer_validation()








