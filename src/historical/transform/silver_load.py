import os
from dotenv import load_dotenv
import argparse
from sqlalchemy import text
from ...utils import mysql_connect_create_db,load_yml,get_engine_session
import datetime as dt
import logging
from ...logger import setup_logging

#loading the database password
load_dotenv(dotenv_path=".env")
db_pass = os.getenv("DB_PASS")

# loading the configuration
parser = argparse.ArgumentParser()
parser.add_argument("--bulk", default = "config/bulk.yaml")
args = parser.parse_args()

def silver_load():

    #logging configuration
    setup_logging()
    logger = logging.getLogger('silver-execution')
    # start time
    runtime_start = dt.datetime.now()

    insert_ts = str(dt.date.today())

    # config variables
    bulk_config = load_yml(args.bulk)
    # initializing the config variables
    dbname = bulk_config["dbname"][1]
    username = bulk_config["user_name"]
    host = bulk_config["host"]
    port = bulk_config["port"]

    # start a connection to - MySQL server
    try:
        mysql_connect_create_db(dbname, username, host, port, db_pass, create_flag=False)
        logger.info("successfully connected to the MySql Server....")
    except Exception as e:
        logger.exception("Connection to the MySql Server failed...")
        raise RuntimeError("Cannot connect to the MySql Server...")

    # loading block
    try:

        engine,_ = get_engine_session(dbname, username, host, port, db_pass)
        logger.info("Starting silver layer load into schema enforced tables....")

        # ohclv data load
        try:
            with engine.begin() as conn:

                conn.execute(text(f"TRUNCATE TABLE {dbname}.ohclv_silver"))
                logger.info("ohclv_silver table truncated, starting the load")
                conn.execute(text(f"""
                    
                    INSERT INTO {dbname}.ohclv_silver (ticker,date,open,high,low,close,volume,insert_datetime) 
                    SELECT 
                        ticker,
                        date,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        '{insert_ts}'
                    FROM {dbname}.ohclv_clean
            
                """))
                logger.info("ohclv_silver tabled data loaded successfully....")
        except Exception as e:
            logger.exception("Failed to insert ohclv_silver table")

        try:

            with engine.begin() as conn:
                conn.execute(text(f"""TRUNCATE TABLE {dbname}.company_meta_data_silver"""))
                logger.info("company_meta_data_silver table truncated, starting the load")
                conn.execute(text(f"""INSERT INTO {dbname}.company_meta_data_silver (
                
                company_name, ticker, price, market_cap, sector, industry
                
                ) SELECT
                    company_name,
                    ticker,
                    price,
                    market_cap,
                    sector,
                    industry
                FROM {dbname}.company_meta_data_clean
                
                """))
                logger.info("company_meta_data_silver loaded successfully....")
        except Exception as e:
            logger.exception("Failed to insert company_meta_data_silver")

        try:

            with engine.begin() as conn:
                conn.execute(text(f"""TRUNCATE TABLE {dbname}.macro_economic_data_silver"""))
                logger.info("macro_economic_data_silver table truncated, starting the load")
                conn.execute(text(f"""INSERT INTO {dbname}.macro_economic_data_silver (
                
                country_name,
                country_code,
                year,
                nominal_gdp,
                real_gdp,
                inflation,
                unemployment
                
                ) SELECT
                    country_name,
                    country_code,
                    year,
                    nominal_gdp,
                    real_gdp,
                    inflation,
                    unemployment
                FROM {dbname}.macro_economic_data_clean
                
                """))
                logger.info("macro_economic_data_silver table loaded successfully....")

        except Exception as e:
            logger.exception("Failed to insert macro_economic_data_clean")

        try:
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {dbname}.exchange_rates_silver"))
                logger.info("exchange_rates_silver table truncated, starting the load")
                conn.execute(text(f"""INSERT INTO {dbname}.exchange_rates_silver (
                date, inr_rate, usd_amount,insert_datetime
                )
                SELECT
                    date,
                    inr_rate,
                    usd_amount,
                    '{insert_ts}'
                FROM {dbname}.exchange_rates_clean
                """))
                logger.info("exchange_rates_silver loaded successfully....")
        except Exception as e:
            logger.exception("Failed to insert exchange_rates_silver")

        runtime_end = dt.datetime.now()
        logger.info("Silver layer data loaded successfully....")
        logger.info(f"Runtime in - {runtime_end - runtime_start}")
    except Exception as e:
        logger.exception("Error processing the silver load...")
        print(e)

if __name__ == "__main__":
    silver_load()


