from dotenv import load_dotenv
import argparse
from ...utils import mysql_connect_create_db,load_yml,get_engine_session
import os
import logging
import datetime as dt
from sqlalchemy import text
from ...logger import setup_logging

# loading data base password
load_dotenv(dotenv_path='.env')
db_pass = os.getenv("DB_PASS")

# loading configs
parser = argparse.ArgumentParser()
parser.add_argument("--bulk",default = "config/bulk.yaml")
args = parser.parse_args()

def add_rank_trim():

    #logging module configuration
    setup_logging()
    logger = logging.getLogger('bronze-execution')

    start_time = dt.datetime.now()

    #load the config variables
    bulk_config = load_yml(args.bulk)
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

        # ranking the stock market data to deduplicate in silver layer and trimming the string fields
        try:

            with engine.connect() as conn:

                conn.execute(text("""DROP TABLE IF EXISTS bronze.ohclv_processed"""))

                logger.info("ohclv_processed table dropped successfully in the Bronze layer....")

                conn.execute(text("""
                
                CREATE TABLE bronze.ohclv_processed AS
                SELECT 
                    id AS stock_id,
                    TRIM(ticker) AS ticker,
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    ROW_NUMBER() OVER(PARTITION BY ticker,date) AS rank_assigned
                FROM bronze.ohclv_bronze
                    
                """))

            logger.info("created new table - ohclv data processed successfully in the Bronze layer....")

        except Exception as e:
            logger.exception("Error while processing stock market data...")



        # ranking company metadata and trimming the string fields

        try:

            with engine.connect() as conn:

                conn.execute(text("""DROP TABLE IF EXISTS bronze.company_meta_data_processed"""))
                logger.info("company_meta_data_processed table dropped successfully in the Bronze layer....")

                conn.execute(text("""
                
                CREATE TABLE bronze.company_meta_data_processed AS
                SELECT
                    company_id,
                    TRIM(company_name) AS company_name,
                    TRIM(ticker) AS ticker,
                    price,
                    market_cap,
                    TRIM(sector) AS sector,
                    TRIM(industry) AS industry,
                    ROW_NUMBER() OVER(PARTITION BY ticker) AS rank_assigned
                FROM bronze.company_meta_data_bronze
                    
                
                """))

            logger.info("created new table - company_meta data processed successfully in the Bronze layer....")
        except Exception as e:
            logger.exception("Error while processing company meta data....")

        # ranking the exchange rate data to dedup in silver layer

        try:

            with engine.connect() as conn:

                conn.execute(text("""DROP TABLE IF EXISTS bronze.exchange_rates_processed"""))

                logger.info("exchange_rates_processed table dropped successfully in the Bronze layer....")

                conn.execute(text("""
                
                CREATE TABLE bronze.exchange_rates_processed AS
                SELECT
                    id AS rate_id,
                    DATE(date) AS date,
                    inr_rate,
                    usd_amount,
                    ROW_NUMBER() OVER(PARTITION BY date) AS rank_assigned
                FROM bronze.exchange_rates_bronze
                
                """))

            logger.info("created new table - exchange_rates processed successfully in the Bronze layer....")

        except Exception as e:
            logger.exception("Error while processing exchange rate data....")


        # adding rank and trimming text fields for the silver layer
        try:

            with engine.connect() as conn:

                conn.execute(text("""DROP TABLE IF EXISTS bronze.macro_economic_data_processed"""))

                logger.info("macro_economic_data_processed table dropped successfully in the Bronze layer....")


                conn.execute(text("""
                
                CREATE TABLE bronze.macro_economic_data_processed AS
                SELECT
                    id AS data_id,
                    TRIM(country_name) AS country_name,
                    TRIM(country_id) AS country_code,
                    year,
                    nominal_gdp,
                    real_gdp,
                    inflation,
                    unemployment,
                    ROW_NUMBER() OVER(PARTITION BY country_id,year) AS rank_assigned
                FROM bronze.macro_economic_data_bronze
                    
                """))

            logger.info("created new table - macro economic data processed successfully in the Bronze layer....")

        except Exception as e:
            logger.exception("Error while processing macro economic data....")

    except Exception as e:
        print(e)


    runtime_datetime = dt.datetime.now()

    logger.info("successfully added ranks enabling deduplication for silver layer, trimmed the string fields....")
    logger.info(f"Runtime in -{runtime_datetime - start_time}...")

if __name__ == "__main__":
    add_rank_trim()


