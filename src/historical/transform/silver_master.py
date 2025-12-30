import logging
import argparse
from ...utils import mysql_connect_create_db,load_yml,get_engine_session
from dotenv import load_dotenv
import os
from sqlalchemy import text
import datetime as dt
from ...logger import setup_logging

# loading the db password
load_dotenv(dotenv_path=".env")

# configuration arguments setup
parser = argparse.ArgumentParser()
parser.add_argument("--bulk", default = 'config/bulk.yaml')
args = parser.parse_args()


def silver_ddl():

    #logging configuration
    setup_logging()
    logger = logging.getLogger('silver-execution')

    # getting the project config files
    bulk_config = load_yml(args.bulk)

    # runtime start
    runtime_start = dt.datetime.now()

    # initializing the config variables
    dbname = bulk_config["dbname"][0]
    username = bulk_config["user_name"]
    host = bulk_config["host"]
    port = bulk_config["port"]

    # silver db name
    dbname_silver = bulk_config["dbname"][1]

    # db pass
    db_pass = os.getenv("DB_PASS")

    # start a connection to - MySQL server
    try:
        mysql_connect_create_db(dbname, username, host, port, db_pass, create_flag=False)
        logger.info("successfully connected to the MySql Server....")
    except Exception as e:
        logger.exception("Connection to the MySql Server failed...")
        raise RuntimeError("Cannot connect to the MySql Server...")

    # working logic for silver layer
    try:

        # getting the database engine for the bronze layer
        engine, session = get_engine_session(dbname, username, host, port, db_pass)

        try:
            # creating the silver db if not exist
            with engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {dbname_silver};"))
                logger.info("Successfully silver DB created / silver DB exists...")
        except Exception as e:
            logger.exception("Failed to create the database...")

        # logical block to deduplicate the data from bronze layer and create clean table for ohclv

        try:
            with engine.connect() as conn:

                conn.execute(text(f"DROP TABLE IF EXISTS {dbname_silver}.ohclv_clean"))

                logger.info("Successfully dropped the ochlv_clean table...")

                conn.execute(text(f"""
                
                CREATE TABLE {dbname_silver}.ohclv_clean AS
                SELECT
                    stock_id,
                    ticker,
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM {dbname}.ohclv_processed
                WHERE rank_assigned = 1
                
                """))

                logger.info("Successfully created the ohclv_clean table...")

                conn.execute(text(f"""

                CREATE TABLE IF NOT EXISTS {dbname_silver}.ohclv_silver (
                        
                        stock_id INT AUTO_INCREMENT PRIMARY KEY,
                        ticker VARCHAR(10) NOT NULL,
                        date DATE NOT NULL,
                        open DECIMAL(6,2) NOT NULL,
                        high DECIMAL(6,2) NOT NULL,
                        low DECIMAL(6,2) NOT NULL,
                        close DECIMAL(6,2) NOT NULL,
                        volume BIGINT NOT NULL,
                        insert_datetime DATE NOT NULL,
                        UNIQUE KEY uq_ticker_date (ticker, date)
            
                    )
                """))

                logger.info("Successfully created the ohclv_silver table with schema enforced...")

        except Exception as e:
            logger.exception("error processing the ohclv table load for silver...")

        try:

            with engine.connect() as conn:
                conn.execute(text(f"""DROP TABLE IF EXISTS {dbname_silver}.company_meta_data_clean"""))
                logger.info("Successfully dropped the company_meta_data_clean table...")

                conn.execute(text(f"""
                    
                    CREATE TABLE {dbname_silver}.company_meta_data_clean AS
                    SELECT
                        company_id,
                        company_name,
                        ticker,
                        price,
                        market_cap,
                        sector,
                        industry
                    FROM {dbname}.company_meta_data_processed
                    WHERE rank_assigned = 1
                
                """))

                logger.info("Successfully created the company_meta_data_clean table...")

                conn.execute(text(f"""
                
                CREATE TABLE IF NOT EXISTS {dbname_silver}.company_meta_data_silver (
                
                    company_id INT AUTO_INCREMENT PRIMARY KEY,
                    company_name VARCHAR(100) NOT NULL,
                    ticker VARCHAR(10) NOT NULL,
                    price DECIMAL(6,2) NOT NULL,
                    market_cap BIGINT NOT NULL,
                    sector VARCHAR(50) NOT NULL,
                    industry VARCHAR(50) NOT NULL
                    
                )
                
                """))

                logger.info("Successfully created the company_meta_data_silver table with schema enforced....")

        except Exception as e:
            logger.exception("error processing the company meta data table load for silver...")

        try:
            with engine.connect() as conn:
                conn.execute(text(f"""DROP TABLE IF EXISTS {dbname_silver}.macro_economic_data_clean"""))
                logger.info("Successfully dropped the macro_economic_data_clean table....")

                conn.execute(text(f"""
                
                    CREATE TABLE {dbname_silver}.macro_economic_data_clean AS
                    SELECT
                        data_id,
                        country_name,
                        country_code,
                        year,
                        nominal_gdp,
                        real_gdp,
                        inflation,
                        unemployment
                    FROM {dbname}.macro_economic_data_processed
                    WHERE rank_assigned = 1
                
                """))

                logger.info("Successfully created the macro_economic_data_clean table...")

                conn.execute(text(f"""CREATE TABLE IF NOT EXISTS {dbname_silver}.macro_economic_data_silver (
                
                    data_id INT AUTO_INCREMENT PRIMARY KEY,
                    country_name VARCHAR(50) NOT NULL,
                    country_code VARCHAR(25) NOT NULL,
                    year INT NOT NULL,
                    nominal_gdp FLOAT NOT NULL,
                    real_gdp FLOAT NOT NULL,
                    inflation FLOAT NOT NULL,
                    unemployment FLOAT NOT NULL
                )
                """))

                logger.info("Successfully created the macro_economic_data_silver table with schema enforced....")

        except Exception as e:
            logger.exception("error processing the macro_economic_data table load for silver...")

        try:
            with engine.connect() as conn:
                conn.execute(text(f"""DROP TABLE IF EXISTS {dbname_silver}.exchange_rates_clean"""))
                logger.info("Successfully dropped the exchange_rates_clean table...")
                conn.execute(text(f"""
                    
                    CREATE TABLE {dbname_silver}.exchange_rates_clean AS
                    SELECT
                        rate_id,
                        date,
                        inr_rate,
                        usd_amount
                    FROM {dbname}.exchange_rates_processed
                    WHERE rank_assigned = 1
                    
                """))
                logger.info("Successfully created the exchange_rates_clean table...")

                conn.execute(text(f"""
                
                    CREATE TABLE IF NOT EXISTS {dbname_silver}.exchange_rates_silver (
                    
                    rate_id INT AUTO_INCREMENT PRIMARY KEY,
                    date DATE NOT NULL,
                    inr_rate FLOAT NOT NULL,
                    usd_amount SMALLINT NOT NULL,
                    insert_datetime DATE NOT NULL,
                    UNIQUE KEY uq_exchange_rate (date)
                                       
                    )
                
                """))

                logger.info("Successfully created the exchange_rates_silver table with schema enforced....")

        except Exception as e:
            logger.info("error processing the exchange_rate_data table load for silver...")

    except Exception as e:
        print(e)

    runtime_end = dt.datetime.now()
    logger.info("Successfully finished loading the data from bronze to silver and created schema enforced tables...")
    logger.info(f"Runtime in - {runtime_end - runtime_start}")


if __name__ == "__main__":
    silver_ddl()


