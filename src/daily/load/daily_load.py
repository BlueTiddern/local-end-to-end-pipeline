import os
from dotenv import load_dotenv
import argparse
import pandas as pd
from ...utils import mysql_connect_create_db,get_engine_session, load_yml
from ...logger import setup_logging
import logging
import datetime as dt
from sqlalchemy import text
from pathlib import Path

# main execution block

def daily_load():

    # loading the database password
    load_dotenv(dotenv_path='.env')
    db_pass = os.getenv("DB_PASS")

    # argument parsing - loading the configuration
    parser = argparse.ArgumentParser()
    parser.add_argument("--bulk", default = 'config/bulk.yaml')
    args = parser.parse_args()

    # loading the arguments
    bulk_config = load_yml(args.bulk)
    ohclv_root = Path(bulk_config['ohclv_daily_root'])
    exchange_root = Path(bulk_config['exchange_rate_daily_root'])
    db_name = bulk_config['dbname'][0]
    user_name = bulk_config['user_name']
    host = bulk_config['host']
    port = bulk_config['port']

    # logging configuration
    setup_logging()
    logger = logging.getLogger('daily-execution')

    # execution implementation block
    try:

        logger.info("Starting the daily load execution...")
        runtime_start = dt.datetime.now()

        # execution runtime datetimes
        runtime_year = str(runtime_start.year)
        runtime_month = runtime_start.strftime("%b")
        runtime_date_day = runtime_start.strftime("%d")

        # start a connection to - MySQL server
        try:
            mysql_connect_create_db(db_name, user_name, host, port, db_pass, create_flag=False)
            logger.info("successfully connected to the MySql Server....")
        except Exception as e:
            logger.exception("Connection to the MySql Server failed...")
            raise RuntimeError("Cannot connect to the MySql Server...")

        # Db action block - DDL block for the landing zone
        try:

            # getting the database engine for the bronze layer
            engine, session = get_engine_session(db_name, user_name, host, port, db_pass)

            try:

                logger.info("Starting the DDL database executions...")

                # --- DDL block ---

                with engine.begin() as conn:

                    # lineage table ddl
                    conn.execute(text(


                        f"""CREATE TABLE IF NOT EXISTS {db_name}.ohclv_daily_lineage (
            
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                ticker VARCHAR(20) NOT NULL,
                                date DATE NOT NULL,
                                open FLOAT NOT NULL,
                                high FLOAT NOT NULL,
                                low FLOAT NOT NULL,
                                close FLOAT NOT NULL,
                                volume BIGINT NOT NULL,
                                insert_datetime DATETIME NOT NULL,
                                UNIQUE KEY uq_ticker_date (ticker, date)
                            );
    
                        """

                    ))

                    # dropping combiner table
                    conn.execute(text(

                        f"""
                        
                        DROP TABLE IF EXISTS {db_name}.ohclv_daily_bronze;
                        
                        """

                    ))

                    # ohclv_daily table DDL
                    conn.execute(text(

                        f"""
                        
                        CREATE TABLE IF NOT EXISTS {db_name}.ohclv_daily_bronze (
            
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                ticker VARCHAR(20) NOT NULL,
                                date DATE NOT NULL,
                                open DECIMAL(6,2) NOT NULL,
                                high DECIMAL(6,2) NOT NULL,
                                low DECIMAL(6,2) NOT NULL,
                                close DECIMAL(6,2) NOT NULL,
                                volume BIGINT NOT NULL,
                                insert_datetime DATETIME NOT NULL
                            );
    
                        """

                    ))

                    # exchange rate lineage DDL
                    conn.execute(text(

                        f"""
                        
                        CREATE TABLE IF NOT EXISTS {db_name}.exchange_daily_lineage (
                        
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            date DATE NOT NULL,
                            inr_rate FLOAT NOT NULL,
                            usd_amount INT NOT NULL,
                            insert_datetime DATETIME NOT NULL,
                            UNIQUE KEY uq_fx_date (date)
                            
                        );
    
                        """

                    ))

                    # exchange rate combiner table drop
                    conn.execute(text(

                        f"""
                        
                        DROP TABLE IF EXISTS {db_name}.exchange_daily_bronze;
                        
                        """

                    ))

                    # exchange rate table DDL
                    conn.execute(text(

                        f"""
                        
                        CREATE TABLE IF NOT EXISTS {db_name}.exchange_daily_bronze (
                        
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            date DATE NOT NULL,
                            inr_rate FLOAT NOT NULL,
                            usd_amount INT NOT NULL,
                            insert_datetime DATETIME NOT NULL
                            
                        );
    
                        """

                    ))

                    logger.info("successfully completed the DDL execution for daily load....")
            except Exception as e:
                logger.exception(f"Error while processing the DDL executions : {e}")

            # <--- Data loading for ohclv block --->

            try:
                logger.info("Starting the staging load for ohclv data into the bronze layer")

                for ticker in os.listdir(ohclv_root):

                    # folder setup
                    ticker_path = ohclv_root / ticker
                    leaf_folder = ticker_path / runtime_year / runtime_month / runtime_date_day

                    # list of csv files in the root folder
                    files = list(leaf_folder.glob("*.csv"))
                    latest_file = None

                    if files:
                        latest_file = max(files, key=os.path.getmtime)
                    else:
                        logger.exception(f"There are no files in {leaf_folder}")

                    ohclv_df = pd.read_csv(latest_file)

                    batch_ts = runtime_start

                    # renaming the fields to match the database
                    load_df = ohclv_df.rename(columns={
                        "Date": "date",
                        "OPEN": "open",
                        "HIGH": "high",
                        "LOW": "low",
                        "CLOSE": "close",
                        "VOLUME": "volume",
                        "COMPANY_TICKER": "ticker",
                    }).copy()

                    #parsing the datetime as date
                    load_df['date'] = pd.to_datetime(load_df['date']).dt.date
                    # adding the insert timestamp
                    load_df['insert_datetime'] = batch_ts

                    # restructure the df
                    load_df = load_df[["ticker", "date", "open", "high", "low", "close", "volume", "insert_datetime"]]

                    # insert into the daily bronze
                    load_df.to_sql(
                        name="ohclv_daily_bronze",
                        con=engine,
                        schema=db_name,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=20
                        )

                    # implementing an upsert to prevent adding duplicate values to lineage table
                    rows = load_df.to_dict('records')

                    # upsert sql statement
                    sql_statement = text(

                        f"""
    
                            INSERT INTO {db_name}.ohclv_daily_lineage (ticker, date, open, high, low, close, volume, insert_datetime) VALUES (
                            :ticker, :date, :open, :high, :low, :close, :volume, :insert_datetime
                            ) ON DUPLICATE KEY UPDATE 
                                open =VALUES(open),
                                high =VALUES(high),
                                low =VALUES(low),
                                close =VALUES(close),
                                volume =VALUES(volume),
                                insert_datetime =VALUES(insert_datetime);
                        """

                    )

                    # running the sql statement
                    with engine.begin() as conn:
                        conn.execute(sql_statement,rows)


                    logger.info(f"Loaded the ohclv for the ticker : {ticker}")
                logger.info("Completed the each ticker load into the bronze layer")
            except Exception as e:
                logger.exception(f"Error while processing ohclv load into bronze tables : {e}")

            # <--- Data load for exchange rate --->
            try:
                logger.info("Starting the data load for exchange rate data into the bronze layer tables")

                # API data landing path
                exchange_rate_folder = exchange_root / runtime_year / runtime_month / runtime_date_day

                exchange_files = list(exchange_rate_folder.glob("*.csv"))
                exchange_run_time_file = max(exchange_files, key=os.path.getmtime)

                # loading the df
                exchange_df = pd.read_csv(exchange_run_time_file)

                batch_ts = runtime_start

                load_df = exchange_df.rename(columns={"USD_rate" : "usd_amount"}).copy()
                load_df['date'] = pd.to_datetime(load_df['date']).dt.date
                load_df['insert_datetime'] = batch_ts

                load_df = load_df[[
                    'date','inr_rate','usd_amount','insert_datetime'
                ]]

                # loading the daily data to load
                load_df.to_sql(
                    name="exchange_daily_bronze",
                    con=engine,
                    schema=db_name,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=20
                )

                # upsert logic for the lineage table
                rows = load_df.to_dict('records')
                sql_statement = text(

                    f"""INSERT INTO {db_name}.exchange_daily_lineage (date, inr_rate, usd_amount, insert_datetime) VALUES (
                    
                    :date, :inr_rate, :usd_amount, :insert_datetime
                    
                    ) AS new
                    ON DUPLICATE KEY UPDATE 
                        inr_rate = new.inr_rate,
                        usd_amount = new.usd_amount,
                        insert_datetime = new.insert_datetime;
                    """
                )

                # executing the statement
                with engine.begin() as conn:
                    conn.execute(sql_statement, rows)
                logging.info("loaded the values into the lineage table and the daily loader table for the exchange rate")

            except Exception as e:
                logger.exception(f"Error while processing exchange rate data into bronze tables : {e}")

        except Exception as e:
            logger.exception(f"Error while processing execution in daily load landing to DB : {e}")

        logger.info("Completed the run for daily load into the bronze layer")
        runtime_end = dt.datetime.now()
        logger.info(f"Processed the daily load execution in : {runtime_end - runtime_start}")

    except Exception as e:
        logger.exception(f"Error while executing the daily load process : {e}")

if __name__ == "__main__":
    daily_load()






































