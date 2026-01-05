import os
from dotenv import load_dotenv
import argparse
import pandas as pd
from ...utils import mysql_connect_create_db,get_engine_session, load_yml
from ...logger import setup_logging
import logging
import datetime as dt
from sqlalchemy import text

# main execution block

def daily_transform( bulk: str = "config/bulk.yaml", dagster_run_id: str | None = None ):

    # loading the database password
    load_dotenv(dotenv_path='.env')
    db_pass = os.getenv("DB_PASS")

    # argument parsing - loading the configuration
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--bulk", default = 'config/bulk.yaml')
    # args = parser.parse_args()

    # loading the arguments
    bulk_config = load_yml(bulk)
    db_name = bulk_config['dbname'][0]
    db_name_silver = bulk_config['dbname'][1]
    user_name = bulk_config['user_name']
    host = bulk_config['host']
    port = bulk_config['port']

    # logging configuration
    setup_logging()
    logger = logging.getLogger('daily-execution')

    # logging Dagster run id and timestamp - correlation log
    if dagster_run_id:
        logger.info(f"**dagster_run_id** : {dagster_run_id} -> starting daily transform orchestration")
        logger.info(f"**dagster_log_ts** : {dt.datetime.now().isoformat()} -> starting daily transform marking")

    try:
        # enclosing block to catch the errors
        logger.info("Starting the transformation layer...")

        # runtime start
        runtime_start = dt.datetime.now()

        # start a connection to - MySQL server
        try:
            mysql_connect_create_db(db_name, user_name, host, port, db_pass, create_flag=False)
            logger.info("successfully connected to the MySql Server....")
        except Exception as e:
            logger.exception("Connection to the MySql Server failed...")
            raise RuntimeError("Cannot connect to the MySql Server...")

        # database operations block

        try:

            # getting the database engine for the bronze layer
            engine, session = get_engine_session(db_name, user_name, host, port, db_pass)

            # Block to process the deduplication of ohclv and processing the columns
            try:
                logger.info("Starting the deduplication and the trimming for string values")

                # SQL code block
                with engine.begin() as conn:
                    # drop table
                    conn.execute(text(f"""DROP TABLE IF EXISTS {db_name}.ohclv_daily_processed;"""))

                    logger.info("Successfully dropped the table - ohclv_daily_processed")

                    #query
                    query = text(f"""
                                    CREATE TABLE IF NOT EXISTS {db_name}.ohclv_daily_processed AS
                                    SELECT
                                        ticker,
                                        date,
                                        open,
                                        high,
                                        low,
                                        close,
                                        volume,
                                        insert_datetime                                                                
                                           FROM (SELECT
                                                id,
                                                TRIM(ticker) AS ticker,
                                                date,
                                                open,
                                                high,
                                                low,
                                                close,
                                                volume,
                                                CAST(insert_datetime AS DATE) as insert_datetime,
                                                ROW_NUMBER() OVER(PARTITION BY ticker,date) AS rn
                                            FROM {db_name}.ohclv_daily_bronze) as t
                                    WHERE rn = 1;               
                                    """)
                    conn.execute(query)

                    logger.info("Created the daily processed ohclv table with clean data")

                    # block to insert the records into the silver layer

                    conn.execute(text(f"""
                                    
                                                                            
                                        
                                    INSERT INTO {db_name_silver}.ohclv_silver
                                          (ticker, date, open, high, low, close, volume, insert_datetime)
                                    SELECT
                                          p.ticker,
                                          p.date,
                                          p.open,
                                          p.high,
                                          p.low,
                                          p.close,
                                          p.volume,
                                          p.insert_datetime
                                    FROM {db_name}.ohclv_daily_processed p
                                        ON DUPLICATE KEY UPDATE
                                          open  = VALUES(open),
                                          high  = VALUES(high),
                                          low   = VALUES(low),
                                          close = VALUES(close),
                                          volume = VALUES(volume),
                                          insert_datetime = VALUES(insert_datetime);
                                          
                                          
                                    
                                    """))

                    logger.info("Ran the process to insert and append the record to ohclv silver table")

                    conn.execute(text(f"""

                                        INSERT INTO {db_name_silver}.exchange_rates_silver
                                            (date,inr_rate,usd_amount,insert_datetime)
                                        SELECT
                                            p.date,
                                            p.inr_rate,
                                            p.usd_amount,
                                            CAST(p.insert_datetime AS DATE)
                                        FROM {db_name}.exchange_daily_bronze p
                                            ON DUPLICATE KEY UPDATE
                                            inr_rate = VALUES(inr_rate),
                                            usd_amount = VALUES(usd_amount),
                                            insert_datetime = VALUES(insert_datetime);                                            
                                        
                                    """))

                    logger.info("Ran the process to insert and append the record to exchange silver table")



            except Exception as e:
                logger.exception(f"Error while performing ranking and trimming : {e}")


        except Exception as e:
            logger.exception(f"Error while performing operations on the database: {e}")

    except Exception as e:
        logger.exception(f"Error while processing the transformation layer : {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bulk", default="config/bulk.yaml")
    args = parser.parse_args()
    daily_transform(bulk=args.bulk)
