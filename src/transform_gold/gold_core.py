import argparse
import datetime as dt
from ..utils import mysql_connect_create_db,get_engine_session,load_yml
from sqlalchemy import text
from dotenv import load_dotenv
import os
import logging
from ..logger import setup_logging

# loading the database password
load_dotenv(dotenv_path='.env')
db_pass = os.getenv('DB_PASS')

# argument parser setup
parser = argparse.ArgumentParser()
parser.add_argument("--bulk", default = 'config/bulk.yaml')
args = parser.parse_args()
bulk_config = load_yml(args.bulk)

def gold_exec():

    #logging configuration
    setup_logging()
    logger = logging.getLogger('gold-execution')

    #starting execution
    runtime_start = dt.datetime.now()
    logger.info("Starting Gold layer set up for business logic....")

    # configuration arguments
    db_name = bulk_config["dbname"][2]
    db_silver = bulk_config["dbname"][1]
    user_name = bulk_config["user_name"]
    host = bulk_config["host"]
    port = bulk_config["port"]

    # start a connection to - MySQL server
    try:
        mysql_connect_create_db(db_name, user_name, host, port, db_pass, create_flag=True)
        logger.info("successfully connected to the MySql Server....")
    except Exception as e:
        logger.exception("Connection to the MySql Server failed...")
        raise RuntimeError("Cannot connect to the MySql Server...")

    try:
        engine,_ = get_engine_session(db_name, user_name,host, port, db_pass)
        logger.info("successfully created engine for using gold layer....")

        # <----  STOCK FACTS BLOCK  ---->
        try:
            with engine.begin() as conn:
                conn.execute(text(f"""
                
                CREATE OR REPLACE VIEW {db_name}.stock_facts AS
                
                    WITH base_returns_data AS (
                    
                        SELECT
                            stock_id AS UUID,
                            CONCAT('STK_',ticker) AS stock_id,
                            ticker,
                            date AS trade_date,
                            open AS open_price,
                            high AS high_price,
                            low AS low_price,
                            close AS close_price,
                            volume AS stock_volume,
                            LAG(close) OVER(PARTITION BY ticker ORDER BY date) AS prev_close_price
                        FROM {db_silver}.ohclv_silver
                    ),
                    
                    metrics_calculation AS (
                    
                        SELECT
                            UUID,
                            stock_id,
                            ticker,
                            trade_date,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                            stock_volume,
                            prev_close_price,
                            
                            -- calculating daily return
                            
                            CASE
                                WHEN prev_close_price IS NULL THEN NULL
                                ELSE (close_price - prev_close_price) / prev_close_price
                            END AS daily_return,
                            
                            -- calculating 30 day return : volatitlity ( standard deviation )
                            
                            STDDEV_SAMP(
                                CASE
                                    WHEN prev_close_price IS NULL THEN NULL
                                    ELSE (close_price - prev_close_price) / prev_close_price
                                END) 
                            OVER(
                            
                            PARTITION BY ticker 
                            ORDER BY trade_date 
                            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                            
                        ) AS volatility_days_30,
                        
                        -- calculating the 90-day rolling average
                        AVG(close_price)
                        OVER(
                        
                            PARTITION BY ticker
                            ORDER BY trade_date 
                            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
                        
                        ) AS stock_90_day_average
                    FROM base_returns_data
                    )
                    
                    SELECT * FROM metrics_calculation
                
                """))
            logger.info("Successfully created view for stocks facts in gold layer....")

        except Exception as e:
            logger.exception("Error processing the view for stock facts....")

        # <----  MACRO INDICATORS FACTS BLOCK  ---->
        try:
            with engine.begin() as conn:
                conn.execute(text(f"""
                    
                    CREATE OR REPLACE VIEW {db_name}.macro_facts AS

                        WITH base_lead_lag AS (
                        
                            SELECT 
                                data_id AS UUID,
                                country_code,
                                country_name,
                                year AS data_point_year,
                                nominal_gdp,
                                real_gdp,
                                inflation,
                                unemployment,
                                
                                -- getting the window values
                                
                                LAG(nominal_gdp) OVER(PARTITION BY country_code ORDER BY year) AS prev_nominal_gdp,
                                LAG(real_gdp) OVER(PARTITION BY country_code ORDER BY year) AS prev_real_gdp,
                                LAG(inflation) OVER(PARTITION BY country_code ORDER BY year) AS prev_inflation,
                                LAG(unemployment) OVER(PARTITION BY country_code ORDER BY year) AS prev_unemployment
                                
                            FROM {db_silver}.macro_economic_data_silver
                        
                        ),
                        
                        calculation AS (
                        
                            SELECT
                                UUID,
                                country_code,
                                country_name,
                                data_point_year,
                                nominal_gdp,
                                real_gdp,
                                inflation,
                                unemployment,
                                
                                -- GDP growth rate of change calculations : nominal and real GDP
                                
                                CASE
                                    WHEN prev_nominal_gdp IS NULL OR prev_nominal_gdp = 0 THEN NULL
                                    ELSE (nominal_gdp - prev_nominal_gdp) / prev_nominal_gdp
                                END AS year_on_year_gdp_change,
                                
                                CASE
                                    WHEN prev_real_gdp IS NULL OR prev_real_gdp = 0 THEN NULL
                                    ELSE (real_gdp - prev_real_gdp) / prev_real_gdp
                                END AS year_on_real_gdp_change,
                                
                                -- Inflation acceleration/decceleration calculation
                                
                                CASE
                                    WHEN prev_inflation IS NULL OR prev_inflation = 0 THEN NULL
                                    ELSE (inflation - prev_inflation)
                                END AS inflation_change_year_on_year,
                                
                                -- Unemployment rate of change calculation
                                
                                CASE
                                    WHEN prev_inflation IS NULL OR prev_inflation = 0 THEN NULL
                                    ELSE (inflation - prev_inflation)
                                END AS unemployment_change_year_on_year
                            
                            FROM base_lead_lag
                        )
                        
                        SELECT * FROM calculation
                
                """))
            logger.info("Successfully created view for macro facts in gold layer....")
        except Exception as e:
            logger.exception("Error processing the view for macro facts....")



        runtime_end = dt.datetime.now()
        logger.info("Finished Gold layer set up for business logic....")
        logger.info(f"Runtime in - {runtime_end - runtime_start}")

    except Exception as e:
        logger.exception("Failed to work with gold engine for using gold layer....")
        print(e)

if __name__ == "__main__":
    gold_exec()
