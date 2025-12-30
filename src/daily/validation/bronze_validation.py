from pathlib import Path
import pandas as pd
import logging
from ...utils import load_yml,mysql_connect_create_db,get_engine_session
import argparse
from dotenv import load_dotenv
import os
import datetime as dt
import great_expectations as gx
import json
from great_expectations.exceptions import GreatExpectationsError
from sqlalchemy.exc import SQLAlchemyError
from ...logger import setup_logging

# main execution block

def daily_validation():

    # parsing the arguments from configuration
    parser = argparse.ArgumentParser()
    parser.add_argument('--bulk',default='config/bulk.yaml')
    args = parser.parse_args()

    # calling the parser
    bulk_config = load_yml(args.bulk)

    # loading the configuration options
    db_name = bulk_config['dbname'][0]
    user_name = bulk_config['user_name']
    host = bulk_config['host']
    port = bulk_config['port']

    # loading the dbpass from env
    load_dotenv(dotenv_path='.env')
    db_pass = os.getenv("DB_PASS")

    # setting up the logging configuration
    setup_logging()
    logger = logging.getLogger('daily-validation-execution')

    logger.info("Starting daily validation for the tables loaded....")

    runtime_start = dt.datetime.now()

    try:
        # great expectations context
        context = gx.get_context(mode = 'ephemeral')

        # start a connection to the db
        try:
            mysql_connect_create_db(db_name, user_name, host, port, db_pass, create_flag=False)
            logger.info("successfully connected to the MySql Server....")
        except Exception as e:
            logger.exception("Connection to the MySql Server failed...")
            raise RuntimeError("Cannot connect to the MySql Server...")
        try:
            # Get the engine and the session
            engine, session = get_engine_session(db_name, user_name, host, port, db_pass)

            # connection string builder block
            conn_string = list(str(engine.url).split(":"))
            conn_string[2] = f'{db_pass}@{host}'
            conn_string = ":".join(conn_string)

            logger.info("Successfully connected to bronze database")

            # great expectations configuration
            datasource = context.data_sources.add_sql(name = "daily_load", connection_string=conn_string)

            # <--- Validation block for ohclv data --->

            try:

                logger.info("Starting the validation for ohclv table validation - daily load...")

                ohclv_start_time = dt.datetime.now()

                # Asset definitions
                ohclv_asset = datasource.add_table_asset(name = "ohclv_bronze", table_name = f"{db_name}.ohclv_daily_bronze")

                ohclv_batch_definition = ohclv_asset.add_batch_definition_whole_table(name = "ohclv_bronze") # batch definition passing the whole table

                # getting the whole table as the batch
                ohclv_batch = ohclv_batch_definition.get_batch()

                # validator suite
                ohclv_validator = context.get_validator(batch = ohclv_batch, create_expectation_suite_with_name="daily_ohclv_suit")

                # 1. company tick field validation
                ohclv_validator.expect_column_values_to_not_be_null('ticker')
                ohclv_validator.expect_column_value_lengths_to_be_between('ticker', min_value=1, max_value=7)
                ohclv_validator.expect_column_values_to_match_regex("ticker", r'^[A-Z0-9]{1,7}$')

                # 2. date field validation
                ohclv_validator.expect_column_values_to_not_be_null('date')
                ohclv_validator.expect_column_values_to_be_between(
                    "date",
                    min_value=dt.date.today(),
                    max_value=dt.date.today()
                )

                # 3. open stock value validation
                ohclv_validator.expect_column_values_to_not_be_null("open")
                ohclv_validator.expect_column_values_to_be_between(
                    "open",
                    min_value=0.0,
                    max_value=None,
                    strict_min=True
                )

                # 4. low stock value validation
                ohclv_validator.expect_column_values_to_not_be_null("low")

                ohclv_validator.expect_column_values_to_be_between(
                    "open",
                    min_value=0.0,
                    max_value=None,
                    strict_min=True
                )

                # 5. high stock value validation
                ohclv_validator.expect_column_values_to_not_be_null("high")

                # high ≥ low && high >=open
                ohclv_validator.expect_column_pair_values_A_to_be_greater_than_B(
                    column_A="high",
                    column_B="low",
                    or_equal=True
                )
                ohclv_validator.expect_column_pair_values_A_to_be_greater_than_B(
                    column_A="high",
                    column_B="open",
                    or_equal=True
                )

                # 6. stock volume value validation
                ohclv_validator.expect_column_values_to_not_be_null("volume")

                ohclv_validator.expect_column_values_to_be_between(
                    "volume",
                    min_value=0.0,
                    max_value=None,
                    strict_min=False
                )

                # 7. stock close value validation
                ohclv_validator.expect_column_values_to_not_be_null("close")
                # logic between low <= close <= high
                ohclv_validator.expect_column_pair_values_A_to_be_greater_than_B(
                    column_A="close",
                    column_B="low",
                    or_equal=True
                )

                ohclv_validator.expect_column_pair_values_A_to_be_greater_than_B(
                    column_A="high",
                    column_B="close",
                    or_equal=True
                )

                ohclv_validator.expect_column_values_to_be_between(
                    "volume",
                    min_value=0.0,
                    max_value=None,
                    strict_min=False
                )

                ohclv_result = ohclv_validator.validate().to_json_dict()

                # report file directory
                report_path = Path('reports/bronzeValidation/ohclv_daily')
                report_path.mkdir(parents=True, exist_ok=True)
                # write the report file
                with open(report_path / f'ohclv_bronze_report.json_{dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")}', 'w') as f:
                    json.dump(ohclv_result, f, indent=4)

                logger.info("Completed the Ohclv daily load bronze validation")
                ohclv_end_time = dt.datetime.now()
                logger.info(f"ohclv validation took - {ohclv_end_time - ohclv_start_time}")

            except SQLAlchemyError as db_error:
                logger.exception("Database error while configuring or running GX on bronze ohclv")
            except GreatExpectationsError as gx_err:
                logger.exception("Great Expectations error while validating ohclv bronze")
            except Exception as e:
                logger.exception("Unexpected error in bronze_ohclv_validation")

            try:

                logger.info("Starting the validation for exchange rate validation - daily load...")

                exchg_start_time = dt.datetime.now()

                # Asset definitions
                exchg_asset = datasource.add_table_asset(name = "exchange_bronze", table_name = f"{db_name}.exchange_daily_bronze")

                exchg_batch_definition = exchg_asset.add_batch_definition_whole_table(name = "exchange_bronze") # batch definition passing the whole table

                # getting the whole table as the batch
                exchg_batch = exchg_batch_definition.get_batch()

                # validator suite
                exchg_validator = context.get_validator(batch = exchg_batch, create_expectation_suite_with_name="daily_exchg_suit")


                # 1. date field validation
                exchg_validator.expect_column_values_to_not_be_null('date')
                exchg_validator.expect_column_values_to_be_between(
                    "date",
                    min_value=dt.date.today(),
                    max_value=dt.date.today(),
                )

                # 2. inr rate field validation
                # Not null
                exchg_validator.expect_column_values_to_not_be_null("inr_rate")
                # > 0
                exchg_validator.expect_column_values_to_be_between(
                    "inr_rate",
                    min_value=0,
                    strict_min=True,   # strictly greater than 0
                )
                # Max 3 decimal places – regex
                exchg_validator.expect_column_values_to_match_regex(
                    "inr_rate",
                    regex=r"^\d+(\.\d{1,3})?$",
                )

                # 3. usd amount field validation
                # Not null
                exchg_validator.expect_column_values_to_not_be_null("usd_amount")
                # > 0
                exchg_validator.expect_column_values_to_be_between(
                    "usd_amount",
                    min_value=0,
                    strict_min=True,
                )
                exchg_result = exchg_validator.validate().to_json_dict()

                # report file directory
                report_path = Path('reports/bronzeValidation/daily_exchange_rate')
                report_path.mkdir(parents=True, exist_ok=True)
                # write the report file
                with open(report_path / f'exchange_rate_bronze_report.json_{dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")}', 'w') as f:
                    json.dump(exchg_result, f, indent=4)

                logger.info("Completed the exchange rate daily load bronze validation")
                exchg_end_time = dt.datetime.now()
                logger.info(f"exchange data validation took - {exchg_end_time - exchg_start_time}")

            except SQLAlchemyError as db_error:
                logger.exception("Database error while configuring or running GX on exchange_rate_bronze")
            except GreatExpectationsError as gx_err:
                logger.exception("Great Expectations error while validating exchange_rate_bronze")
            except Exception as e:
                logger.exception("Unexpected error in bronze_exchange_rate_validation")
        except Exception as e:
            logger.exception("Unexpected error while validation process for daily load....")

        logger.info("completed running the expectations on the daily load....")
        runtime_end = dt.datetime.now()
        logger.info(f"Expectations runtime took - {runtime_end - runtime_start}")

    except Exception as e:
        logger.exception("Unexpected error while running great expectations....")

if __name__ == '__main__':
    daily_validation()













