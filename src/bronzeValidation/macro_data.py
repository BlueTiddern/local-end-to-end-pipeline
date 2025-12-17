from pathlib import Path
import pandas as pd
import logging
from ..utils import load_yml,mysql_connect_create_db,get_engine_session
import argparse
from dotenv import load_dotenv
import os
import datetime as dt
import great_expectations as gx
import json
from great_expectations.exceptions import GreatExpectationsError
from sqlalchemy.exc import SQLAlchemyError
from ..logger import setup_logging

# loading environment variables
load_dotenv(dotenv_path=".env")

#data base password
db_pass = os.getenv("DB_PASS")

# initializing argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--bulk", default = "config/bulk.yaml")
args = parser.parse_args()

# great expectations context
context = gx.get_context(mode = 'ephemeral')

def bronze_macro_data_validation():

    # start time
    runtime_start = dt.datetime.now()
    current_year = dt.datetime.now().year

    # logging module setup
    setup_logging()
    logger = logging.getLogger('bronze-validation')

    logger.info("Starting Bronze macro data Validation....")
    logger.info("Starting a connection to Bronze macro data....")

    # getting the project config files
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
    try:
        # Get the engine and the session
        engine, session = get_engine_session(dbname, username, host, port, db_pass)

        # connection string builder block
        conn_string = list(str(engine.url).split(":"))
        conn_string[2] = f'{db_pass}@{host}'
        conn_string = ":".join(conn_string)

        # great expectations data connection block
        data_source = context.data_sources.add_sql(name = "macro_bronze", connection_string=conn_string) # connects to the MySql engine as data source
        data_asset = data_source.add_table_asset(name = "macro_bronze", table_name = "bronze.macro_economic_data_bronze") # adding the table as the data asset
        batch_definition = data_asset.add_batch_definition_whole_table(name = "macro_bronze") # batch definition passing the whole table

        # getting the whole table as the batch
        batch = batch_definition.get_batch()

        logger.info("Successfully connected to the Bronze DB table - macro_economic_data_bronze....")

        # creating a great expectation suit
        validator = context.get_validator(batch = batch, create_expectation_suite_with_name="bronze_macro_suit")

        # 1. country id field validation
        validator.expect_column_values_to_not_be_null("country_id")

        validator.expect_column_value_lengths_to_be_between(
            "country_id",
            min_value=2,
            max_value=10,
        )

        # 2. year field validation
        validator.expect_column_values_to_not_be_null("year")
        validator.expect_column_values_to_be_between(
            "year",
            min_value=2020,
            max_value=current_year,
        )

        # 3. country name field validation
        validator.expect_column_values_to_not_be_null("country_name")
        validator.expect_column_value_lengths_to_be_between(
            "country_name",
            min_value=1,
            max_value=200
        )
        validator.expect_column_values_to_not_match_regex(
            "country_name",
            regex=r"^\s*$"
        )


        # 4. nominal gdp field validation

        validator.expect_column_values_to_be_between(
            "nominal_gdp",
            min_value=0,
            strict_min=False
        )

        # 5. real gdp field validation

        validator.expect_column_values_to_be_between(
            "real_gdp",
            min_value=0,
            strict_min=False
        )

        # 6. inflation field validation

        validator.expect_column_values_to_be_between(
            "inflation",
            min_value=-50,
            max_value=20000,
        )

        # 7. unemployment field validation

        validator.expect_column_values_to_be_between(
            "unemployment",
            min_value=0,
            max_value=100,
            strict_min=False,
            strict_max=False,
        )

        result = validator.validate().to_json_dict()

        # report file directory
        report_path = Path('reports/bronzeValidation/macro_data')
        report_path.mkdir(parents=True, exist_ok=True)
        # write the report file
        with open(report_path / f'macro_data_bronze_report.json_{dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")}', 'w') as f:
            json.dump(result, f, indent=4)
    except SQLAlchemyError as db_error:
        logger.exception("Database error while configuring or running GX on exchange_rate_bronze")
    except GreatExpectationsError as gx_err:
        logger.exception("Great Expectations error while validating exchange_rate_bronze")
    except Exception as e:
        logger.exception("Unexpected error in bronze_exchange_rate_validation")

    # end time
    runtime_end = dt.datetime.now()

    # closing logs
    logger.info("Finished Bronze macro data Validation....")
    logger.info(f"Runtime in : {runtime_end - runtime_start}") # time difference for the validation runtime


if __name__ == "__main__":
    bronze_macro_data_validation()