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

def bronze_company_meta_data_validation():

    # logging module setup
    setup_logging()
    logger = logging.getLogger('bronze-validation')

    # --------------------------
    # Allowed value sets for sector and industry
    # --------------------------

    allowed_sectors = [
        "Technology",
        "Consumer Cyclical",
        "Financial Services",
        "Consumer Defensive",
    ]

    allowed_industries = [
        "Consumer Electronics",
        "Internet Content & Information",
        "Semiconductors",
        "Auto - Manufacturers",
        "Software - Infrastructure",
        "Specialty Retail",
        "Financial - Capital Markets",
        "Discount Stores",
        "Banks - Diversified",
        "Software - Application",
        "Information Technology Services",
        "Electronic Gaming & Multimedia",
    ]
    # start time
    runtime_start = dt.datetime.now()

    logger.info("Starting Bronze company meta data Validation....")
    logger.info("Starting a connection to Bronze company_meta_data table....")

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
        data_source = context.data_sources.add_sql(name = "meta_bronze", connection_string=conn_string) # connects to the MySql engine as data source
        data_asset = data_source.add_table_asset(name = "meta_bronze", table_name = "bronze.company_meta_data_bronze") # adding the table as the data asset
        batch_definition = data_asset.add_batch_definition_whole_table(name = "meta_bronze") # batch definition passing the whole table

        # getting the whole table as the batch
        batch = batch_definition.get_batch()

        logger.info("Successfully connected to the Bronze DB table - company_meta_data_bronze....")

        # creating a great expectation suit
        validator = context.get_validator(batch = batch, create_expectation_suite_with_name="bronze_meta_data_suit")

        # 1. Company name expectation
        validator.expect_column_value_lengths_to_be_between(
            "company_name",
            min_value=1,
            max_value=200,
        ) # expected length
        validator.expect_column_values_to_not_be_null("company_name") # cannot be null
        validator.expect_column_values_to_match_regex(
            "company_name",
            regex=r"^(?=.{1,200}$)[A-Za-z0-9,.\-& ]+$",
        )

        # 2. company tick field validation
        validator.expect_column_values_to_not_be_null('ticker')
        validator.expect_column_value_lengths_to_be_between('ticker', min_value=1, max_value=7)
        validator.expect_column_values_to_match_regex("ticker", r'^[A-Z0-9]{1,7}$')

        # 3. price field validation
        validator.expect_column_values_to_be_between(
            "price",
            min_value=0,
            strict_min=True,   # > 0
        )

        # 4. market cap field validation
        validator.expect_column_values_to_not_be_null("market_cap")
        validator.expect_column_values_to_be_between(
            "market_cap",
            min_value=0,
            strict_min=False,  # ≥ 0
        )

        # 5. sector field validation
        validator.expect_column_values_to_not_be_null("sector")
        validator.expect_column_values_to_be_in_set(
            "sector",
            value_set=allowed_sectors,
        )

        # 6. industry field validation
        validator.expect_column_values_to_not_be_null("industry")
        validator.expect_column_values_to_be_in_set(
            "industry",
            value_set=allowed_industries,
        )


        result = validator.validate().to_json_dict()

        # report file directory
        report_path = Path('reports/bronzeValidation/meta')
        report_path.mkdir(parents=True, exist_ok=True)
        # write the report file
        with open(report_path / f'meta_bronze_report.json_{dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")}', 'w') as f:
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
    logger.info("Finished Bronze company metadata Validation....")
    logger.info(f"Runtime in : {runtime_end - runtime_start}") # time difference for the validation runtime


if __name__ == "__main__":
    bronze_company_meta_data_validation()