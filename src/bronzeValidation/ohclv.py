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

# loading environment variables
load_dotenv(dotenv_path=".env")

#data base password
db_pass = os.getenv("DB_PASS")

# initializing argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--bulk", default = "config/bulk.yaml")
args = parser.parse_args()

# setup custom logger
logger = logging.getLogger("validations")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("logs/validations/bronze/bronze_validation.log")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

# great expectations context
context = gx.get_context(mode = 'ephemeral')

def bronze_ohclv_validation():

    # start time
    runtime_start = dt.datetime.now()

    logger.info("Starting Bronze OHCLV Validation....")
    logger.info("Starting a connection to Bronze OHCLV....")

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
        data_source = context.data_sources.add_sql(name = "ohclv_bronze", connection_string=conn_string) # connects to the MySql engine as data source
        data_asset = data_source.add_table_asset(name = "ohclv_bronze", table_name = "bronze.ohclv_bronze") # adding the table as the data asset
        batch_definition = data_asset.add_batch_definition_whole_table(name = "ohclv_bronze") # batch definition passing the whole table

        # getting the whole table as the batch
        batch = batch_definition.get_batch()

        logger.info("Successfully connected to the Bronze DB table - ohclv_bronze....")

        # creating a great expectation suit
        validator = context.get_validator(batch = batch, create_expectation_suite_with_name="bronze_ohclv_suit")

        # 1. company tick field validation
        validator.expect_column_values_to_not_be_null('ticker')
        validator.expect_column_value_lengths_to_be_between('ticker', min_value=1, max_value=7)
        validator.expect_column_values_to_match_regex("ticker", r'^[A-Z0-9]{1,7}$')

        # 2. date field validation
        validator.expect_column_values_to_not_be_null('date')
        validator.expect_column_values_to_be_between(
            "date",
            min_value=dt.datetime(2000, 1, 1),
            max_value=dt.datetime.today()
        )

        # 3. open stock value validation
        validator.expect_column_values_to_not_be_null("open")
        validator.expect_column_values_to_be_between(
            "open",
            min_value=0.0,
            max_value=None,
            strict_min=True
        )

        # 4. low stock value validation
        validator.expect_column_values_to_not_be_null("low")

        validator.expect_column_values_to_be_between(
            "open",
            min_value=0.0,
            max_value=None,
            strict_min=True
        )

        # 5. high stock value validation
        validator.expect_column_values_to_not_be_null("high")

        # high ≥ low && high >=open
        validator.expect_column_pair_values_A_to_be_greater_than_B(
            column_A="high",
            column_B="low",
            or_equal=True
        )
        validator.expect_column_pair_values_A_to_be_greater_than_B(
            column_A="high",
            column_B="open",
            or_equal=True
        )


        # 6. stock volume value validation
        validator.expect_column_values_to_not_be_null("volume")

        validator.expect_column_values_to_be_between(
            "volume",
            min_value=0.0,
            max_value=None,
            strict_min=False
        )

        # 7. stock close value validation
        validator.expect_column_values_to_not_be_null("close")
        # logic between low <= close <= high
        validator.expect_column_pair_values_A_to_be_greater_than_B(
            column_A="close",
            column_B="low",
            or_equal=True
        )
        validator.expect_column_pair_values_A_to_be_greater_than_B(
            column_A="high",
            column_B="close",
            or_equal=True
        )

        validator.expect_column_values_to_be_between(
            "volume",
            min_value=0.0,
            max_value=None,
            strict_min=False
        )

        result = validator.validate().to_json_dict()



        # report file directory
        report_path = Path('reports/bronzeValidation/ohclv')
        report_path.mkdir(parents=True, exist_ok=True)
        # write the report file
        with open(report_path / f'ohclv_bronze_report.json_{dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")}', 'w') as f:
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
    logger.info("Finished Bronze OHCLV Validation....")
    logger.info(f"Runtime in : {runtime_end - runtime_start}") # time difference for the validation runtime


if __name__ == "__main__":
    bronze_ohclv_validation()

