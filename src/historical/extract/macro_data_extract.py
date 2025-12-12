"""


Macro economics data : GDP, inflation, interest rates and unemployment trends

["countryname","id","year","nGDP","rGDP","infl","unemp","strate","ltrate"]


"""

from global_macro_data import gmd
import pandas as pd
from ...utils import load_yml,make_dir
from pathlib import Path
from argparse import ArgumentParser
import datetime as dt
import logging
import io
from contextlib import redirect_stdout
from ...logger import setup_logging


def load_macro():

    # runtime start for macro data extract
    runtime_start = dt.datetime.now()

    # logging configuration
    setup_logging()
    logger = logging.getLogger("bronze-execution")
    logger.info("Starting data extraction from the Global Macro data API....")

    parser = ArgumentParser()
    parser.add_argument("--bulk", default="config/bulk.yaml")
    args = parser.parse_args()

    bulk_config = load_yml(args.bulk)

    # list of macro variables
    macro_variables = bulk_config["macro_variables"]
    start_year = int(bulk_config["start_date"].split("-")[0])
    end_year = int(bulk_config["end_date"].split("-")[0])

    # creating the folder for historical data ingestion
    macro_file_path = Path(bulk_config["macro_data_root"])

    runtime_datetime = dt.datetime.now()
    runtime_year = str(runtime_datetime.year)
    runtime_month = runtime_datetime.strftime("%b")
    runtime_date = runtime_datetime.strftime("%d")
    runtime_time = runtime_datetime.time().strftime("%H-%M-%S")

    with io.StringIO() as buf,redirect_stdout(buf):
        macro_df = gmd(variables=macro_variables)
        output = buf.getvalue().strip().split("\n")
    logger.info(f"{output[0]}")
    logger.info(f"{output[1]}")
    logger.info(f"{output[2]}")

    if not macro_df.empty:
        logger.info("Data from GMD API loaded successfully....")

    # applying the year filer
    filtered_macro_df = macro_df[(macro_df["year"] >= start_year) & (macro_df["year"] <= end_year)]

    filtered_macro_df = filtered_macro_df.rename(columns={"nGDP": "NOMINAL_GDP","rGDP": "REAL_GDP","infl": "INFLATION","unemp": "UNEMPLOYMENT"})


    runtime_filepath = macro_file_path / runtime_year / runtime_month / runtime_date
    make_dir(runtime_filepath)
    filtered_macro_df.to_csv(runtime_filepath / f"macro_data_historic_{runtime_time}.csv", index=False)
    logger.info("Data successfully extracted and loaded into landing path....")
    #end time macro data extract
    runtime_end = dt.datetime.now()
    runtime = str(runtime_end - runtime_start)
    logger.info(f"Runtime in : {runtime}")


if __name__ == "__main__":
    load_macro()







