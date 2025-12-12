import logging
from src.historical.extract import ohclv_extract,company_metadata_extract,exchange_rate_extract,macro_data_extract
from src.historical.load import ohclv_historic,meta_data_historic,exchange_rate_historic,macro_data_historic
from src.bronzeValidation import ohclv,company_meta_data,macro_data,exchange_rate,bronze_layer_validation
from src.historical.transform import bronze_rank_trim,silver_master,silver_load
import pandas as pd
import datetime as dt
import logging
from src.logger import setup_logging

def main():
    """This is the main function that will control all the historical function calls"""

    # logger module configuration
    setup_logging()
    logger = logging.getLogger('pipeline-historical')

    pipeline_start_time = dt.datetime.now()

    # Historical extract execution block

    logger.info("starting Historical ETL pipeline...")
    try:
        extract_start_time = dt.datetime.now()
        logger.info("Starting Historical Extract...")
        try:
            logger.info("Starting OHCLV Extract...")
            ohclv_extract.ohclv_load()
            logger.info("Successfully completed OHCLV Extract...")
        except Exception as e:
            logger.exception("Error in extracting OHCLV data...")
        try:
            logger.info("Starting Metadata Extract...")
            company_metadata_extract.load_metadata()
            logger.info("Successfully completed Metadata Extract...")
        except Exception as e:
            logger.exception("Error in loading Metadata data...")
        try:
            logger.info("Starting Exchange Rate Extract...")
            exchange_rate_extract.load_exchange_rates()
            logger.info("Successfully completed Exchange Rate Extract...")
        except Exception as e:
            logger.exception("Error in loading Exchange Rate data...")
        try:
            logger.info("Starting MacroData Extract...")
            macro_data_extract.load_macro()
            logger.info("Successfully completed MacroData Extract...")
        except Exception as e:
            logger.exception("Error in loading MacroData data...")

        extract_end_time = dt.datetime.now()
        logger.info("Finished Historical Extract...")
        logger.info(f"Extract took: {extract_end_time - extract_start_time}")

        # historical data load block

        load_start_time = dt.datetime.now()
        logger.info("Starting Load Pipeline...")
        try:
            logger.info("Starting OHCLV Load...")
            ohclv_historic.load_ohclv_bronze()
            logger.info("Successfully completed OHCLV Load...")
        except Exception as e:
            logger.exception("Error in loading OHCLV data...")
        try:
            logger.info("Starting Metadata Load...")
            meta_data_historic.load_meta_bronze()
            logger.info("Successfully completed Metadata Load...")
        except Exception as e:
            logger.info("Error in loading Metadata data...")
        try:
            logger.info("Starting Exchange Rate Load...")
            exchange_rate_historic.load_exhange_bronze()
            logger.info("Successfully completed Exchange Rate Load...")
        except Exception as e:
            logger.exception("Error in loading Exchange Rate data...")
        try:
            logger.info("Starting MacroData Load...")
            macro_data_historic.load_macro_bronze()
            logger.info("Successfully completed MacroData Load...")
        except Exception as e:
            logger.exception("Error in loading MacroData data...")

        load_end_time = dt.datetime.now()
        logger.info("Finished Historical Load...")
        logger.info(f"Load took: {load_end_time - load_start_time}")

        # validation code block
        validation_start_time = dt.datetime.now()
        logger.info("Starting Validation Pipeline...")
        try:
            logger.info("Starting OHCLV Validation...")
            ohclv.bronze_ohclv_validation()
            logger.info("Successfully completed OHCLV Validation...")
        except Exception as e:
            logger.exception("Error in validating OHCLV data...")
        try:
            logger.info("Starting Metadata Validation...")
            company_meta_data.bronze_company_meta_data_validation()
            logger.info("Successfully completed Metadata Validation...")
        except Exception as e:
            logger.exception("Error in validating Metadata data...")
        try:
            logger.info("Starting Exchange Rate Validation...")
            exchange_rate.bronze_exchange_rate_validation()
            logger.info("Successfully completed Exchange Rate Validation...")
        except Exception as e:
            logger.exception("Error in validating Exchange Rate data...")
        try:
            logger.info("Starting MacroData Validation...")
            macro_data.bronze_macro_data_validation()
            logger.info("Successfully completed MacroData Validation...")
        except Exception as e:
            logger.exception("Error in validating MacroData data...")
        try:
            logger.info("Starting layer wide statistic count....")
            bronze_layer_validation.bronze_layer_validation()
            logger.info("Successfully completed layer wide statistic count...")
        except Exception as e:
            logger.exception("Error in accumulating layer wide statistic data...")

        validation_end_time = dt.datetime.now()
        logger.info("Finished Historical Validation...")
        logger.info(f"Validation took: {validation_end_time - validation_start_time}")

        # Transformation code block

        transform_start_time = dt.datetime.now()
        logger.info("Starting tansformations on DB...")
        try:
            logger.info("Starting bronze ranking and trimming...")
            bronze_rank_trim.add_rank_trim()
            logger.info("Successfully completed Bronze ranking and trimming on DB...")
        except Exception as e:
            logger.exception("Error in processing Bronze ranking and trimming data...")

        try:
            logger.info("Starting silver master DDL...")
            silver_master.silver_ddl()
            logger.info("Successfully completed Silver master DDL...")
        except Exception as e:
            logger.exception("Error in processing Silver master DDL...")

        try:
            logger.info("Starting Silver layer final setup...")
            silver_load.silver_load()
            logger.info("Successfully completed Silver layer final setup...")
        except Exception as e:
            logger.info("Error in processing Silver layer final setup...")
        transform_end_time = dt.datetime.now()
        logger.info("Finished Historical Transformations on silver layer...")
        logger.info(f"Transformations took: {transform_end_time - transform_start_time}")

        pipeline_end_time = dt.datetime.now()
        logger.info(f"Finished Historical Pipeline in {pipeline_end_time - pipeline_start_time}...")

    except Exception as e:
        logger.exception("Error in processing historic data ETL pipeline...")
        print(e)

if __name__ == '__main__':
    main()





