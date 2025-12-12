import logging
import datetime as dt

def setup_logging():
    """This is the central configuration function to control logging functionality"""

    run_ts = dt.datetime.now().date().strftime("%Y-%m-%d")

    # <--- Pipeline logger configuration ---->

    pipeline_logger = logging.getLogger('pipeline-historical')
    pipeline_logger.setLevel(logging.INFO)
    pipeline_logger.propagate = False

    if not pipeline_logger.handlers:
        handler = logging.FileHandler(f'logs/pipeline/historical/pipeline_{run_ts}.log')
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        pipeline_logger.addHandler(handler)

    # <--- bronze layer validation logger configuration --->
    bronze_logger = logging.getLogger('bronze-validation')
    bronze_logger.setLevel(logging.INFO)
    bronze_logger.propagate = False

    if not bronze_logger.handlers:
        handler = logging.FileHandler(f'logs/validations/bronze/bronze_validation_{run_ts}.log')
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        bronze_logger.addHandler(handler)

    # <--- bronze execution configuration -->
    bronze_execution_logger = logging.getLogger('bronze-execution')
    bronze_execution_logger.setLevel(logging.INFO)
    bronze_execution_logger.propagate = False

    if not bronze_execution_logger.handlers:
        handler = logging.FileHandler(f'logs/executions/bronze/bronze_execution_{run_ts}.log')
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        bronze_execution_logger.addHandler(handler)

    # <--- silver execution configuration -->
    silver_execution_logger = logging.getLogger('silver-execution')
    silver_execution_logger.setLevel(logging.INFO)
    silver_execution_logger.propagate = False

    if not silver_execution_logger.handlers:
        handler = logging.FileHandler(f"logs/executions/silver/silver_execution_{run_ts}.log")
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        silver_execution_logger.addHandler(handler)

    # <--- gold execution configuration --->
    gold_execution_logger = logging.getLogger('gold-execution')
    gold_execution_logger.setLevel(logging.INFO)
    gold_execution_logger.propagate = False

    if not gold_execution_logger.handlers:
        handler = logging.FileHandler(f"logs/executions/gold/gold_execution_{run_ts}.log")
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        gold_execution_logger.addHandler(handler)





