from dagster import AssetExecutionContext, asset

from ..daily.extract.daily_extract import daily_extr
from ..daily.load.daily_load import daily_load
from ..daily.validation.bronze_validation import daily_validation
from ..daily.transform.daily_transform import daily_transform

DEFAULT_CONFIG = "config/bulk.yaml"

@asset(name="extract_daily")
def extract_daily(context: AssetExecutionContext) -> None:
    #passing in the context run id and calling the daily_extract
    daily_extr(bulk=DEFAULT_CONFIG, dagster_run_id=context.run_id)

@asset(name="load_daily", deps=[extract_daily])
def load_daily(context: AssetExecutionContext) -> None:
    # added dependency to load and passing the context
    daily_load(bulk=DEFAULT_CONFIG, dagster_run_id=context.run_id)

@asset(name="validate_daily", deps=[load_daily])
def validate_daily(context: AssetExecutionContext) -> None:
    daily_validation(bulk=DEFAULT_CONFIG, dagster_run_id=context.run_id)

@asset(name="transform_daily", deps=[validate_daily])
def transform_daily(context: AssetExecutionContext) -> None:
    daily_transform(bulk=DEFAULT_CONFIG, dagster_run_id=context.run_id)

    