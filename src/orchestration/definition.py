from dagster import Definitions

from .assets import extract_daily,load_daily,validate_daily,transform_daily
from .jobs import daily_pipeline_job
from .schedules import daily_noon_schedule

definitions = Definitions(

    assets = [extract_daily,load_daily,validate_daily,transform_daily],
    jobs = [daily_pipeline_job],
    schedules = [daily_noon_schedule],

)