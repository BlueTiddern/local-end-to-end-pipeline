from dagster import ScheduleDefinition

from .jobs import daily_pipeline_job

daily_noon_schedule = ScheduleDefinition(

    name="daily_noon_cron",
    cron_schedule="30 12 * * *", # 12:30 PM Everyday
    job=daily_pipeline_job,
    execution_timezone="America/New_York", # set to EST/EDT

)