from dagster import AssetSelection, define_asset_job

daily_pipeline_job = define_asset_job(

    name="daily_pipeline",
    selection = AssetSelection.assets(
        "extract_daily",
        "load_daily",
        "validate_daily",
        "transform_daily",
    ),

)