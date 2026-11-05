from dagster import Definitions, define_asset_job, AssetSelection
from dagster_dbt import DbtCliResource

from lakehouse_pipeline.assets.dbt_assets import dbt_project, lakehouse_dbt_assets

# Import our ingestion assets
from lakehouse_pipeline.assets.ingestion import (
    iceberg_schemas,
    raw_taxi_file,
    iceberg_raw_yellow_trips,
)
# Import our Trino Resource
from lakehouse_pipeline.resources.trino_resource import TrinoResource

# 1. Define a Job that runs our current assets
lakehouse_ingestion_pipeline = define_asset_job(
    name="lakehouse_ingestion_pipeline",
    selection=AssetSelection.assets(
        iceberg_schemas, raw_taxi_file, iceberg_raw_yellow_trips
    ),
    description="Run the ingestion pipeline: setup schema → download file → ingest to Iceberg",
)

lakehouse_full_pipeline = define_asset_job(
    name="lakehouse_full_pipeline",
    selection=AssetSelection.all(),
    description="Run ingestion followed by the staging and mart dbt models",
)

# 2. Wire it all together in the Definitions object
defs = Definitions(
    assets=[
        iceberg_schemas,
        raw_taxi_file,
        iceberg_raw_yellow_trips,
        lakehouse_dbt_assets,
    ],
    jobs=[lakehouse_ingestion_pipeline, lakehouse_full_pipeline],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project.project_dir,
            profiles_dir=dbt_project.profiles_dir,
        ),
        # This matches the argument name 'trino_resource' we used in our ingestion.py functions
        "trino_resource": TrinoResource(),
    },
)
