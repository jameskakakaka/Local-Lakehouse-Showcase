from pathlib import Path
from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

dbt_project = DbtProject(
    project_dir=REPOSITORY_ROOT / "dbt_project",
    profiles_dir=REPOSITORY_ROOT / "lakehouse_pipeline" / "trino",
)

# Refresh the manifest during `dagster dev`; otherwise load the existing artifact.
dbt_project.prepare_if_dev()
if not dbt_project.manifest_path.is_file():
    raise FileNotFoundError(
        "Missing dbt manifest. From the repository root run: "
        "dbt parse --project-dir dbt_project "
        "--profiles-dir lakehouse_pipeline/trino"
    )

# Custom translator to group all dbt models neatly in the Dagster UI
class CustomDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if (
            dbt_resource_props["resource_type"] == "source"
            and dbt_resource_props["source_name"] == "raw_taxi"
            and dbt_resource_props["name"] == "yellow_trips"
        ):
            return AssetKey(["raw", "yellow_trips"])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        return "transforms"

@dbt_assets(
    manifest=dbt_project.manifest_path, 
    dagster_dbt_translator=CustomDbtTranslator()
)
def lakehouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Executes the dbt models (Silver and Gold layers) inside Trino.
    """
    yield from dbt.cli(["build"], context=context).stream()
