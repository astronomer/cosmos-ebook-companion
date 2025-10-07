from airflow.sdk import dag, chain, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles.databricks import DatabricksTokenProfileMapping
import os
from pathlib import Path

DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")
CATALOG_NAME = os.getenv("DATABRICKS_CATALOG", "dev_catalog")
SCHEMA_NAME = os.getenv("DATABRICKS_SCHEMA", "dev_schema")
COMPUTE_NAME = os.getenv("DATABRICKS_COMPUTE", "shared_compute")

# Resolve path to dbt project relative to this file
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "dbt" / "jaffle_shop").resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_databricks/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id=DATABRICKS_CONN_ID,
        profile_args={
            "catalog": CATALOG_NAME,
            "schema": SCHEMA_NAME,
            "compute": {
                "type": "serverless",
                "compute_name": COMPUTE_NAME,
            },
        },
    ),
)

_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

_default_args = {
    "retries": 2,
}


@dag(
    max_active_tasks=1,
    tags=["part_1", "databricks"],
)
def example_DbtTaskGroup_databricks():

    @task
    def pre_dbt():
        pass

    dbt_project = DbtTaskGroup(
        group_id="dbt_project",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        default_args=_default_args,
    )

    @task
    def post_dbt():
        pass

    chain(pre_dbt(), dbt_project, post_dbt())


example_DbtTaskGroup_databricks()
