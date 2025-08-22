"""
This dag runs the jaffle_shop dbt project on databricks using
the `DbtDag` class from Cosmos.
"""

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import DatabricksTokenProfileMapping
import os
from pathlib import Path
from pendulum import datetime

# You need to set this Airflow connection, for an example see the .env_example file in the root of this repository
DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")
# Use your own values for the catalog, schema, compute type and compute names
CATALOG_NAME = os.getenv("DATABRICKS_CATALOG", "dev_catalog")
SCHEMA_NAME = os.getenv("DATABRICKS_SCHEMA", "dev_schema")
COMPUTE_NAME = os.getenv("DATABRICKS_COMPUTE", "shared_compute")
COMPUTE_TYPE = os.getenv("DATABRICKS_COMPUTE_TYPE", "serverless")

# Adjust this to your own project name, the path to the dbt project and
# the path to the dbt executable if you are using one
DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
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
                "type": COMPUTE_TYPE,
                "compute_name": COMPUTE_NAME,
            },
        },
    ),
)

# Only needed if you can't install dbt-databricks in the requirements.txt file
_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

_default_args = {
    "retries": 2,
}


example_DbtDag_databricks = DbtDag(
    # Mandatory DAG parameters
    dag_id="example_DbtDag_databricks",
    # Mandatory Cosmos parameters
    project_config=_project_config,
    profile_config=_profile_config,
    # Add optional Cosmos parameters as needed, for example
    execution_config=_execution_config,
    # Add optional DAG parameters, for example:
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    default_args=_default_args,
    tags=["basic", "databricks", "jaffle_shop"],
)
