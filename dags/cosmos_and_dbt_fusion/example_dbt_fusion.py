from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeEncryptedPrivateKeyPemProfileMapping

import os
from pathlib import Path
from pendulum import datetime

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_snowflake/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeEncryptedPrivateKeyPemProfileMapping(
        conn_id="SNOWFLAKE_DEFAULT"
    ),
)

_execution_config = ExecutionConfig(
    dbt_executable_path="/home/astro/.local/bin/dbt",
)

_default_args = {
    "retries": 0,
}

example_dbt_fusion = DbtDag(
    # Mandatory DAG parameters
    dag_id="example_dbt_fusion",
    # Mandatory Cosmos parameters
    project_config=_project_config,
    profile_config=_profile_config,
    # Add optional Cosmos parameters as needed, for example
    execution_config=_execution_config,
    # Add optional DAG parameters, for example:
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    default_args=_default_args,
    tags=["out-of-the-box"],
)
