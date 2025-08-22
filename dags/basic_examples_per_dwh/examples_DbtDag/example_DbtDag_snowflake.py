"""
This dag runs the jaffle_shop dbt project on snowflake using
the `DbtDag` class from Cosmos.
"""

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeEncryptedPrivateKeyPemProfileMapping

import os
from pathlib import Path
from pendulum import datetime

# You need to set this Airflow connection, for an example see the .env_example file in the root of this repository
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")

# Adjust this to your own project name, the path to the dbt project and
# the path to the dbt executable if you are using one
DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_snowflake/bin/dbt"

# Only needed if you can't install dbt-snowflake in the requirements.txt file
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
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

_default_args = {
    "retries": 0,
}

example_DbtDag_snowflake = DbtDag(
    # Mandatory DAG parameters
    dag_id="example_DbtDag_snowflake",
    # Mandatory Cosmos parameters
    project_config=_project_config,
    profile_config=_profile_config,
    # Add optional Cosmos parameters as needed, for example
    execution_config=_execution_config,
    # Add optional DAG parameters, for example:
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    default_args=_default_args,
    tags=["basic", "snowflake", "jaffle_shop"],
)
