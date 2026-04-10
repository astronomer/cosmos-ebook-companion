"""
This dag runs the jaffle_shop using the new experimental watcher execution mode.
"""

from pathlib import Path

from cosmos import (
    DbtDag,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    TestBehavior,
    RenderConfig,
    ExecutionMode,
    InvocationMode
)
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

import os
from pendulum import datetime

# You need to set this Airflow connection, for an example see the .env_example file in the root of this repository
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

# Adjust this to your own project name, the path to the dbt project and
# the path to the dbt executable if you are using one
DBT_PROJECT_NAME = "jaffle_shop"
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "include" / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
)

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": SCHEMA_NAME},
    ),
)

_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.WATCHER, invocation_mode=InvocationMode.DBT_RUNNER
)


example_dag_watcher_mode = DbtDag(
    # Mandatory DAG parameters
    dag_id="example_dag_watcher_mode",
    # Mandatory Cosmos parameters
    project_config=_project_config,
    profile_config=_profile_config,
    # Add optional Cosmos parameters as needed, for example
    execution_config=_execution_config,
    # Add optional DAG parameters, for example:
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    tags=["watcher", "postgres", "jaffle_shop", "out-of-the-box"],
)
