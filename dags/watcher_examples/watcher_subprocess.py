"""
This dag runs the jaffle_shop using watcher execution mode
with subprocess invocation mode, for cases where dbt is installed
in a separate virtual environment from Airflow.
"""

from pathlib import Path

from cosmos import (
    DbtDag,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
    ExecutionMode,
    InvocationMode,
)
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

import os
from pendulum import datetime

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

DBT_PROJECT_NAME = "jaffle_shop"
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "include" / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_postgres/bin/dbt"

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
    execution_mode=ExecutionMode.WATCHER,
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

watcher_subprocess = DbtDag(
    dag_id="watcher_subprocess",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    tags=["watcher", "subprocess", "postgres", "jaffle_shop"],
)
