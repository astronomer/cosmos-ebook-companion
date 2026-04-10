"""
This dag runs the jaffle_shop using watcher execution mode with
tuned performance settings: multiple dbt threads for parallel model
execution, and adjusted poke_interval / timeout on the consumer sensors.
"""

from pathlib import Path

from cosmos import (
    DbtDag,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
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

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={
            "schema": SCHEMA_NAME,
            "threads": 4,
        },
    ),
)

_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.WATCHER,
    invocation_mode=InvocationMode.DBT_RUNNER,
)

watcher_tuned = DbtDag(
    dag_id="watcher_tuned",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    operator_args={
        "poke_interval": 5,
        "timeout": 3600,
    },
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    tags=["watcher", "tuned", "threads", "postgres", "jaffle_shop"],
)
