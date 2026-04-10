"""
Selecting dbt models by their fully qualified name (FQN).

FQN selection lets you pinpoint models using their full dbt path:

    jaffle_shop.customers           → the customers mart model
    jaffle_shop.staging.stg_orders  → a specific staging model

This example selects the `customers` model together with all of its upstream
dependencies using the `+` graph operator, which results in:
    stg_customers, stg_orders, stg_payments → customers

FQN selectors work with any load mode. Since Cosmos 1.14 they are also
supported with LoadMode.DBT_MANIFEST.
"""

from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
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
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

_render_config = RenderConfig(
    select=["+fqn:jaffle_shop.customers"],
)

fqn_selectors = DbtDag(
    dag_id="fqn_selectors",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    render_config=_render_config,
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    tags=["selectors", "fqn", "postgres", "jaffle_shop"],
)
