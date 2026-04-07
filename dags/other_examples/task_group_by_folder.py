"""
Grouping dbt models into Airflow TaskGroups by their folder structure.

Setting group_nodes_by_folder=True (available since Cosmos 1.14) creates an
Airflow TaskGroup for each folder in your dbt project's models directory.
For jaffle_shop this produces:

    staging/
        stg_customers_run
        stg_orders_run
        stg_payments_run
    customers_run
    orders_run

This reduces visual clutter in the Airflow UI for large dbt projects while
keeping per-model task granularity.
"""

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

import os
from pendulum import datetime

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/include/dbt/jaffle_shop"
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
    group_nodes_by_folder=True,
)

task_group_by_folder = DbtDag(
    dag_id="task_group_by_folder",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    render_config=_render_config,
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    tags=["reduce-granularity", "task-groups", "postgres", "jaffle_shop"],
)
