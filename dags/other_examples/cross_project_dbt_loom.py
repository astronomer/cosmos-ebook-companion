"""
Cross-project dbt references using dbt-loom and Cosmos (1.13+).

The upstream project exposes staging models as public,
and the downstream project references them via:
    ref('upstream_project', 'stg_customers')

Cosmos handles dbt-loom's external node references by skipping
nodes without file paths during parsing, so only each project's
own models appear as Airflow tasks.

Prerequisites:
- dbt-loom pip-installed in the same environment as dbt
- The upstream project's manifest.json must exist before the
  downstream project can be parsed (run `dbt parse` on upstream first)
"""

from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
)
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

import os
from airflow.sdk import dag, chain
from pendulum import datetime

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")

DBT_ROOT_PATH = f"{os.environ['AIRFLOW_HOME']}/include/dbt"
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_postgres/bin/dbt"

@dag(
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    tags=["dbt-loom", "cross-project", "postgres"],)
def cross_project_dbt_loom():

    upstream_task_group = DbtTaskGroup(
        group_id="upstream_project",
        project_config=ProjectConfig(
            dbt_project_path=f"{DBT_ROOT_PATH}/upstream_project",
        ),
        profile_config=ProfileConfig(
            profile_name="upstream_project",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id=POSTGRES_CONN_ID,
                profile_args={"schema": "upstream"},
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
    )

    downstream_task_group = DbtTaskGroup(
        group_id="downstream_project",
        project_config=ProjectConfig(
            dbt_project_path=f"{DBT_ROOT_PATH}/downstream_project",
        ),
        profile_config=ProfileConfig(
            profile_name="downstream_project",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id=POSTGRES_CONN_ID,
                profile_args={"schema": "downstream"},
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
    )

    chain(upstream_task_group, downstream_task_group)


cross_project_dbt_loom()