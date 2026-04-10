"""
Example using the DbtBuildLocalOperator to build a dbt project in one task.
"""

from pathlib import Path

from airflow.sdk import dag, chain, task
from cosmos import (
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    DbtBuildLocalOperator,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
DB_NAME = os.getenv("POSTGRES_DB", "DEMO_DB")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA_SIMPLEST")

DBT_PROJECT_NAME = "simplest_dbt_project"
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


@dag(tags=["out-of-the-box", "reduce-granularity", "postgres"])
def example_dag_dbtbuildlocal():

    @task
    def pre_dbt():
        pass

    dbt_project = DbtBuildLocalOperator(
        task_id="build_dbt_project",
        project_dir=DBT_PROJECT_PATH,
        profile_config=_profile_config,
        execution_config=_execution_config,
    )

    @task
    def post_dbt():
        pass

    chain(pre_dbt(), dbt_project, post_dbt())


example_dag_dbtbuildlocal()
