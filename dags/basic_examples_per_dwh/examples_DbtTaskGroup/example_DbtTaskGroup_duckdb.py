from airflow.sdk import dag, chain, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping
import os
from pathlib import Path

DUCKDB_CONN_ID = os.getenv("DUCKDB_CONN_ID", "duckdb_default")
DB_PATH = "/usr/local/airflow/include/my_duck.duckdb"
SCHEMA_NAME = os.getenv("DUCKDB_SCHEMA", "main")

# Resolve path to dbt project relative to this file
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "dbt" / "jaffle_shop").resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_duckdb/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id=DUCKDB_CONN_ID,
        profile_args={
            "path": DB_PATH,
            "schema": SCHEMA_NAME,
        },
    ),
)

_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

_default_args = {
    "retries": 2,
}


@dag(
    max_active_tasks=1,
    tags=["out-of-the-box", "duckdb"],
)
def example_DbtTaskGroup_duckdb():

    @task
    def pre_dbt():
        pass

    dbt_project = DbtTaskGroup(
        group_id="dbt_project",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        default_args=_default_args,
    )

    @task
    def post_dbt():
        pass

    chain(pre_dbt(), dbt_project, post_dbt())


example_DbtTaskGroup_duckdb()
