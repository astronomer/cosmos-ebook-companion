from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping

import os
from pathlib import Path
from pendulum import datetime

DUCKDB_CONN_ID = os.getenv("DUCKDB_CONN_ID", "duckdb_default")
DB_PATH = "/usr/local/airflow/include/my_duck.duckdb"
SCHEMA_NAME = os.getenv("DUCKDB_SCHEMA", "main")

DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
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
    "retries": 0,
}

example_DbtDag_duckdb = DbtDag(
    # Mandatory DAG parameters
    dag_id="example_DbtDag_duckdb",
    # Mandatory Cosmos parameters
    project_config=_project_config,
    profile_config=_profile_config,
    # Add optional Cosmos parameters as needed, for example
    execution_config=_execution_config,
    # Add optional DAG parameters, for example:
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    default_args=_default_args,
    max_active_tasks=1,
) 