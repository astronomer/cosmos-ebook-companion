"""
At the time of writing hosting dbt docs in Airflow 3 is not
supported yet.
"""

from airflow.decorators import dag
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.operators import DbtDocsS3Operator

import os
from pathlib import Path

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
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

_default_args = {
    "retries": 0,
}


@dag(
    schedule=None,
    start_date=None,
    catchup=False,
    tags=["dbt_docs", "Airflow 2"],
)
def example_dbt_docs():

    _dbt_project = DbtTaskGroup(
        group_id="dbt_project",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        default_args=_default_args,
    )

    _generate_dbt_docs_aws = DbtDocsS3Operator(
        task_id="generate_dbt_docs_aws",
        project_dir=DBT_PROJECT_PATH,
        profile_config=_profile_config,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        # docs-specific arguments
        connection_id="aws_default",
        bucket_name="<your-bucket-name>",
        folder_dir="dbt_docs",
    )

    _dbt_project >> _generate_dbt_docs_aws


example_dbt_docs()
