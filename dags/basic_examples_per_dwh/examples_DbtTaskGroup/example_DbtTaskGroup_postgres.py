"""
This dag runs the jaffle_shop dbt project on postgres using
the `DbtTaskGroup` class from Cosmos.
"""

from airflow.sdk import dag, chain, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
from pathlib import Path

# You need to set this Airflow connection, for an example see the .env_example file in the root of this repository
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

# Adjust this to your own project name, the path to the dbt project and
# the path to the dbt executable if you are using one
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "dbt" / "jaffle_shop").resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_postgres/bin/dbt"

# Only needed if you can't install dbt-postgres in the requirements.txt file
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
    "retries": 2,
}


@dag(
    max_active_tasks=1,
    tags=["basic", "postgres", "jaffle_shop", "out-of-the-box"],
)
def example_DbtTaskGroup_postgres():

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


example_DbtTaskGroup_postgres()
