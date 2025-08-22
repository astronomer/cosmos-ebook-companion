from airflow.sdk import dag, task, Asset
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
from pathlib import Path

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

# Resolve path to dbt project relative to this file
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / "simplest_dbt_project").resolve().as_posix()
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


cosmos_assets = DbtDag(
    # Mandatory DAG parameters
    dag_id="cosmos_assets",
    # Mandatory Cosmos parameters
    project_config=_project_config,
    profile_config=_profile_config,
    tags=["assets", "postgres", "out-of-the-box"],
)


@dag(
    schedule=[Asset("postgres://postgres_data:5432/cosmos/DEMO_SCHEMA/model1")],
    tags=["assets", "postgres", "out-of-the-box"],
)
def my_downstream_dag():

    @task
    def placeholder_task():
        pass

    placeholder_task()


my_downstream_dag()
