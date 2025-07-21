from airflow.sdk import dag, chain, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeEncryptedPrivateKeyPemProfileMapping
import os
from pathlib import Path

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
DB_NAME = os.getenv("SNOWFLAKE_DATABASE", "DEMO_DB")
SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA", "DEMO_SCHEMA")
WAREHOUSE_NAME = os.getenv("SNOWFLAKE_WAREHOUSE", "HUMANS")

# Resolve path to dbt project relative to this file
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "dbt" / "jaffle_shop").resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_snowflake/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeEncryptedPrivateKeyPemProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID
    ),
)

_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

_default_args = {
    "retries": 2,
}


@dag(max_active_tasks=1)
def example_DbtTaskGroup_snowflake():

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


example_DbtTaskGroup_snowflake()
