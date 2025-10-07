from airflow.sdk import dag, chain, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles.spark import SparkThriftProfileMapping
import os
from pathlib import Path

SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark_default")
DB_NAME = os.getenv("SPARK_DATABASE", "dev_database")
SCHEMA_NAME = os.getenv("SPARK_SCHEMA", "dev_schema")

# Resolve path to dbt project relative to this file
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "dbt" / "jaffle_shop").resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_spark/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SparkThriftProfileMapping(
        conn_id=SPARK_CONN_ID,
        profile_args={
            "schema": SCHEMA_NAME,
            "method": "thrift",
            "cluster": "spark-cluster",
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
    tags=["out-of-the-box", "spark"],
)
def example_DbtTaskGroup_spark():

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


example_DbtTaskGroup_spark()
