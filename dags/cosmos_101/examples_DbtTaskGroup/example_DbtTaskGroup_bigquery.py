from airflow.sdk import dag, chain, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
import os
from pathlib import Path

BIGQUERY_CONN_ID = os.getenv("BIGQUERY_CONN_ID", "bigquery_default")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "my-project")
DATASET_NAME = os.getenv("BIGQUERY_DATASET", "dev_dataset")
BIGQUERY_KEYFILE_PATH = os.getenv(
    "BIGQUERY_KEYFILE_PATH", "/usr/local/airflow/include/secrets/bigquery-key.json"
)

# Resolve path to dbt project relative to this file
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "dbt" / "jaffle_shop").resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_bigquery/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id=BIGQUERY_CONN_ID,
        profile_args={
            "project": PROJECT_ID,
            "dataset": DATASET_NAME,
            "keyfile": BIGQUERY_KEYFILE_PATH,
        },
    ),
)

_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


_default_args = {
    "retries": 2,
}


@dag(max_active_tasks=1)
def example_DbtTaskGroup_bigquery():

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


example_DbtTaskGroup_bigquery()
