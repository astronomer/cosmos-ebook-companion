from airflow.sdk import dag, chain, task, Param
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

import os
from pathlib import Path

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

# Resolve path to dbt project relative to this file
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / "inject_dbt_vars").resolve().as_posix()
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

# Only needed if you can't install dbt-postgres in the requirements.txt file
_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


@dag(
    params={
        "my_department": Param(
            "DEFAULT_VALUE",
            type="string",
        ),
    },
    tags=["out-of-the-box"],
)
def example_inject_dbt_vars():

    @task
    def pre_dbt(**context):
        return context["params"]["my_department"]

    _pre_dbt = pre_dbt()

    dbt_project = DbtTaskGroup(
        group_id="dbt_project",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        operator_args={
            "vars": '{"my_department": "{{ ti.xcom_pull(task_ids="pre_dbt") }}"}',
        },
    )

    chain(_pre_dbt, dbt_project)


example_inject_dbt_vars()
