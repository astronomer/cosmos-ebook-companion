"""
This dag shows how to reduce granularity to improve parsing and execution times 
by rendering parts of the dbt project with DbtBuildLocalOperator (low granularity)
and the DbtTaskGroup class (high granularity).
"""

from airflow.sdk import dag, chain
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
    DbtBuildLocalOperator,
)
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
import os
from pathlib import Path

# You need to set this Airflow connection, for an example see the .env_example file in the root of this repository
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

# Adjust this to your own project name, the path to the dbt project and
# the path to the dbt executable if you are using one
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / "jaffle_shop").resolve().as_posix()
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


@dag(tags=["out-of-the-box", "reduce-granularity"])
def example_reduce_granularity():

    # run all the seeds and staging models without granularity
    _bulk_dbt_project = DbtBuildLocalOperator(
        task_id="bulk_dbt_project",
        project_dir=DBT_PROJECT_PATH,
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=RenderConfig(
            select=[
                "resource_type:seed",
                "path:models/staging",
            ],  # include both, seeds and models in staging
        ),
    )

    # # render the dbt project parts that are of high interest with
    # # a high granularity using the DbtTaskGroup class
    _granular_dbt_project = DbtTaskGroup(
        group_id="granular_dbt_project",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=RenderConfig(
            select=[
                "resource_type:model",
                "resource_type:test",
            ],  # only render models and tests
            exclude=["path:models/staging"],  # exclude staging models
        ),
    )

    chain(_bulk_dbt_project, _granular_dbt_project)


example_reduce_granularity()
