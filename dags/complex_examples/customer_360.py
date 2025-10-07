"""
This dag runs the complex customer_360 dbt project on postgres using
the `DbtTaskGroup` class from Cosmos.
It uses a manifest.json file and precomputed dbt deps to improve parsing and execution times.
"""

from airflow.sdk import dag, chain, task
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
    LoadMode,
    TestBehavior,
)
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
import os
from pathlib import Path

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

# Resolve path to dbt project relative to this file
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / "customer_360").resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_postgres/bin/dbt"
DBT_MANIFEST_PATH = f"{DBT_PROJECT_PATH}/target/manifest.json"

# using a manifest.json file and precomputed dbt deps
_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    manifest_path=DBT_MANIFEST_PATH,
    install_dbt_deps=False,
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


_render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,  # only allow manifest loading
    test_behavior=TestBehavior.BUILD,  # render one task for a model and its test
)


@dag(tags=["out-of-the-box"])
def customer_360():

    @task
    def pre_dbt():
        pass

    dbt_project = DbtTaskGroup(
        group_id="dbt_project",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=_render_config,
    )

    @task
    def post_dbt():
        pass

    chain(pre_dbt(), dbt_project, post_dbt())


customer_360()
