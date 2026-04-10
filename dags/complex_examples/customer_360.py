"""
This dag runs the complex customer_360 dbt project on postgres using
the `DbtTaskGroup` class from Cosmos.
It uses a manifest.json file and precomputed dbt deps to improve parsing and execution times.
"""

from pathlib import Path

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

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA_C360")

DBT_PROJECT_NAME = "customer_360"
DBT_PROJECT_PATH = (
    (Path(__file__).parents[2] / "include" / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
)
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

_execution_config = ExecutionConfig()


_render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,  # only allow manifest loading
    test_behavior=TestBehavior.AFTER_EACH,
)


@dag(tags=["out-of-the-box", "postgres"], default_args={"retries": 2})
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
