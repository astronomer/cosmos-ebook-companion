"""
Using YAML selectors to select dbt models by a named selector.

YAML selectors let you define reusable named selectors in your dbt project's
selectors.yml and reference them by name in your Cosmos DAG.

This example references a `staging_only` selector defined in selectors.yml:

    selectors:
      - name: staging_only
        description: "Select only the staging models (views)"
        definition:
          method: path
          value: models/staging

With dbt_ls load mode, dbt resolves the selector directly.
Since Cosmos 1.13, YAML selectors are also supported with
LoadMode.DBT_MANIFEST, where Cosmos resolves them from the manifest at parse
time without invoking dbt.
"""

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

import os
from pendulum import datetime

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/include/dbt/jaffle_shop"
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

_render_config = RenderConfig(
    selector="staging_only",
)

yaml_selectors = DbtDag(
    dag_id="yaml_selectors",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    render_config=_render_config,
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    tags=["selectors", "yaml", "postgres", "jaffle_shop"],
)
