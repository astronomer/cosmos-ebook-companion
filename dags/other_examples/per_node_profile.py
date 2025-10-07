"""
This dag runs a simple dbt project with the main connection being the default
one provided in the ProfileConfig and another connection 
(my_other_postgres_connection) being used on the level of
an individual node in the dbt project:

Snippet from the dbt project schema.yml file:

models:
  - name: model1
    description: This is a test model
    meta:
      cosmos:
        operator_kwargs:
            retries: 10
        profile_config:
          profile_name: my_other_profile
          target_name: prod
          profile_mapping:
            conn_id: my_other_postgres_connection
"""

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

import os
from pathlib import Path

POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "profiles_per_node")
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
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


per_node_profile = DbtDag(
    # Mandatory DAG parameters
    dag_id="per_node_profile",
    # Mandatory Cosmos parameters
    project_config=_project_config,
    profile_config=_profile_config,
    # Add optional Cosmos parameters as needed, for example
    execution_config=_execution_config,
    tags=["out-of-the-box", "per-node-profile"],
)
