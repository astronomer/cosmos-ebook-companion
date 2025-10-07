"""
Example showcasing how to use the `async` execution mode for a Cosmos dbt project with BigQuery.
"""

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode
from cosmos.profiles.bigquery import GoogleCloudServiceAccountFileProfileMapping

import os
from pathlib import Path

BIGQUERY_CONN_ID = os.getenv("BIGQUERY_CONN_ID", "bigquery_default")

DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "async_dbt_project")
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_bigquery/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id=BIGQUERY_CONN_ID
    ),
)
_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
    execution_mode=ExecutionMode.AIRFLOW_ASYNC,
    async_py_requirements=["dbt-bigquery==1.10.1"],
)

dbt_project = DbtDag(
    dag_id="cosmos_async",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    operator_args={
        "location": "US",
        "install_deps": True,
        "full_refresh": True,
    },
    tags=["bigquery", "async"],
)
