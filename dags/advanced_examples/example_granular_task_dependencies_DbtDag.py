"""
Example showcasing how to create granular task dependencies between tasks outside of the 
dbt project and individual tasks inside the dbt project rendered with Cosmos when using `DbtDag`.
"Walk the dag graph" approach.
"""

from cosmos import (
    DbtDag,
    DbtResourceType,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
)
from airflow.sdk import chain, task
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

import os
from pathlib import Path

# You need to set this Airflow connection, for an example see the .env_example file in the root of this repository
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA")

# Adjust this to your own project name, the path to the dbt project and
# the path to the dbt executable if you are using one
DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / DBT_PROJECT_NAME).resolve().as_posix()
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

# Only needed if you can't install dbt-postgres in the requirements.txt file
_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


with DbtDag(
    dag_id="example_granular_task_dependencies_DbtDag",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    tags=["out-of-the-box", "task-dependencies"],
) as dag:

    # define the upstream task
    @task
    def downstream_of_all_seeds():
        pass

    _downstream_of_all_seeds = downstream_of_all_seeds()

    # Walk the dbt graph
    for unique_id, dbt_node in dag.dbt_graph.filtered_nodes.items():
        # Filter by any dbt_node property you prefer, for example only selecting the resource type "source"
        if dbt_node.resource_type == DbtResourceType.SEED:
            # Look up the corresponding Airflow task or task group in the DbtToAirflowConverter.tasks_map property.
            my_dbt_task = dag.tasks_map[unique_id]
            # Create a task upstream of this Airflow source task/task group.
            chain(my_dbt_task, _downstream_of_all_seeds)
