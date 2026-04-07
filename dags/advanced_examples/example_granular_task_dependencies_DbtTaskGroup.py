"""
Example showcasing how to create granular task dependencies between tasks outside of the 
dbt project and individual tasks inside the dbt project rendered with Cosmos when using `DbtTaskGroup`.
"""

from airflow.sdk import dag, chain, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
import os

# You need to set this Airflow connection, for an example see the .env_example file in the root of this repository
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA", "DEMO_SCHEMA_ACCESS_NODES")

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/include/dbt/access_nodes"
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


@dag(tags=["out-of-the-box", "task-dependencies", "postgres"])
def example_granular_task_dependencies_DbtTaskGroup():

    @task
    def pre_dbt():
        pass

    dbt_project = DbtTaskGroup(
        group_id="dbt_project",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
    )

    @task
    def post_dbt():
        pass

    @task
    def downstream_of_model_1():
        pass

    @task
    def upstream_of_model_3():
        pass

    # Accessing nodes in the dbt project task group by task_id
    _downstream_of_model_1 = downstream_of_model_1()
    _downstream_of_model_1.set_upstream(dbt_project.get_child_by_label("model1_run"))

    _upstream_of_model_3 = upstream_of_model_3()
    _upstream_of_model_3.set_downstream(dbt_project.get_child_by_label("model3_run"))

    chain(pre_dbt(), dbt_project, post_dbt())


example_granular_task_dependencies_DbtTaskGroup()
