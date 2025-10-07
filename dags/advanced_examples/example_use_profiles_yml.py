"""
Example showcasing how to use a profiles.yml file instead of a `ProfileMapping` with an Airflow connection.
"""

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
import os
from pathlib import Path

# Resolve path to dbt project relative to this file
DBT_PROJECT_PATH = (
    (Path(__file__).parents[1] / "dbt" / "project_with_profiles_yml")
    .resolve()
    .as_posix()
)
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv_postgres/bin/dbt"

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

# profile config using a profiles.yml file located in the dbt project
_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH + "/profiles.yml",
)


# Only needed if you can't install dbt-postgres in the requirements.txt file
_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


example_use_profiles_yml = DbtDag(
    dag_id="example_use_profiles_yml",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    tags=["out-of-the-box", "profiles-yml"],
)
