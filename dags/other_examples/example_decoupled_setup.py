# """
# Example for a decoupled setup with Kubernetes execution mode and manifest load mode.
# """

# from cosmos import DbtDag, ExecutionConfig, ExecutionMode, RenderConfig
# from airflow.providers.cncf.kubernetes.secret import Secret

# _execution_config = ExecutionConfig(
#     execution_mode=ExecutionMode.KUBERNETES,
#     # provide the project path in the ExecutionConfig instead of ProfileConfig
#     dbt_project_path="path/to/dbt/project/within/dockerimage"
# )

# # define secrets as a k8s Secret
# postgres_password_secret = Secret(
#     deploy_type="env",
#     deploy_target="POSTGRES_PASSWORD",
#     secret="postgres-secrets",
#     key="password",
# )

# postgres_host_secret = Secret(
#     deploy_type="env",
#     deploy_target="POSTGRES_HOST",
#     secret="postgres-secrets",
#     key="host",
# )

# _operator_args={
#     "image": "dbt-jaffle-shop:1.0.0",
#     "secrets": [postgres_password_secret, postgres_host_secret],
#     "is_delete_pod_operator": False,
#     # add additional KPO arguments
# }

# _render_config=RenderConfig(
#     dbt_project_path="path/to/manifest.json" # path to the manifest in the Airflow project
# )

# k8s_dag = DbtDag(
#     # ...
#     execution_config=_execution_config,
#     render_config=_render_config,
#     operator_args=_operator_args
# )
