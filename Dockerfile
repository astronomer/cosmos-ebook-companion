FROM astrocrpublic.azurecr.io/runtime:3.0-4

# install dbt-postgres into a virtual environment
RUN python -m venv dbt_venv_postgres && source dbt_venv_postgres/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate

# install dbt-snowflake into a virtual environment
RUN python -m venv dbt_venv_snowflake && source dbt_venv_snowflake/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate

# install dbt-databricks into a virtual environment  
RUN python -m venv dbt_venv_databricks && source dbt_venv_databricks/bin/activate && \
    pip install --no-cache-dir dbt-databricks && deactivate

# install dbt-spark into a virtual environment
RUN python -m venv dbt_venv_spark && source dbt_venv_spark/bin/activate && \
    pip install --no-cache-dir dbt-spark[PyHive] && deactivate

# install dbt-bigquery into a virtual environment
RUN python -m venv dbt_venv_bigquery && source dbt_venv_bigquery/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

# install dbt-duckdb into a virtual environment
RUN python -m venv dbt_venv_duckdb && source dbt_venv_duckdb/bin/activate && \
    pip install --no-cache-dir dbt-duckdb && deactivate