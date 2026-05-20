FROM apache/airflow:2.9.1-python3.10

# Switch to root to install system dependencies if needed, then back to airflow
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
# Install dbt-core and the snowflake adapter inside the Airflow container
RUN pip install --no-cache-dir dbt-core dbt-snowflake