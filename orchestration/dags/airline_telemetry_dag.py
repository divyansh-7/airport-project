from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Define the default arguments for the pipeline
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True, # In production, this emails the FinOps/Data team
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    'airline_telemetry_medallion_pipeline',
    default_args=default_args,
    description='Automated execution of the Bronze to Gold dbt pipeline for airline telemetry.',
    schedule_interval='*/15 * * * *', # Cron expression: Run every 15 minutes
    start_date=datetime(2026, 5, 20),
    catchup=False,
    tags=['snowflake', 'dbt', 'airline', 'analytics'],
) as dag:

    # TASK 1: Start the pipeline
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # TASK 2: Run the dbt transformation layer (Silver & Gold)
    # We use a BashOperator to trigger the exact command you ran in your terminal  C:\Users\hp\Documents\coding\airport-project\airline_transformation
    # The 'cd' command ensures Airflow executes this from inside your dbt project folder
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/airline_transformation && dbt run --profiles-dir .',
    )

    # TASK 3: Run the dbt data quality tests
    # This acts as a circuit breaker. If the data is bad, the pipeline fails here and alerts the team.
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/airflow/airline_transformation && dbt test --profiles-dir .',
    )

    # TASK 4: End the pipeline
    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )

    # Define the execution order (The Graph)
    # Start -> Run Models -> Run Tests -> End
    start_pipeline >> run_dbt_models >> run_dbt_tests >> end_pipeline