from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. DEFAULT ARGUMENTS - Default configuration for our tasks.
default_args = {
    'owner': 'kamil_bigaj',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. DAG DEFINITION - The main wrapper defining the workflow.
with DAG(
    dag_id='aviation_weather_etl_pipeline',
    default_args=default_args,
    description='Automated daily ETL pipeline for aviation and weather data',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 14),
    catchup=False,
    tags=['aviation', 'weather', 'etl'],
) as dag:

    # 3. TASK - A specific unit of work to be executed.
    run_etl_script = BashOperator(
        task_id='run_main_python_script',
        bash_command='python /opt/airflow/main.py',
    )

    # 4. EXECUTION ORDER (PIPELINE) - Defines the order in which tasks are executed.
    run_etl_script