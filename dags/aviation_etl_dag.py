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
    tags=['aviation', 'weather', 'etl', 'dbt'],
) as dag:

    # 3. TASK 1 (ETL)
    run_etl_script = BashOperator(
        task_id='run_main_python_script',
        bash_command='python /opt/airflow/main.py',
    )

    # 4. TASK 2 (ELT)
    run_dbt_transform = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/aviation_analytics && dbt run --profiles-dir .',
    )

    # 5. EXECUTION ORDER (PIPELINE)
    run_etl_script >> run_dbt_transform