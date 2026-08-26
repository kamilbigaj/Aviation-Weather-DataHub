import os
import sys
import requests
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow')

from etl.flights import run_extract_flights_phase, run_transform_flights_phase
from etl.weather import run_weather_phase
from etl.database import run_load_phase

# Initialize Airflow logger for the callbacks
logger = logging.getLogger(__name__)

# 1. ALERTING FUNCTIONS: DISCORD WEBHOOK (FAILURE & SUCCESS)
def send_discord_alert(context):
    """Sends a formatted alert message to Discord upon task failure."""
    try:
        logger.info("Starting Discord alert dispatch sequence...")
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        
        if not webhook_url:
            logger.error("CRITICAL: DISCORD_WEBHOOK_URL not found in environment variables!")
            return

        task_instance = context.get('task_instance')
        task_id = task_instance.task_id
        dag_id = task_instance.dag_id
        execution_date = context.get('execution_date')
        log_url = task_instance.log_url

        payload = {
            "username": "Airflow Alert Bot",
            "avatar_url": "https://airflow.apache.org/images/feature-image.png",
            "embeds": [
                {
                    "title": "🚨 CRITICAL PIPELINE FAILURE 🚨",
                    "description": f"Task **{task_id}** in DAG **{dag_id}** has failed!",
                    "color": 16711680,
                    "fields": [
                        {"name": "Execution Date (UTC)", "value": str(execution_date), "inline": False},
                        {"name": "Check Logs", "value": f"[Click here to view Airflow logs]({log_url})", "inline": False}
                    ],
                    "footer": {"text": "Aviation ETL Pipeline - Auto Monitoring"}
                }
            ]
        }
        
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logger.info("SUCCESS: Discord alert dispatched and accepted by the server!")
        
    except Exception as e:
        logger.error(f"FAILED to send Discord alert. Reason: {e}")

def send_discord_success(context):
    """Sends a formatted success message to Discord upon full DAG completion."""
    try:
        logger.info("Starting Discord success notification sequence...")
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        
        if not webhook_url:
            logger.warning("DISCORD_WEBHOOK_URL not found. Success notification suppressed.")
            return

        dag_id = context.get('dag').dag_id
        execution_date = context.get('execution_date')

        payload = {
            "username": "Airflow Success Bot",
            "avatar_url": "https://airflow.apache.org/images/feature-image.png",
            "embeds": [
                {
                    "title": "✅ PIPELINE EXECUTED SUCCESSFULLY ✅",
                    "description": f"All modular tasks in DAG **{dag_id}** completed without errors!",
                    "color": 65280,
                    "fields": [
                        {"name": "Execution Date (UTC)", "value": str(execution_date), "inline": False}
                    ],
                    "footer": {"text": "Aviation ETL Pipeline - Auto Monitoring"}
                }
            ]
        }
        
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logger.info("SUCCESS: Discord success notification dispatched!")
        
    except Exception as e:
        logger.error(f"FAILED to send Discord success notification. Reason: {e}")

# 2. DAG DEFINITION & CONFIGURATION
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_discord_alert  # Applies to any individual failed task
}

with DAG(
    'aviation_weather_etl_pipeline',
    default_args=default_args,
    description='Automated modular daily ETL pipeline for aviation and weather data',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_success_callback=send_discord_success,  # Applies when all tasks succeed
    tags=['aviation', 'weather', 'etl', 'dbt'],
) as dag:

    # 3. MODULAR TASKS
    extract_flights = PythonOperator(
        task_id='extract_flights',
        python_callable=run_extract_flights_phase,
    )

    transform_flights = PythonOperator(
        task_id='transform_flights',
        python_callable=run_transform_flights_phase,
    )

    extract_weather = PythonOperator(
        task_id='extract_weather',
        python_callable=run_weather_phase,
    )

    load_postgres = PythonOperator(
        task_id='load_postgres',
        python_callable=run_load_phase,
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/aviation_analytics && dbt run --profiles-dir .',
    )

    # 4. TASK DEPENDENCIES (THE PIPELINE FLOW)
    extract_flights >> transform_flights >> extract_weather >> load_postgres >> run_dbt_models