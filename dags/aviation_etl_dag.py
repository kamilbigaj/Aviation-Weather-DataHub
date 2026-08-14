import os
import requests
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

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

        # Extract failure details from the Airflow context
        task_instance = context.get('task_instance')
        task_id = task_instance.task_id
        dag_id = task_instance.dag_id
        execution_date = context.get('execution_date')
        log_url = task_instance.log_url

        # Construct the Discord Embed payload
        payload = {
            "username": "Airflow Alert Bot",
            "avatar_url": "https://airflow.apache.org/images/feature-image.png",
            "embeds": [
                {
                    "title": "🚨 CRITICAL PIPELINE FAILURE 🚨",
                    "description": f"Task **{task_id}** in DAG **{dag_id}** has failed!",
                    "color": 16711680,  # Red color code
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

        # Construct the Discord Embed payload for SUCCESS
        payload = {
            "username": "Airflow Success Bot",
            "avatar_url": "https://airflow.apache.org/images/feature-image.png",
            "embeds": [
                {
                    "title": "✅ PIPELINE EXECUTED SUCCESSFULLY ✅",
                    "description": f"All tasks in DAG **{dag_id}** completed without errors!",
                    "color": 65280,  # Green color code
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
    'retries': 0, 
    'on_failure_callback': send_discord_alert  # <--- APPLIES TO INDIVIDUAL FAILED TASKS
}

with DAG(
    'aviation_weather_etl_pipeline',
    default_args=default_args,
    description='Automated daily ETL pipeline for aviation and weather data',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_success_callback=send_discord_success   # <--- APPLIES ONLY ONCE WHEN THE ENTIRE DAG SUCCEEDS
) as dag:

    # Task 1: Data Extraction & Loading (Python ETL)
    run_main_python_script = BashOperator(
        task_id='run_main_python_script',
        bash_command='python /opt/airflow/main.py',
    )

    # Task 2: Analytical Transformation (dbt)
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/aviation_analytics && dbt run --profiles-dir .',
    )

    # Define task dependencies
    run_main_python_script >> run_dbt_models