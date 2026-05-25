"""
Demo DAG: Timeout
Simulates a task that exceeds its execution_timeout.
"""

from datetime import datetime, timedelta
import time
from airflow import DAG
from airflow.operators.python import PythonOperator


def slow_task(**context):
    print("Starting a very slow operation...")
    time.sleep(120)
    print("Done!")


with DAG(
    dag_id="demo_timeout",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "broken"],
    description="Demo DAG that times out",
):
    PythonOperator(
        task_id="slow_task",
        python_callable=slow_task,
        execution_timeout=timedelta(seconds=5),
    )
