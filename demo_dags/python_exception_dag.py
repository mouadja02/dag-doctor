"""
Demo DAG: Python Exception
Simulates a runtime Python exception (KeyError referencing missing column).
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def broken_python_task(**context):
    data = {"open": 100, "high": 110, "low": 95}
    # This will raise KeyError: 'close'
    close_price = data["close"]
    print(f"Close price: {close_price}")


with DAG(
    dag_id="demo_python_exception",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "broken"],
    description="Demo DAG that fails with a Python KeyError",
):
    PythonOperator(
        task_id="broken_python",
        python_callable=broken_python_task,
    )
