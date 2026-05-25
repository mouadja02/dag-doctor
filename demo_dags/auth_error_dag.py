"""
Demo DAG: Authentication Error
Simulates a failed authentication to an external service (Snowflake).
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def broken_auth_task(**context):
    raise RuntimeError(
        "snowflake.connector.errors.DatabaseError: "
        "Incorrect username or password was specified. "
        "Authentication failed for user DATAOPS."
    )


with DAG(
    dag_id="demo_auth_error",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "broken"],
    description="Demo DAG that fails with an authentication error",
):
    PythonOperator(
        task_id="broken_auth",
        python_callable=broken_auth_task,
    )
