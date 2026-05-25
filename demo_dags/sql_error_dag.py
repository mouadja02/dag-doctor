"""
Demo DAG: SQL Error
Simulates a SQL error by referencing a non-existent column.
Use with dag-doctor to test failure classification and reporting.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def broken_sql_task(**context):
    raise RuntimeError(
        "snowflake.connector.errors.ProgrammingError: "
        "002003 (42S02): SQL compilation error: "
        "Column 'market_cap_usd' does not exist"
    )


with DAG(
    dag_id="demo_sql_error",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "broken"],
    description="Demo DAG that fails with a SQL error",
):
    PythonOperator(
        task_id="broken_query",
        python_callable=broken_sql_task,
    )
