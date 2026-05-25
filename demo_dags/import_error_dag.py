"""
Demo DAG: Import Error
Simulates a missing Python dependency.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def broken_import_task(**context):
    try:
        import nonexistent_package_xyz  # noqa: F401
    except ImportError:
        raise ImportError("No module named 'nonexistent_package_xyz'")


with DAG(
    dag_id="demo_import_error",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "broken"],
    description="Demo DAG that fails with an import error",
):
    PythonOperator(
        task_id="broken_import",
        python_callable=broken_import_task,
    )
