from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def first_python_task():
    return "first python task completed"


def second_python_task():
    return "second python task completed"


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["week7", "q3"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    python_task_1 = PythonOperator(
        task_id="python_task_1",
        python_callable=first_python_task,
    )

    python_task_2 = PythonOperator(
        task_id="python_task_2",
        python_callable=second_python_task,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> python_task_1 >> python_task_2 >> end