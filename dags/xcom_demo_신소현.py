from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import get_current_context
from airflow.providers.standard.operators.python import PythonOperator


def push_value():
    value = "Hello from XCom"
    print(f"Returning value: {value}")
    return value


def pull_value_with_retry():
    context = get_current_context()
    ti = context["ti"]

    pulled_value = ti.xcom_pull(task_ids="push_value")
    print(f"Pulled XCom value: {pulled_value}")
    print(f"Current try number: {ti.try_number}")

    if ti.try_number == 1:
        print("First attempt intentionally fails.")
        raise ValueError("Intentional failure for retry demonstration")

    print("Retry succeeded.")
    print(f"Final XCom value: {pulled_value}")


with DAG(
    dag_id="xcom_demo_신소현",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["week7", "q6"],
) as dag:

    push_value_task = PythonOperator(
        task_id="push_value",
        python_callable=push_value,
    )

    pull_and_retry_task = PythonOperator(
        task_id="pull_and_retry",
        python_callable=pull_value_with_retry,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    push_value_task >> pull_and_retry_task