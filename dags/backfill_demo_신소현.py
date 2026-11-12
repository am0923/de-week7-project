from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import get_current_context


OUTPUT_DIR = Path("/opt/airflow/data/backfill_demo")


def create_ds_file():
    context = get_current_context()
    ds = context["ds"]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_DIR / f"{ds}.txt"
    output_file.write_text(
        f"logical_date={ds}\n",
        encoding="utf-8",
    )

    print(f"Created file: {output_file}")
    print(f"logical_date: {ds}")


with DAG(
    dag_id="backfill_demo_신소현",
    start_date=datetime(2026, 11, 5),
    schedule="@daily",
    catchup=True,
    tags=["week7", "q7"],
) as dag:

    create_file = PythonOperator(
        task_id="create_ds_file",
        python_callable=create_ds_file,
    )