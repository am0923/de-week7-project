import os
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

BUCKET_NAME = os.getenv("S3_BUCKET")

BRONZE_KEY = "bronze/netflix_titles.csv"

AIRFLOW_INPUT = "/opt/airflow/data/netflix_titles.csv"
SPARK_INPUT = "/opt/airflow/data/netflix_titles.csv"

SPARK_OUTPUT = "/opt/airflow/data/silver_output"
AIRFLOW_OUTPUT = "/opt/airflow/data/silver_output"


def download_from_s3():
    if not BUCKET_NAME:
        raise ValueError("S3_BUCKET environment variable is required.")

    os.makedirs("/opt/airflow/data", exist_ok=True)

    s3 = S3Hook(aws_conn_id="aws_default")

    s3.get_conn().download_file(
        BUCKET_NAME,
        BRONZE_KEY,
        AIRFLOW_INPUT,
    )

    print(f"Downloaded: s3://{BUCKET_NAME}/{BRONZE_KEY}")
    print(f"Local input: {AIRFLOW_INPUT}")


def upload_to_s3(**context):
    if not BUCKET_NAME:
        raise ValueError("S3_BUCKET environment variable is required.")

    ds = context["ds"]
    prefix = f"silver/{ds}/"

    s3 = S3Hook(aws_conn_id="aws_default")

    uploaded_count = 0

    for root, _, files in os.walk(AIRFLOW_OUTPUT):
        for filename in files:
            if filename.startswith(".") or filename.startswith("_"):
                continue

            local_file = os.path.join(root, filename)

            relative_path = os.path.relpath(
                local_file,
                AIRFLOW_OUTPUT,
            )

            s3_key = f"{prefix}{relative_path}"

            s3.load_file(
                filename=local_file,
                key=s3_key,
                bucket_name=BUCKET_NAME,
                replace=True,
            )

            uploaded_count += 1
            print(f"Uploaded: s3://{BUCKET_NAME}/{s3_key}")

    print(f"Uploaded file count: {uploaded_count}")
    print(f"Silver prefix: s3://{BUCKET_NAME}/{prefix}")


with DAG(
    dag_id="weekly_pipeline_신소현",
    start_date=datetime(2026, 11, 1),
    schedule="@weekly",
    catchup=False,
    tags=["week7", "q9"],
) as dag:

    download_task = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
    )

    transform_task = SparkSubmitOperator(
        task_id="spark_transform",
        conn_id="spark_default",
        application="/opt/airflow/dags/jobs/transform.py",
        application_args=[
            "--input",
            SPARK_INPUT,
            "--output",
            SPARK_OUTPUT,
            "--baseline-year",
            "2015",
        ],
        verbose=True,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    download_task >> transform_task >> upload_task