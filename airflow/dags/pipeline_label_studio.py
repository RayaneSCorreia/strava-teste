from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timezone

default_args = {
    "owner": "rayane",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_label_studio",
    default_args=default_args,
    description="Pipeline ETL Label Studio (Bronze -> Silver -> Gold)",
    start_date=datetime(2025, 11, 14, tzinfo=timezone.utc),
    schedule_interval="0 3 * * *",   # roda todo dia às 03:00
    catchup=False,
    tags=["label_studio", "etl"],
) as dag:

    # 1. Extrai dados do Label Studio e envia para Bronze do MinIO
    bronze_task_label_studio = BashOperator(
        task_id="extract_bronze_label_studio",
        bash_command="python /opt/airflow/apps/01-bronze/extract_api_label_studio_strava.py"
    )
    # Extrai dados da bronze para silver
    silver_task_label_studio = BashOperator(
        task_id="extract_silver_label_studio",
        bash_command="python /opt/airflow/apps/02-silver/extract-bronze-to-silver-strava-label_studio.py"
    )

    bronze_task_label_studio >> silver_task_label_studio
