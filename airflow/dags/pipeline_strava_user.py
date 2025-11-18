from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timezone

default_args = {
    "owner": "rayane",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_strava_user_profile",
    default_args=default_args,
    description="Pipeline ETL Strava (Bronze -> Silver -> Gold)",
    start_date=datetime(2025, 11, 14, tzinfo=timezone.utc),
    schedule_interval="0 3 * * *",   # 03:00 todo dia
    catchup=False,
    tags=["strava", "etl"],
) as dag:


    # Extrai dados da API para Bronze
    bronze_task_user = BashOperator(
        task_id="extract_bronze_user",
        bash_command="python  /opt/airflow/apps/01-bronze/extract_api_strava_user.py"
    )

    # Extrai dados da camada Bronze para Silver
    silver_task_user = BashOperator(
        task_id="extract_silver_user",
        bash_command="python  /opt/airflow/apps/02-silver/extract-bronze-to-silver-strava-user.py"
    )

    # Definir a ordem
    bronze_task_user >> silver_task_user
    