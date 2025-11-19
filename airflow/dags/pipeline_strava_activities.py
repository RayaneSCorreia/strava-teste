from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timezone

default_args = {
    "owner": "rayane",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_strava_activities",
    default_args=default_args,
    description="Pipeline ETL Strava Activities (Bronze -> Silver -> Gold)",
    start_date=datetime(2025, 11, 14, tzinfo=timezone.utc),
    schedule_interval="0 3 * * *",   # 03:00 todo dia
    catchup=False,
    tags=["strava", "etl"],
) as dag:

    # 1. Extrai dados da API para Bronze
    bronze_task_activities = BashOperator(
        task_id="extract_bronze_activities",
        bash_command="python /opt/airflow/apps/01-bronze/extract_api_strava_user_activities.py"
    )

    # 2. Extrai DADOS DA camada Bronze para Silver
    silver_task_activities = BashOperator(
        task_id="extract_silver_activities",
        bash_command="python /opt/airflow/apps/02-silver/extract-bronze-to-silver-strava-activities.py"
    )

    # 3 Send to Gold
    gold_task_activities = BashOperator(
        task_id="extract_gold_fact_activities",
        bash_command="python /opt/airflow/apps/03-gold/fact_user_activities_strava.py"
    )

    # Aplicando camada de data quality
    gold_data_quality = BashOperator(
        task_id="data_quality_gold_activities",
        bash_command="python /opt/airflow/apps/05-DataQuality/data_quality_gold.py"
    )

    # 4 Send to Postgres BI
    gold_activitties_to_postgres_bi = BashOperator(
        task_id="gold_activitties_to_postgres_bi",
        bash_command="python /opt/airflow/apps/04-LoadBI/gold_activitties_to_postgres_bi.py"
    )


    # Definir a ordem
    bronze_task_activities >> silver_task_activities >> gold_task_activities >> gold_data_quality >> gold_activitties_to_postgres_bi
    