from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from minio import Minio

#spark = get_spark("gold-user-activities-labels")
# -------------------------------------------------------------------
# 0. CONFIG GERAL
# -------------------------------------------------------------------
os.environ["SILENCE_TOKEN_WARNINGS"] = "true"

ENV_PATH = "/opt/airflow/apps/.env"
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

ACCESS_KEY = os.getenv("MINIO_USER")
SECRET_KEY = os.getenv("MINIO_PASS")

    
spark = (
    SparkSession.builder
        .appName("silver-to-gold-activities-labels")
        .master("local[*]")
        # JARs do S3A
        .config(
            "spark.jars",
            "/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar"
        )
        # Config MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .getOrCreate()
)

BUCKET_SILVER_ACTIVITIES = os.getenv("SILVER_BUCKET_USER_ACTIVITIES")
BUCKET_SILVER_LABELS     = os.getenv("SILVER_BUCKET_LABEL_STUDIO")    
BUCKET_GOLD_ACTIVITIES   = os.getenv("GOLD_BUCKET_USER_ACTIVITIES")  

#Configurando os paths fomato s3a:// para o spark ler/escrever no Minio
SILVER_ACTIVITIES_PATH = f"s3a://{BUCKET_SILVER_ACTIVITIES}"
SILVER_LABELS_PATH     = f"s3a://{BUCKET_SILVER_LABELS}"
GOLD_DELTA_PATH        = f"s3a://{BUCKET_GOLD_ACTIVITIES}/fact_user_activities_labels"

# Leitura dos dados parquet da prata pra gerar views e fazer o spark sql
df_acts = spark.read.parquet(f"{SILVER_ACTIVITIES_PATH}/*/*")
df_labels = spark.read.parquet(f"{SILVER_LABELS_PATH}/*/*")
df_acts.createOrReplaceTempView("silver_activities")
df_labels.createOrReplaceTempView("silver_labels")

#No futuro quando a prata ja tiver em formato delta tbm, da pra usar o MERGE INTO direto pra atualizar a ouro considerando UPDATES e DELETES
#acessando apenas a particoes alteradas atraves do hub_transaction_date
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_gold_activities_labels AS     
    WITH activities_ranked AS (
        SELECT a.*
        FROM (
            SELECT
                a.*,
                ROW_NUMBER() OVER ( PARTITION BY a.activity_id ORDER BY a.timestamp DESC) AS hub_rank
            FROM silver_activities a) A
        WHERE hub_rank = 1
    ),

    labels_ranked AS (
        SELECT a.*
        FROM (
            SELECT
                task_id,
                activity_id,
                annotation_id,
                task_created_at,
                task_updated_at,
                annotation_created_at,
                annotation_updated_at,
                completed_by,
                was_cancelled,
                ground_truth,
                annotation_result,
                start_date_local,
            ROW_NUMBER() OVER ( PARTITION BY a.task_id ORDER BY a.task_updated_at DESC) AS hub_rank
            FROM silver_labels a) A
        WHERE hub_rank = 1
    )

    SELECT
        a.*,
        l.task_id,
        l.annotation_id,
        l.task_created_at,
        l.task_updated_at,
        l.annotation_created_at,
        l.annotation_updated_at,
        l.completed_by,
        l.was_cancelled,
        l.ground_truth,
        l.annotation_result,
        DATE(COALESCE(a.start_date_local, a.start_date)) AS activity_date,
        YEAR(DATE(COALESCE(a.start_date_local, a.start_date)))  AS year,
        MONTH(DATE(COALESCE(a.start_date_local, a.start_date))) AS month
    FROM activities_ranked a
    LEFT JOIN labels_ranked l
        ON a.activity_id = l.activity_id
    """)

df_gold = spark.table("vw_gold_activities_labels")

# (opcional) dá uma olhada rápida
print("Schema da GOLD (antes de escrever em Delta):")
df_gold.printSchema()

print("Escrevendo GOLD em Delta no MinIO em:", GOLD_DELTA_PATH)

(
    df_gold.write
        .mode("overwrite")          # full load – sobrescreve
        .partitionBy("year", "month")
        .parquet(GOLD_DELTA_PATH)
)

print("Carga Silver -> Gold finalizada com sucesso.")
spark.stop()
