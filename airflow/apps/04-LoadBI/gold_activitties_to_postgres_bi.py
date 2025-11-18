from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

os.environ["SILENCE_TOKEN_WARNINGS"] = "true"

ENV_PATH = "/opt/airflow/apps/.env"
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

ACCESS_KEY = os.getenv("MINIO_USER")
SECRET_KEY = os.getenv("MINIO_PASS")

spark = (
    SparkSession.builder
        .appName("gold-activities-to-postgres-bi")
        .master("local[*]")
        .config(
            "spark.jars",
            "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/opt/spark-jars/postgresql-42.7.4.jar"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .getOrCreate()
)


BUCKET_GOLD = os.getenv("GOLD_BUCKET_USER_ACTIVITIES") 
GOLD_PARQUET_PATH = f"s3a://{BUCKET_GOLD}/fact_user_activities_labels"

print(f"Lendo GOLD em: {GOLD_PARQUET_PATH}")

df_gold = (
    spark.read
        .format("parquet")
        .load(GOLD_PARQUET_PATH)
)

print("Schema da GOLD:")
df_gold.printSchema()

# Host é o nome do serviço no docker-compose
jdbc_url = "jdbc:postgresql://postgres_bi:5432/bi"

connection_props = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

table_name = "public.strava_activities_labels"

print(f"Escrevendo tabela no Postgres BI: {table_name}")

(
    df_gold
        .write
        .mode("overwrite") 
        .jdbc(jdbc_url, table_name, properties=connection_props)
)

print("✅ Carga GOLD -> Postgres BI finalizada com sucesso.")

spark.stop()
