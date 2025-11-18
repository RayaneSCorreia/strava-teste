from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# -------------------------------------------------------------------
# 0. CONFIG GERAL
# -------------------------------------------------------------------
os.environ["SILENCE_TOKEN_WARNINGS"] = "true"

ENV_PATH = "/opt/airflow/apps/.env"
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)
ACCESS_KEY = os.getenv("MINIO_USER")
SECRET_KEY = os.getenv("MINIO_PASS")

#iniciando confgd do spark ja com o Minio para fazer leitura e escrita
def get_spark(app_name :str):
    return (
    SparkSession.builder
        .appName("silver-to-gold-activities-labels")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access","true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") 
        .getOrCreate()
)


hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

# Teste simples — tentar listar o prefixo raiz
print("\n=== Tentando listar buckets ===")
try:
    files = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    for f in fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path("s3a://")):
        print(" -", f.getPath().toString())
except Exception as e:
    print("ERRO AO LISTAR BUCKETS:")
    print(e)

spark.stop()
