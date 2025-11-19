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
BUCKET_GOLD_ACTIVITIES   = os.getenv("GOLD_BUCKET_USER_ACTIVITIES")  

#Configurando os paths fomato s3a:// para o spark ler/escrever no Minio
SILVER_ACTIVITIES_PATH = f"s3a://{BUCKET_SILVER_ACTIVITIES}"
GOLD_DELTA_PATH        = f"s3a://{BUCKET_GOLD_ACTIVITIES}/fact_user_activities_labels"


# Leitura dos dados parquet da prata pra gerar views e fazer o spark sql
df_acts_gold = spark.read.parquet(f"{GOLD_DELTA_PATH}/*/*")
df_acts_silver = spark.read.parquet(f"{SILVER_ACTIVITIES_PATH}/*/*")

df_acts_gold.createOrReplaceTempView("gold_activities")
df_acts_silver.createOrReplaceTempView("silver_activities")

#usando validadores negociais e de pipeline (pip)
#validador usado na outro que permite o cruzamento de tablas para validacoes negociais

spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_gold_activities_labels AS           

        WITH activities_silver AS (
            SELECT activity_id
            FROM (
                SELECT
                    a.activity_id,
                    ROW_NUMBER() OVER (PARTITION BY a.activity_id ORDER BY a.timestamp DESC) AS hub_rank
                FROM silver_activities a
            ) A
            WHERE hub_rank = 1
        ),

        erros_atividade_sem_data AS (
            SELECT
                'Atividade sem data' AS error,
                COUNT(DISTINCT activity_id) AS qtde_erros
            FROM gold_activities
            WHERE activity_date IS NULL
        ),

        erros_atividade_sem_atleta AS (
            SELECT
                'Atividade sem atleta atrelado' AS error,
                COUNT(DISTINCT activity_id) AS qtde_erros
            FROM gold_activities
            WHERE athlete_id IS NULL
        ),

        erros_atividade_duplicada AS (
            SELECT
                'Atividade duplicada' AS error,
                COUNT(*) - COUNT(DISTINCT activity_id) AS qtde_erros
            FROM gold_activities
        ),

        erros_faltando_gold AS (
            SELECT
                'Erro de pipeline: faltando linhas no destino (gold)' AS error,
                COUNT(DISTINCT a.activity_id) AS qtde_erros
            FROM activities_silver a
            LEFT JOIN gold_activities b 
                ON a.activity_id = b.activity_id
            WHERE b.activity_id IS NULL
        )

        SELECT * FROM erros_atividade_sem_data
        UNION ALL
        SELECT * FROM erros_atividade_sem_atleta
        UNION ALL
        SELECT * FROM erros_atividade_duplicada
        UNION ALL
        SELECT * FROM erros_faltando_gold
""")


df_gold = spark.table("vw_gold_activities_labels").toPandas()

# Filtra quando algum validador acusou quantidade de erros >0 
erros_encontrados = df_gold[df_gold["qtde_erros"] > 0]

if not erros_encontrados.empty:
    print("Foram encontrados problemas de Data Quality na camada OURO:")

    erros = erros_encontrados.to_dict(orient="records")

    for erro in erros:
        print(f"• {erro['error']}: {erro['qtde_erros']} registros")

    # Gerar exceção e parar a DAG
    raise Exception(
        "❌ [DATA QUALITY FAILED] ❌  Inconsistências encontradas na camada GOLD. "
        "Veja os logs acima para detalhes."
    )

print(" ✅ [DATA QUALITY OK] ✅ Nenhuma inconsistência encontrada na camada OURO.")

