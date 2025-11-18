import os, logging
from dotenv import load_dotenv, set_key 
from pathlib import Path
import os
from datetime import datetime, timedelta
from urllib3.exceptions import HTTPError, MaxRetryError
import pandas as pd
from minio import Minio
import json
from pandas import json_normalize
from io import BytesIO

########## PADRAO DE EXTRACAO DA BRONZE PARA SILVER - NAO MUDAR ##########
# Oculta warnings desnecessários
os.environ["SILENCE_TOKEN_WARNINGS"] = "true"

#garantindo o uso do refresh token gerado no IF anterior
ENV_PATH = "/opt/airflow/apps/.env"
#garantindo o uso do refresh token gerado no first_auth
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

TYPE_PROCESS = os.getenv("TYPE_PROCESS_USER_ACTIVITIES") # FULL ou INCREMENTAL
BUCKET_SILVER = os.getenv("SILVER_BUCKET_USER_ACTIVITIES")  
BUCKET_BRONZE = os.getenv("BRONZE_BUCKET_USER_ACTIVITIES")
LAST_PROCESSED_START_DATE_USER_ACTIVITIES = os.getenv("LAST_PROCESSED_START_DATE_USER_ACTIVITIES")

print(BUCKET_SILVER )

#config minio
client_minio = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_USER"),
    secret_key=os.getenv("MINIO_PASS"),
    secure=False
)
####################

# Função para padronizar os dados da bronze
def padronizar_atividade_bronze(activity: dict) -> dict:
    athlete = activity.get("athlete") or {}
    m = activity.get("map") or {}

    row = {
        # Identidade e campos criados na bronze para organizacao do dado
        "activity_id": activity.get("id"),
        "hash_id": activity.get("hash_id"),
        "timestamp": activity.get("timestamp"),

        # Dados da Atividade
        "start_date": activity.get("start_date"),
        "start_date_local": activity.get("start_date_local"),
        "timezone": activity.get("timezone"),
        "name": activity.get("name"),
        "sport_type": activity.get("sport_type"),
        "type": activity.get("type"),
        "workout_type": activity.get("workout_type"),
        "distance": activity.get("distance"),
        "elapsed_time": activity.get("elapsed_time"),
        "moving_time": activity.get("moving_time"),
        "average_speed": activity.get("average_speed"),
        "max_speed": activity.get("max_speed"),
        "total_elevation_gain": activity.get("total_elevation_gain"),
        "elev_high": activity.get("elev_high"),
        "elev_low": activity.get("elev_low"),                 
        "average_cadence": activity.get("average_cadence"),
        "max_cadence": activity.get("max_cadence"),
        "average_watts": activity.get("average_watts"),
        "max_watts": activity.get("max_watts"),
        "weighted_average_watts": activity.get("weighted_average_watts"),
        "has_heartrate": activity.get("has_heartrate"),
        "average_heartrate": activity.get("average_heartrate"),
        "max_heartrate": activity.get("max_heartrate"),
        "suffer_score": activity.get("suffer_score"),
        "trainer": activity.get("trainer"),
        "commute": activity.get("commute"),
        "private": activity.get("private"),
        "visibility": activity.get("visibility"),
        "manual": activity.get("manual"),
        "kudos_count": activity.get("kudos_count"),
        "athlete_id": athlete.get("id"),
        "map_id": m.get("id"),
    }

    return row

# FULL LOAD
print('Esvaziando bucket sulver para full load')

BUCKET_SILVER 
for obj in client_minio.list_objects(BUCKET_SILVER, recursive=True):
    client_minio.remove_object(BUCKET_SILVER, obj.object_name)
    print("🗑️ Removido:", obj.object_name)

print("✅ Bucket esvaziado:", BUCKET_SILVER)


registros = []

for obj in client_minio.list_objects(BUCKET_BRONZE, recursive=True):
    if obj.object_name.endswith(".json"):
        data_bytes = client_minio.get_object(BUCKET_BRONZE, obj.object_name).read()
        activity = json.loads(data_bytes.decode("utf-8"))

        linha = padronizar_atividade_bronze(activity)
        registros.append(linha)

if not registros:
    print("Nenhum JSON encontrado na bronze.")
else:
    # Cria DF já padronizado
    df = pd.DataFrame(registros)

    # Deduplicação por hash_id
    if "hash_id" in df.columns:
        antes = len(df)
        df = df.drop_duplicates(subset=["hash_id"])
        depois = len(df)
        print(f"🔁 Deduplicados: {antes - depois} removidos")
    else:
        print("⚠️ Coluna 'hash_id' não encontrada para deduplicação.")

    
    df["start_date_local_dt"] = pd.to_datetime(df["start_date_local"], errors="coerce")

    # elimina linhas sem data local válida
    df = df.dropna(subset=["start_date_local_dt"])

    # ano como string
    df["ano"] = df["start_date_local_dt"].dt.year.astype(int).astype(str)

    # mês forçado no mapa 1..12 -> '01'..'12'dpois existem meses com 0 à esquerda e algum comporta
    #n mapeados que gerava partições erradas como 2023/ 07 2 e isso quebrava no spark golh
    mes_map = {
        1: "01", 2: "02", 3: "03", 4: "04",
        5: "05", 6: "06", 7: "07", 8: "08",
        9: "09", 10: "10", 11: "11", 12: "12",
    }
    df["mes"] = df["start_date_local_dt"].dt.month.map(mes_map)

    # joga fora qualquer coisa que ainda tenha escapado
    df = df[df["mes"].isin(list(mes_map.values()))]

    print("Valores únicos de mês na Silver:", df["mes"].unique())

    # -----------------------------
    # SALVANDO EM PARQUET (ajustado p/ Spark)
    # -----------------------------
    for (ano, mes), df_particao in df.groupby(["ano", "mes"]):
        print(f"📁 Gravando partição ano={ano}, mes={mes!r}")

        key = f"{ano}/{mes}/atividades_{ano}-{mes}.parquet"

        buffer = BytesIO()
        df_particao.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
        buffer.seek(0)

        client_minio.put_object(
            BUCKET_SILVER,
            key,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )

        print(f"📦 Salvo na silver: {BUCKET_SILVER}/{key}")

    print("✅ Processo finalizado com sucesso ✅")
    print(f"Carga Bronze -> Silver finalizada com sucesso. Total de registros na Silver: {len(df)}")