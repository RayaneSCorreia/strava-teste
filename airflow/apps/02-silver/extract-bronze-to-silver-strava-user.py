import os, logging
from dotenv import load_dotenv, set_key 
from pathlib import Path
from datetime import datetime, timedelta
from urllib3.exceptions import HTTPError, MaxRetryError
import pandas as pd
from minio import Minio
import json
from io import BytesIO

########## PADRAO DE EXTRACAO DA BRONZE PARA SILVER - NAO MUDAR ##########
# Oculta warnings desnecessários
os.environ["SILENCE_TOKEN_WARNINGS"] = "true"

ENV_PATH = "/opt/airflow/apps/.env"
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

# Ajuste os nomes das variáveis de ambiente conforme seu .env
TYPE_PROCESS = os.getenv("TYPE_PROCESS_USER")  # FULL ou INCREMENTAL (por enquanto só FULL)
BUCKET_SILVER = os.getenv("SILVER_BUCKET_USER")  
BUCKET_BRONZE = os.getenv("BRONZE_BUCKET_USER")
LAST_PROCESSED_UPDATED_AT_USER = os.getenv("LAST_PROCESSED_UPDATED_AT_USER")

print("BUCKET SILVER USER:", BUCKET_SILVER)

# Config MinIO
client_minio = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_USER"),
    secret_key=os.getenv("MINIO_PASS"),
    secure=False
)

# -----------------------------------------
# Função para padronizar o JSON de USER
# -----------------------------------------
def padronizar_user_bronze(user: dict) -> dict:
    """
    Recebe o JSON bruto do usuário (athlete) vindo da bronze
    e devolve um dicionário flat para ir para a silver.
    """

    bikes = user.get("bikes") or []
    shoes = user.get("shoes") or []
    clubs = user.get("clubs") or []

    row = {
        "user_id": user.get("id"),
        "firstname": user.get("firstname"),
        "lastname": user.get("lastname"),
        "sex": user.get("sex"),
        "athlete_type": user.get("athlete_type"),
        "city": user.get("city"),
        "state": user.get("state"),
        "country": user.get("country"),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at"),
        "premium": user.get("premium"),
        "summit": user.get("summit"),
        "badge_type_id": user.get("badge_type_id"),
        "membership": user.get("membership"),
        "plan": user.get("plan"),
        "premium_expiration_date": user.get("premium_expiration_date"),
        "agreed_to_terms": user.get("agreed_to_terms"),
        "weight": user.get("weight"),
        "ftp": user.get("ftp"),
        "max_heartrate": user.get("max_heartrate"),
        "measurement_preference": user.get("measurement_preference"),
        "date_preference": user.get("date_preference"),
        "follower_count": user.get("follower_count"),
        "friend_count": user.get("friend_count"),
        "mutual_friend_count": user.get("mutual_friend_count"),
        "follower_request_count": user.get("follower_request_count"),
        "global_privacy": user.get("global_privacy"),
        "receive_newsletter": user.get("receive_newsletter"),
        "email_kom_lost": user.get("email_kom_lost"),
        "email_send_follower_notices": user.get("email_send_follower_notices"),
        "receive_kudos_emails": user.get("receive_kudos_emails"),
        "receive_follower_feed_emails": user.get("receive_follower_feed_emails"),
        "receive_comment_emails": user.get("receive_comment_emails"),
        "profile": user.get("profile"),
        "profile_medium": user.get("profile_medium"),
        "profile_original": user.get("profile_original"),
        "username": user.get("username"),
        "instagram_username": user.get("instagram_username"),
        "email": user.get("email"),
        "email_language": user.get("email_language"),
        "description": user.get("description"),
        "bikes_count": len(bikes),
        "shoes_count": len(shoes),
        "clubs_count": len(clubs),
        "bikes_json": json.dumps(bikes, ensure_ascii=False),
        "shoes_json": json.dumps(shoes, ensure_ascii=False),
        "clubs_json": json.dumps(clubs, ensure_ascii=False),
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
        user = json.loads(data_bytes.decode("utf-8"))

        linha = padronizar_user_bronze(user)
        registros.append(linha)

if not registros:
    print("Nenhum JSON de USER encontrado na bronze.")
else:
    # Cria DF já padronizado
    df = pd.DataFrame(registros)

    # Deduplicação por usuário, mantendo o mais recente (maior updated_at)
    if "user_id" in df.columns and "updated_at" in df.columns:
        antes = len(df)

        # converte updated_at pra ordenar corretamente
        df["updated_at_dt"] = pd.to_datetime(df["updated_at"])
        df = df.sort_values("updated_at_dt", ascending=False)
        df = df.drop_duplicates(subset=["user_id"], keep="first")

        depois = len(df)
        print(f"🔁 Deduplicados USER: {antes - depois} removidos")
    else:
        print("⚠️ Colunas 'user_id' ou 'updated_at' não encontradas para deduplicação.")

    # Se não existir a coluna de data convertida ainda, cria
    if "updated_at_dt" not in df.columns:
        df["updated_at_dt"] = pd.to_datetime(df["updated_at"])

    # Criar colunas de partição ano / mês a partir de updated_at
    df["ano"] = df["updated_at_dt"].dt.year.astype(str)
    df["mes"] = df["updated_at_dt"].dt.month.astype(str).str.zfill(2)

    # Salvando em PARQUET particionado por ano/mês
    for (ano, mes), df_particao in df.groupby(["ano", "mes"]):
        key = f"{ano}/{mes}/users_{ano}-{mes}.parquet"

        buffer = BytesIO()
        df_particao.to_parquet(buffer, index=False)
        buffer.seek(0)

        client_minio.put_object(
            BUCKET_SILVER,
            key,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )

        print(f"📦 USER salvo na silver: {BUCKET_SILVER}/{key}")

    print("✅ Processo de USER finalizado com sucesso ✅")
