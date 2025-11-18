import os
import io
import json
from datetime import datetime
from pathlib import Path
import os, logging
from dotenv import load_dotenv, set_key
import requests
from minio import Minio
from dotenv import load_dotenv

#Oculta warnings desnecessários
os.environ["SILENCE_TOKEN_WARNINGS"] = "true"
#garantindo o uso do refresh token gerado no IF anterior
ENV_PATH = "/opt/airflow/apps/.env"
#garantindo o uso do refresh token gerado no first_auth
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

PROJECT_ID = 1
LABEL_STUDIO_URL = os.getenv("LABEL_STUDIO_URL")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET_LABEL_STUDIO")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
PAT = os.getenv("LABEL_STUDIO_TOKEN")  # Personal Access Token
print("PAT (começo):", (PAT or "")[:10], "...")

#AJuste no token - para requests precisa haver a troca do PAT (USADO NO SDK) pelo access token
resp = requests.post(
    f"{LABEL_STUDIO_URL}/api/token/refresh",
    headers={"Content-Type": "application/json"},
    json={"refresh": PAT},
    timeout=30,
)
resp.raise_for_status()

access_token = resp.json()["access"]
print("ACCESS TOKEN (começo):", access_token[:20], "...")

# Buscar TASKS do projeto (export)

headers = {"Authorization": f"Bearer {access_token}"}

resp_export = requests.get(
    f"{LABEL_STUDIO_URL}/api/projects/{PROJECT_ID}/export",
    headers=headers,
    params={
        "exportType": "JSON",
        "download_all_tasks": "true"
    },
    timeout=60,
)
resp_export.raise_for_status()

tasks = resp_export.json()
print(f"Tasks retornadas no export: {len(tasks)}")

# Extraindo somente as anotadas pra n ficar duplicado dado de forma desnecessária
tasks_with_annotations = [t for t in tasks if t.get("annotations")]
print(f"Tasks COM anotação: {len(tasks_with_annotations)}")

export_data = tasks_with_annotations  # <--- troque para 'tasks' se quiser todas

#config minio
minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_USER"),
    secret_key=os.getenv("MINIO_PASS"),
    secure=False
)

if not minio_client.bucket_exists(BRONZE_BUCKET):
    minio_client.make_bucket(BRONZE_BUCKET)

now = datetime.utcnow()
partition = now.strftime("%Y/%m/%d")
file_name = f"project_{PROJECT_ID}_tasks_export_{now.strftime('%Y%m%dT%H%M%S')}.json"

object_name = f"project_id={PROJECT_ID}/dt_process={partition}/{file_name}"

json_bytes = json.dumps(export_data, ensure_ascii=False, indent=2).encode("utf-8")

minio_client.put_object(
    bucket_name=BRONZE_BUCKET,
    object_name=object_name,
    data=io.BytesIO(json_bytes),
    length=len(json_bytes),
    content_type="application/json",
)

print("✔ Upload concluído:", object_name)
print("✅ Processo finalizado com sucesso ✅")
