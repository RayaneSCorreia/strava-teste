import os, logging
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import pandas as pd
from minio import Minio
import json
from io import BytesIO

os.environ["SILENCE_TOKEN_WARNINGS"] = "true"

ENV_PATH = "/opt/airflow/apps/.env"
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

TYPE_PROCESS_LABEL_STUDIO = os.getenv("TYPE_PROCESS_LABEL_STUDIO", "FULL")

BUCKET_SILVER = os.getenv("SILVER_BUCKET_LABEL_STUDIO")
BUCKET_BRONZE = os.getenv("BRONZE_BUCKET_LABEL_STUDIO")

print("Bucket Silver Label Studio:", BUCKET_SILVER)

client_minio = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_USER"),
    secret_key=os.getenv("MINIO_PASS"),
    secure=False
)

def padronizar_tarefa_labelstudio(task: dict):
    data = task.get("data") or {}
    annotations = task.get("annotations") or []

    task_id = task.get("id")
    task_created_at = task.get("created_at")
    task_updated_at = task.get("updated_at")

    activity_id = data.get("id")  # id da atividade do Strava
    start_date_local = data.get("start_date_local")

    linhas = []
    for ann in annotations:
        # garantia de dict
        if isinstance(ann, dict) is False:
            continue

        row = {
            "task_id": task_id,
            "activity_id": activity_id,
            "annotation_id": ann.get("id"),

            "task_created_at": task_created_at,
            "task_updated_at": task_updated_at,

            "annotation_created_at": ann.get("created_at"),
            "annotation_updated_at": ann.get("updated_at"),
            "completed_by": ann.get("completed_by"),
            "was_cancelled": ann.get("was_cancelled"),
            "ground_truth": ann.get("ground_truth"),

            "annotation_result": json.dumps(ann.get("result", []), ensure_ascii=False),

            "start_date_local": start_date_local,
        }

        linhas.append(row)

    return linhas


# FULL LOAD
print('Esvaziando bucket sulver para full load')

for obj in client_minio.list_objects(BUCKET_SILVER, recursive=True):
    client_minio.remove_object(BUCKET_SILVER, obj.object_name)
    print("🗑️ Removido:", obj.object_name)

print("✅ Bucket esvaziado:", BUCKET_SILVER)

registros = []

for obj in client_minio.list_objects(BUCKET_BRONZE, recursive=True):
    if not obj.object_name.endswith(".json"):
        continue

    data_bytes = client_minio.get_object(BUCKET_BRONZE, obj.object_name).read()
    raw = data_bytes.decode("utf-8")

    ### 1ª decodificação
    try:
        conteudo = json.loads(raw)
    except json.JSONDecodeError:
        print(f"⚠️ Não consegui dar json.loads em {obj.object_name}")
        continue

    ### Se ainda for string, é JSON dentro de JSON → decodifica de novo
    if isinstance(conteudo, str):
        try:
            conteudo = json.loads(conteudo)
        except json.JSONDecodeError:
            print(f"⚠️ Conteúdo ainda string após json.loads em {obj.object_name}")
            continue

    ### Se vier lista de tarefas, trata cada uma; se vier uma só, coloca em lista
    if isinstance(conteudo, list):
        tasks = conteudo
    else:
        tasks = [conteudo]

    for task in tasks:
        if not isinstance(task, dict):
            print(f"⚠️ Task não é dict em {obj.object_name}: tipo={type(task)}")
            continue

        linhas = padronizar_tarefa_labelstudio(task)
        if linhas:
            registros.extend(linhas)

if not registros:
    print("Nenhum JSON com anotação encontrado na bronze do Label Studio.")
else:
    df = pd.DataFrame(registros)

    if "annotation_id" in df.columns:
        antes = len(df)
        df = df.drop_duplicates(subset=["annotation_id"])
        depois = len(df)
        print(f"🔁 Deduplicados: {antes - depois} registros de anotação removidos")
    else:
        print("⚠️ Coluna 'annotation_id' não encontrada para deduplicação.")

    # Particionamento
    if "start_date_local" in df.columns and df["start_date_local"].notna().any():
        dt_col = pd.to_datetime(df["start_date_local"])
    else:
        dt_col = pd.to_datetime(df["annotation_created_at"])

    df["ano"] = dt_col.dt.year.astype(str)
    df["mes"] = dt_col.dt.month.astype(str).str.zfill(2)

    for (ano, mes), df_particao in df.groupby(["ano", "mes"]):
        key = f"{ano}/{mes}/labelstudio_anotacoes_{ano}-{mes}.parquet"

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

        print(f"📦 Salvo na silver Label Studio: {BUCKET_SILVER}/{key}")

    print("✅ Processo Label Studio → Silver finalizado com sucesso ✅")
