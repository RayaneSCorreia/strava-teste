import os, logging
from dotenv import load_dotenv, set_key
import json
from stravalib import model, Client
from pathlib import Path
import os, time, io
import boto3
from botocore.config import Config
import minio
from minio import Minio

#Oculta warnings desnecessários
os.environ["SILENCE_TOKEN_WARNINGS"] = "true"
logging.getLogger("stravalib.util.limiter").setLevel(logging.ERROR)
#garantindo o uso do refresh token gerado no IF anterior
ENV_PATH = "/opt/airflow/apps/.env"
#garantindo o uso do refresh token gerado no first_auth
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

s3_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_USER"),
    secret_key=os.getenv("MINIO_PASS"),
    secure=False
)

#Faz o input no MINIO na camada BRONZE
def put_json_minio(s3_client, bucket: str, key: str, obj: dict):
    # Converte o dicionário em JSON e prepara o stream de bytes
    body = json.dumps(obj, ensure_ascii=False, default=str, indent=4).encode("utf-8")
    data = io.BytesIO(body)

    # Envia o arquivo para o MinIO
    s3_client.put_object(
        bucket_name=bucket,
        object_name=key,
        data=data,
        length=len(body),
        content_type="application/json"
    )


client = Client()

#Verifica se o token se expirou, expira em 6 horas:
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
print(CLIENT_ID)
if time.localtime(time.time()) >= time.localtime(int(os.getenv("EXPIRES_AT"))):
    print(" ⏳ Token expirado, atualizando... ⏳ ")
    refresh = client.refresh_access_token(
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
                refresh_token=REFRESH_TOKEN
            )

    set_key(str(ENV_PATH), "ACCESS_TOKEN", refresh["access_token"])
    set_key(str(ENV_PATH), "REFRESH_TOKEN", refresh["refresh_token"])
    set_key(str(ENV_PATH), "EXPIRES_AT", str(refresh["expires_at"]))
    print(" ⚠️  O novo token expira em:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(refresh["expires_at"])))
else:
        print(" ⚠️  O Token atual expira em:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(os.getenv("EXPIRES_AT")))))

#garantindo o uso do refresh token gerado no IF anterior
ENV_PATH = "/opt/airflow/apps/.env"
if not os.path.exists(ENV_PATH):
    ENV_PATH = Path(__file__).resolve().parent / ".env"
#garantindo o uso do refresh token gerado no first_auth
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")


token = client.refresh_access_token(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    refresh_token=REFRESH_TOKEN
)

client.access_token = token["access_token"]

print("✅ Credenciais Autenticadas")
#extracao de dados do usuário
athlete = client.get_athlete()

#PEGAR O A DATA DO ULTIMO UPDATE DO JSON DA BRONZE
if os.getenv("LAST_UPDATED_USER") == "" or os.getenv("LAST_UPDATED_USER") < str(athlete.updated_at):
    print("✅ Fazendo inser;áo de novos dados")
    athlete_dict = athlete.model_dump()
    name_file_output_json = f"{str(athlete.updated_at)[0:4]}/{str(athlete.updated_at)[5:7]}/{str(athlete.updated_at)[0:10]}-{athlete.id}.json"
    set_key(str(ENV_PATH), "LAST_UPDATED_USER", str(athlete.updated_at))
    set_key(str(ENV_PATH), "FIRST_DATE_USER", str(athlete.created_at))
    put_json_minio(s3_client, os.getenv("BRONZE_BUCKET_USER") , name_file_output_json, athlete_dict)
else:
    print("✅ Sem dados alterados")