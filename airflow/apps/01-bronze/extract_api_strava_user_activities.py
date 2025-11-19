import os, logging
from dotenv import load_dotenv, set_key
import json
from stravalib import model, Client
from pathlib import Path
import os, time, io
import hashlib
from minio import Minio
from urllib3.exceptions import HTTPError, MaxRetryError
from datetime import datetime, timedelta

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

def save_log_error(activitie_id, activitie_date, error_message):
    log_entry = {
        "activity_id": activitie_id,
        "activity_datetime": str(activitie_date),
        "error_message": str(error_message),
        "timestamp": str(datetime.now())
    }

    data_arquivo_erro = str(activitie_date)[:10]

    # Cria o log do dia do erro caso nao exista ainda
    if not os.path.exists(f"log_{data_arquivo_erro}.json"):
        with open(f"log_{data_arquivo_erro}.json", "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=4)

    # Abre o arquivo criado vazio ou não para adicionar um novo erro a esse dia
    with open(f"log_{data_arquivo_erro}.json", "r", encoding="utf-8") as f:
        logs = json.load(f)

    logs.append(log_entry)

    with open(f"log_{data_arquivo_erro}.json", "w", encoding="utf-8") as f:
        json.dump(logs, f, ensure_ascii=False, indent=4)


def insert_files_json_to_bronze(start_date_process):
    print(start_date_process)
    start_date = start_date_process
    end_date =  datetime.fromisoformat(str(datetime.now() - timedelta(days=1))[:10] + 'T23:59:59Z')
    activitie_count = 0
    ultimo_processado = None
    atividade_alterada = 0
    # A politica do strava so permite 100 requisicoes a cada 15min e uma requisicao pode voltar ate 200 registros
    #coloquei essa trava pra caso ele der problema aguardar os 15min pra continuar
    #o limite diario e 2k, mas estou ajustando pra rodar D-30 pra simular eventos de update, ninguem faz 2k de atividade e um mes
    # pelo menos n eu kkk entao n vai quebrar por isso

    while start_date < end_date:
        print(start_date, end_date)
        activities = []

        try:
            print("Extraindo atividades")
            activities = list(client.get_activities(after=start_date, limit=200))
            print(activities)
        except Exception as e:
            error = str(e)
            if '429' in error:
                print("⏳ Erro por limite de request excedido, aguardar 15min ⏳")
                time.sleep(16 * 60)
                print("⏳ Contando ⏳")
                activities = list(client.get_activities(after=start_date, limit=200))
                print("🏃 Continuando a extração 🏃")
                continue
        
        if activities:
            print("Extraçao iniciada, aguarde... ⏳")
            for activitie in activities:
                activitie_dict = activitie.model_dump()
                
                # hash que concatena todos as colunas pra saber se houve alguma modificacao
                json_string = json.dumps(activitie_dict, sort_keys=True, default=str)
                activitie_dict["hash_id"] = hashlib.md5(json_string.encode()).hexdigest()
                #timestamp depois pra n criar hash sempre diferente, manter padrao
                activitie_dict["timestamp"] = str(datetime.now())

                name_file_output_json = (
                    f"{str(activitie.start_date_local)[:4]}/"
                    f"{str(activitie.start_date_local)[5:7]}/"
                    f"{str(activitie.start_date_local)[:10]}-{activitie.id}-{str(datetime.now())[:10]}.json"
                )

                files_search = (
                    f"{str(activitie.start_date_local)[:4]}/"
                    f"{str(activitie.start_date_local)[5:7]}/"
                    f"{str(activitie.start_date_local)[:10]}-{activitie.id}-"
                )

                # Lista os objetos do bucket com o prefixo
                try:
                    old_files = list(s3_client.list_objects(os.getenv("BRONZE_BUCKET_USER_ACTIVITIES"), prefix=files_search, recursive=True))
                except (HTTPError, MaxRetryError, Exception) as e:
                    print(f"⚠️  Erro ao listar arquivos antigos para a atividade {activitie.id}: {e}")
                    # se não conseguir listar, trata como se não houvesse arquivo antigo
                    old_files = []

                #Se n tiver nenhum arquivo ele so coloca como insert e segue
                if not old_files:
                    activitie_dict["hub_action"] = "I"
                    print(f"Inserindo atividade {activitie.id}: hash {activitie_dict['hash_id']} - data {activitie.start_date_local}")
                    put_json_minio(s3_client, os.getenv("BRONZE_BUCKET_USER_ACTIVITIES"), name_file_output_json, activitie_dict)
                    time.sleep(2)
                    activitie_count = activitie_count + 1
                    continue
                else:
                    # Se existir verifica as modificacoes, se forem arquivos iguais ele continua a exec sem acao, se forem diferentes ele insere
                    # o novo como UPDATE
                    for file in old_files:
                        file = s3_client.get_object(file.bucket_name, file.object_name)
                        file = file.data.decode("utf-8")
                        file_old = json.loads(file)
                        if activitie_dict["hash_id"] != file_old["hash_id"] and activitie.start_date_local != ultimo_processado:
                            print("⚠️ Novo evento de arquivo existente ⚠️")
                            activitie_dict["hub_action"] = 'U'
                            try:
                                put_json_minio(s3_client, os.getenv("BRONZE_BUCKET_USER_ACTIVITIES"), name_file_output_json, activitie_dict)
                                atividade_alterada = atividade_alterada + 1
                            except (HTTPError, MaxRetryError, Exception) as e:
                                #Verifica se o arquivo foi inserido, se n foi salva no log de erros
                                if s3_client.get_object(os.getenv("BRONZE_BUCKET_USER_ACTIVITIES"), name_file_output_json):
                                    print(f"Arquivo {name_file_output_json}, inserido")
                                else:
                                    print(f"Arquivo {name_file_output_json}, não inserido, salvo no log de arquivos")
                                    save_log_error(activitie.id, activitie.start_date_local, e)
                            
                        else: continue
                            
                    set_key(str(ENV_PATH), "LAST_DATE_USER_ACTIVITIE", str(activitie.start_date_local)) 
                    print("Última data atualizada para:", str(activitie.start_date_local)) 

            if activitie.start_date_local == ultimo_processado:
                        start_date = ultimo_processado
                        print(" Nenhuma nova atividade encontrada, finalizando extração ⏳")
                        break
            else:
                    start_date = activitie.start_date_local + timedelta(seconds=1)
                    ultimo_processado = activitie.start_date_local
        else: 
            print("Náo ha mais atividades para serem inseridas.")

        print(start_date, ultimo_processado, activitie.start_date_local)
        print(f" ⏩ Atualizando data para {start_date} ⏩ ")
        
    print(f" 📋 {activitie_count} Atividades registradas, {atividade_alterada} atividades alteradas")
    print(f" 📋 {start_date} Ultima data registrada ")

#Verifica se o bucket esta vazio ou se é a primeira execucao full
list_obj_buc = list(s3_client.list_objects(os.getenv("BRONZE_BUCKET_USER_ACTIVITIES"), recursive=True))
if os.getenv("LAST_DATE_USER_ACTIVITIE") == "" or len(list_obj_buc) == 0:
    print("BUCKET VAZIO OU REPROCESSAMENTO FULL")
    formato = "%Y-%m-%d %H:%M:%S%z"
    date_init = datetime.strptime(os.getenv("FIRST_DATE_USER"), formato)
    insert_files_json_to_bronze(date_init)
    print(os.getenv("LAST_DATE_USER_ACTIVITIE"))
else:
    formato = "%Y-%m-%d %H:%M:%S%z"
    date_init = datetime.strptime(os.getenv("LAST_DATE_USER_ACTIVITIE"), formato) - timedelta(days=30)
    print("PROCESSAMENTO INCREMENTAL A PARTIR DE {}".format(date_init))
    insert_files_json_to_bronze(date_init)
    
print("✅ Processo de extração finalizado ✅")