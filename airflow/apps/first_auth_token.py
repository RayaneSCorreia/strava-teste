from stravalib import Client
from dotenv import load_dotenv, set_key
from pathlib import Path
import os, logging

#garantindo o uso do refresh token gerado no first_auth
#Oculta warnings desnecessários
os.environ["SILENCE_TOKEN_WARNINGS"] = "true"
logging.getLogger("stravalib.util.limiter").setLevel(logging.ERROR)
#garantindo o uso do refresh token gerado no IF anterior
ENV_PATH = "/opt/airflow/apps/.env"
#garantindo o uso do refresh token gerado no first_auth
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)

#https://stravalib.readthedocs.io/en/v2.2/reference/client.html#athlete-methods

# Get the URL needed to authorize your application to access a Strava user's information.
client = Client()
auth_url = client.authorization_url(client_id= os.getenv("CLIENT_ID"),
                                    redirect_uri= os.getenv("REDIRECT_URI"),
                                    scope= ["read","read_all","profile:read_all","activity:read_all"],
                                    approval_prompt="auto" )

#Exchange the temporary authorization code (returned with redirect from Strava authorization URL) 
#for a short-lived access token and a refresh token (used to obtain the next access token later on).

print(" ⚠️  Visite a URL abaixo e autorize o app: ⚠️ ")
print(auth_url)
print("====================================================")
print("Após redirecionar, pegue a hash do parâmetro CODE e cole no input")
print("Esse processo será necessário apenas UMA VEZ por politica do Strava")
print("====================================================")
print("👉 Cole no campo abaixo o código que veio no redirect: ")
code = input().strip()
token_response = client.exchange_code_for_token(client_id=os.getenv("CLIENT_ID"),
                                                client_secret=os.getenv("CLIENT_SECRET"),
                                                code=code)

#https://stravalib.readthedocs.io/en/v2.2/reference/api/stravalib.client.Client.refresh_access_token.html
print(token_response["expires_at"])
ENV_PATH = "/opt/airflow/apps/.env"
#garantindo o uso do refresh token gerado no first_auth
print(f"Usando o arquivo de variaveis em: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH, override=True)
set_key(ENV_PATH,  "REFRESH_TOKEN", token_response["refresh_token"]) 
set_key(ENV_PATH,  "ACCESS_TOKEN", token_response["access_token"]) 
set_key(ENV_PATH,  "EXPIRES_AT", str(token_response["expires_at"]))

print(" ✅ Novo refresh token salvo ✅")
