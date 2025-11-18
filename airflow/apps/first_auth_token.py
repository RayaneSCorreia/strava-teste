from stravalib import Client
from dotenv import load_dotenv, set_key
from pathlib import Path
import os, logging

#garantindo o uso do refresh token gerado no first_auth
#garantindo o uso do refresh token gerado no first_auth
ENV_PATH = Path(__file__).resolve().parent / ".env"
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

env_path = Path(".env").resolve()
set_key(env_path,  "REFRESH_TOKEN", token_response["refresh_token"]) 
set_key(env_path,  "ACCESS_TOKEN", token_response["access_token"]) 
set_key(env_path,  "EXPIRES_AT", str(token_response["expires_at"]))
print(" ✅ Novo refresh token salvo ✅")