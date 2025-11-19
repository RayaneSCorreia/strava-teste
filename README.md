
# 🏋️‍♀️ Data Lakehouse – Pipeline Strava + Observabilidade + Label Studio

## 📝 Descrição Resumida do Projeto

Este projeto implementa um **Data Lakehouse completo**, responsável por coletar, armazenar, processar e enriquecer dados esportivos provenientes da **API do Strava**. A solução integra também anotações manuais feitas no **Label Studio**, permitindo validar e classificar atividades com maior precisão.

A arquitetura utiliza **Apache Airflow**, **MinIO (S3)**, **Pandas/Spark**, **PostgreSQL** e **Metabase** para construir um pipeline totalmente orquestrado, reprodutível e observável — desde a ingestão dos dados brutos até a disponibilização de camadas analíticas (Bronze, Silver e Gold) consumidas por dashboards.

Este projeto demonstra, na prática, uma abordagem sólida de **Big Data & DataOps**, aplicando boas práticas de versionamento, governança, padronização e automação de pipelines.


Este projeto implementa um **Data Lakehouse completo**, incluindo:

- **Coleta de dados da API Strava**
- **Label Studio para anotação e enriquecimento**
- **Coleta de dados da API Label Studio**
- **Armazenamento em camadas (Bronze, Silver, Gold)**
- **Padronização e limpeza dos dados**
- **Orquestração com Apache Airflow**
- **Armazenamento de objetos com MinIO (S3)**
- **Dashboard de monitoramento com Metabase**
- **Dashboard de atividades do Strava com Metabase**
- **Ambiente totalmente reprodutível via Docker Compose**

---
## ✅ Requisitos Mínimos para Execução

### **1. Conta no Strava**
Necessária para:

- Criar o aplicativo do Strava  
- Gerar `CLIENT_ID` e `CLIENT_SECRET`  
- Gerar o *Authorization Code* (usado apenas na primeira execução)  
- Permitir que o pipeline colete automaticamente suas atividades  

---

### **2. Docker + Docker Compose**

| Ferramenta       | Versão mínima |
|------------------|---------------|
| **Docker**       | ≥ 24.x        |
| **Docker Compose** | ≥ 2.20      |

---

### **3. Portas Disponíveis no Host**

| Serviço            | Porta |
|-------------------|-------|
| Airflow Webserver | **8080** |
| Label Studio      | **8081** |
| MinIO Console     | **9011** |
| MinIO API         | **9000** |
| Metabase          | **3000** |


## 📐 Arquitetura Geral

```
─▶  Extrações:
   Strava API  
   Label Studio (anotações) 

─▶  Orquestração :
   Airflow ─▶ Bronze (MinIO)
               │
               ├─▶ Silver (Parquet particionado)
               │
               ├─▶ Gold (dim/fact)
               │
               ├─▶ Postgres Airflow ─▶ Metabase Dashboard de Monitoramento
               │
               └─▶ Postgres BI ─▶ Metabase Dashboard Negocial

```

---

## 🧱 Tecnologias Utilizadas

| Tecnologia      | Função |
|----------------|--------|
| Apache Airflow | Orquestração ETL / ELT |
| MinIO (S3)     | Data Lakehouse |
| Pandas + Parquet | Camada Silver |
| PostgreSQL BI  | Consumo pelo Metabase |
| Metabase       | Dashboards |
| Label Studio   | Anotação de dados |
| Docker Compose | Infraestrutura como código |

---

## 📂 Estrutura do Projeto

```bash
📁 strava-pipeline-dataops/
│
├── airflow/                            
│   ├── dags/
│   │   ├── 🌀 pipeline_strava_activities.py          
│   │   ├── 🌀 pipeline_label_studio.py 
│   │   ├── 🌀 pipeline_strava_user.py     
│   │
│   ├── apps/                           
│   │   ├── 01-bronze/
│   │   │   ├── 📜 extract_api_strava_user.py        
│   │   │   ├── 📜 extract_api_strava_user_activities.py
│   │   │   ├── 📜 extract_api_label_studio_strava.py 
│   │   │
│   │   ├── 02-silver/
│   │   │   ├── 📜 extract-bronze-to-silver-strava-activities.py        
│   │   │   ├── 📜 extract-bronze-to-silver-strava-label_studio.py
│   │   │   ├── 📜 extract-bronze-to-silver-strava-user.py
│   │   │
│   │   ├── 03-gold/
│   │   │   ├── 📜 fact_user_activities_strava.py                                  
│   │   │
│   │   ├── 04-LoadBi/
│   │   │   ├── 📜 gold_activitties_to_postgres_bi.py   
│   │   │
│   │   ├── 05-DataQuality/
│   │   │   ├── 📜 data_quality_gold.py                   
│   │
│   ├── logs/
│   ├── plugins/
│   ├── .env/
│   ├── 📜 first_auth_token.py    
│
├── minio/
│   ├── data/                       
│   │   ├── 🪣 bronze-api-strava-json/              
│   │   ├── 🪣 bronze-api-label-studio-json/        
│   │   ├── 🪣 bronze-log-files/                    
│   │   ├── 🪣 silver-strava-activities-parquet/  
│   │   ├── 🪣 silver-strava-user-parquet/    
│   │   ├── 🪣 silver-label-studio-parquet/         
│   │   ├── 🪣 gold-strava-acitivities-labels-delta/                
│   │   ├── 🪣 gold-strava-user-delta/                      
│
├── labelstudio/                      
├── metabase-data/                       
├── postgres_bi_data/
├── postgres_data/                        
│ 
├── 🐳 docker-compose.yml                 
├── 🐳 Dockerfile.airflow                 
├── requirements.txt           
├── .gitignore
├── template_label_studio.yml         
└── README.md                           
```

---


# 📂 Estrutura dos Buckets

# 🥉 Bronze Layer
- Dados brutos da API Strava e Label Studio  
- Estrutura apenas organizada por ano/mês/dia  
- Erros registrados em `bronze-log-files/`

A Bronze contém os arquivos **raw**, sem transformação, organizados por:

```
ano/mes/dia/arquivo.json
```

Cada atividade vem com:
- `hash_id`  - criado na extração
- `timestamp` - criado na extração
- dados originais da API

# 🥈 Silver Layer

- Seleção de último registro por atividade
- Padronização via `padronizar_atividade_bronze()`  
- Deduplicação por `hash_id`  
- Salvamento em formato **Parquet**  
- Particionamento:  

```
silver-strava-user-activities-parquet/
   2025/
      11/
         atividades_2025-11.parquet
```

# 🥇 Gold Layer
- Fato Atividades
- Dim Usuários
- Unificação Strava + Label Studio

Modelo analítico:

- `gold-fact-activities`

Usados como fonte do Metabase.

---

# 📦 Orquestração – Apache Airflow

Funções:

- Coleta das atividades do Strava  
- Coleta do usuário  
- Coleta e padronização de anotações do Label Studio  
- Padronização Bronze → Silver  
- Silver → Gold  
- Monitoramento e logs  
- Input em SGBD

---

# 🌐 Acesso aos Serviços

| Serviço | URL |
|---------|-----|
| **Airflow Webserver** | http://localhost:8080 |
| **MinIO Console** | http://localhost:9011 |
| **Metabase** | http://localhost:3000 |
| **Label Studio** | http://localhost:8081 |

---

# 🔑 Configurações obrigatorias – Strava & Label Studio

Algumas etapas do pipeline precisam de tokens de acesso.

---

## 🟧 1. Token do Strava (API)

Para usar a API do Strava:

1. Acesse:  
   https://www.strava.com/settings/api  
2. Crie seu aplicativo.  
3. O Strava exibirá:  
   - Client ID  
   - Client Secret  
4. Autorize o app para obter o “code”.

###  ⚠️ IMPORTANTE — Execução Inicial Obrigatória

Antes de rodar qualquer DAG do pipeline, é **obrigatório executar apenas uma vez** o script:

```
airflow/apps/first_auth_user.py
```

### 🔍 Por que isso é necessário?

O Strava exige um processo inicial para converter o **authorization code** em um **refresh token permanente**.  
Esse `refresh_token` permite:

- regeneração automática do `ACCESS_TOKEN`
- evitar expiração manual
- pipeline 100% autônomo

Após isso:

✔ **nunca mais é necessário autenticar manualmente**  
✔ o pipeline sempre renova o token sozinho

## 🟥  2. Token do Label Studio (PAT)

Para gerar o token:

1. Abra:  
   http://localhost:8081  
2. Vá em **Account & Settings**  
3. Clique em **Access Tokens**  
4. Gere um novo token

📌 O Label Studio gera um **Personal Access Token (PAT)**.  
Esse token precisa ser enviado no formato correto pelo pipeline (via cabeçalho de autenticação), mas a geração é feita **totalmente via interface**, sem comandos.

### ⚠️ IMPORTANTE — ETemplate de Anotação — Label Studio

 projeto inclui o arquivo:

```
docs_projeto/template_label_studio.yml
```

Este arquivo contém o **layout oficial de anotação**, incluindo:

- campos esperados
- regras de input
- estrutura de rótulos
- classificação de consistência
- marcação de erro (GPS, HR, pace incoerente)
- classificação do tipo de atividade

💡 **Esse template pode ser carregado diretamente no Label Studio**  
em *Settings → Labeling Interface → Import → YAML*.

---

# 📊 Dashboard Público do Metabase

O dashboard de monitoramento já vem pronto dentro da pasta **metabase-data**, que contém o banco interno do Metabase.

### 🔗 Link público (funciona em qualquer máquina que subir o projeto)

**http://localhost:3000/public/dashboard/b53236fe-88cc-47ad-aeb5-26aee8ae0fd9**

O dashboard Negocial já vem pronto dentro da pasta **metabase-data**, que contém o banco interno do Metabase.

### 🔗 Link público (funciona em qualquer máquina que subir o projeto)

**http://localhost:3000/public/dashboard/b53236fe-88cc-47ad-aeb5-26aee8ae0fd9**

### 🧠 Por que funciona?
O link público e o dashboard são salvos no volume:

```
./metabase-data/
```

Isso garante:

- Reprodutibilidade total  
- Dashboard sempre restaurado  
- Configurações preservadas  

---



# 📁 Onde colocar os tokens

Arquivo:

```
airflow/.env
```

---

## # 🔧 Variáveis de Ambiente — `.env` oficial

---

```bash
# STRAVA
REFRESH_TOKEN=''
ACCESS_TOKEN=''
EXPIRES_AT=''
CLIENT_ID=
CLIENT_SECRET=''
REDIRECT_URI='http://localhost/exchange_token'

# RANGE DE COLETA
LAST_UPDATED_USER=''
DATA_INICIO="2022-01-12"
DATA_FIM="2025-11-09"

# MINIO — ENDPOINT PARA OS CONTAINERS
MINIO_ENDPOINT="http://minio:9000"
MINIO_USER="minioadmin"
MINIO_PASS="minioadmin"

# DATAS DO USUÁRIO
FIRST_DATE_USER=''
LAST_DATE_USER_ACTIVITIE=''

# LABEL STUDIO
LABEL_STUDIO_TOKEN=''
LABEL_STUDIO_URL="http://label-studio:8080"

# BUCKETS
BRONZE_BUCKET_USER="bronze-api-strava-user-json"
BRONZE_BUCKET_USER_ACTIVITIES="bronze-api-strava-user-activities-json"
BRONZE_BUCKET_LABEL_STUDIO="bronze-api-label-studio-json"

SILVER_BUCKET_USER="silver-strava-user-parquet"
SILVER_BUCKET_USER_ACTIVITIES="silver-strava-user-activities-parquet"
SILVER_BUCKET_LABEL_STUDIO="silver-label-studio-parquet"

# TIPOS DE PROCESSO
TYPE_PROCESS_USER_ACTIVITIES="FULL"
TYPE_PROCESS_USER="FULL"

# DATAS ZERADAS PARA FULL LOAD MANUAL
LAST_PROCESSED_START_DATE_USER=""
LAST_PROCESSED_START_DATE_USER_ACTIVITIES=""
LAST_PROCESSED_START_DATE_LABEL_STUDIO=""
```

---
---

# 🛠 Como rodar o projeto

```bash
docker compose up -d
```

Tudo sobe automaticamente:
- Airflow  
- MinIO  
- PostgreSQL BI  
- Label Studio  
- Metabase  

---

## ✅ Passos para iniciar o ambiente:

### 1️⃣ Inserir as variáveis no arquivo `.env`
Preencha o arquivo:

```
airflow/.env
```

com suas credenciais:
- Strava API  
- Label Studio  
- Buckets  
- Endpoint interno do MinIO (`http://minio:9000`)  
- Datas de processamento (podem iniciar vazias)

### 2️⃣ Executar o `first_auth_user.py` (somente 1 vez)
Este script obtém o **refresh token permanente** do Strava, necessário para que o pipeline renove automaticamente o `access_token`.

```bash
python airflow/apps/first_auth_user.py
```

Após isso, **nunca mais é necessário autenticar manualmente**.

### 3️⃣ Verificar se o ambiente está acessível
Acesse os serviços para confirmar que estão operacionais:


Quando tudo estiver disponível, o pipeline está pronto para rodar.

---


# 👩‍💻 Autora

Rayane Correia — Analytics Engineer | Pós-graduação em Engenharia de Dados – UNIFOR
