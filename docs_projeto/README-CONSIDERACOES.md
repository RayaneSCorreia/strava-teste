# 📌 Considerações Técnicas do Projeto  
### Pipeline Strava → MinIO → Airflow → Bronze/Silver → Anotações (Label Studio)  
### Visão geral, decisões, dificuldades e próximos passos

Este documento consolida **todos os aprendizados, problemas encontrados, decisões técnicas e melhorias planejadas** durante a construção do pipeline de ingestão e processamento de dados do Strava, incluindo o módulo de anotações via Label Studio.

O objetivo é registrar o histórico técnico e servir de base para evoluções futuras.

---

# 🧠 1. Motivação e Contexto

O projeto começou com a necessidade de:

- Capturar **minhas atividades do Strava** diariamente.  
- Registrar inconsistências de treino:
  - atividades em esteira sem velocidade,
  - registros inconsistentes no app,
  - erros de GPS,
  - dados incompletos.
- Criar um pipeline que permitisse:
  - ingestão,  
  - padronização,  
  - anotações manuais,  
  - preparação para dashboards.

Durante a exploração da API do Strava surgiram desafios:

- Atividades podem ser **alteradas ou deletadas** → necessidade de estratégia incremental.  
- Strava não fornece *timestamps reais de alteração*.  
- Estrutura JSON extremamente complexa → muitas normalizações.  
- Necessidade de registrar metadados próprios (transação, processamento etc.).  
- Identificação da necessidade futura de **Webhook + Pub/Sub** para garantir frescor dos dados.

---

# 🚨 2. Problemas Encontrados

## 🪣 MinIO
- Ocorrência de **erros de deadlock** durante gravações (`Write Failed (concurrent write)`).
- Mesmo com erro, o arquivo era inserido → incerteza no estado.
- Problemas relacionados a paralelismo e escrita simultânea.
- Diferenças entre Windows e Mac exigiram ajustes.

## 📦 Bronze – ausência de timestamp real
- Strava não fornece `updated_at` confiável em atividades.  
→ Solução temporária: usar **data de criação do arquivo na Bronze** como timestamp técnico para Silver.

## 🟧 Label Studio
- SDK extremamente instável:
  - Não extraía token,
  - incompatibilidade com Airflow,
  - quebra de numpy/pandas.
- Solução: **abandonar SDK e usar `requests`**, trocando o PAT via `/api/token/refresh`.
- Interface confusa, documentação fraca.
- Necessidade de salvar interface separadamente → virou um micro ETL à parte.

## 🛠 Desalinhamento entre ambientes (Local vs Container)
- Variáveis de ambiente espalhadas → divergências.
- Algumas libs funcionavam somente fora do container.
- Conflitos de versão:
  - pandas  
  - numpy  
  - label-studio-sdk  
  - airflow-python

## 📚 Documentação dispersa
Trabalhar com muitas ferramentas ao mesmo tempo gerou curva de aprendizado:

- Airflow  
- MinIO  
- Strava API  
- Label Studio  

Demandou tempo, testes e retrabalho.

---

# 🛠️ 3. Decisões e Soluções Implementadas

## ✔ 1. Requests ao invés do SDK do Label Studio
O SDK tornou o processo inviável.  
→ Substituído por `requests` + refresh do token.  
Estável, simples e controlado.

## ✔ 2. Bronze com Pandas
Volume pequeno por dia → Spark seria overkill.  
Pandas atendeu:
- menor overhead,
- velocidade,
- simplicidade,
- menor carga cognitiva.

## ✔ 3. Estratégia D-30
Para garantir atualização de atividades antigas (likes, comentários, correções):

- processamento sempre com captação dos últimos **30 dias**.

## ✔ 4. Projeção de arquitetura futura: Webhook + Pub/Sub
Baseado nas aulas de microserviços:
- Webhook Strava → Pub/Sub → pipeline incremental verdadeiro.
- Não implementado por conta do escopo e tempo.

## ✔ 5. Airflow como Orquestrador
- Fácil de usar,
- Difícil de configurar container + libs + rede + MinIO + Label Studio.

---

# 🔎 4. Dificuldades Gerais

## 🔧 Compatibilidade entre libs
- conflitos entre numpy, pandas, label-studio-sdk e airflow,
- versões específicas quebravam o ambiente,
- necessidade de pinagens muito precisas.

## 🌀 Strava API
- JSON com muitos níveis,
- campos opcionais,
- dados inconsistentes,
- normalização trabalhosa.

## 🟧 Label Studio
- Documentação pobre,
- Interface confusa,
- SDK inconsistente,
- funcionalmente virou um novo fluxo ETL.

---

# ⚠️ 5. Déficits Identificados

## ❌ Módulo de User
- `updated_at` não representa alteração real.
- Prejudica incremental.
- Faltam transformações específicas.

## ❌ Ausência de timestamp técnico
- Falta de campo como `hub_transaction_date` na Bronze/Silver:
  - impossível detectar incremental,
  - obrigatório rodar full load todo dia.

## ❌ Particionamento insuficiente
- Particionamento apenas por ano/mês.
- O ideal seria por:
  ```
  hub_transaction_date = yyyy/mm/dd
  ```

## ❌ Reprocessamento limitado
- Sem lógica de reprocessar apenas uma pasta/partição.
- Pipelines ainda dependem de full load manual.

## ❌ Schema rígido
- Atividades com estrutura fixa → qualquer novo campo quebra a DAG.
- Ideal: abordagem **schema-on-read**.

---

# 🚀 6. Melhorias Planejadas (Próxima Sprint)

## ✔ Centralizar conexões
- MinIO client
- funções utilitárias (upload, download)
- Strava API

## ✔ Centralizar variáveis de ambiente
- 1 único ponto de carga,
- mesma referência para containers e local.

## ✔ Incremental REAL
- criar `hub_transaction_date`,
- salvar última data processada,
- processar apenas delta.

## ✔ Ajustar módulo de users
- Criar lógica própria para identificar mudanças reais.

## ✔ Suporte a schema dinâmico
- Flatten automático,
- tratamento para novos campos,
- maior resiliência.

## ✔ Reprocessamento por pasta/partição
- gatilho para `ano/mes/dia`.

## ✔ Particionamento avançado na Bronze e Silver
```
s3://.../bronze/activities/hub_transaction_date=2025/11/16
```

## ✔ Revisar módulo de atividades
- Extrair mais metadados:
  - gear
  - device
  - heart rate
  - GPS
  - inconsistências de treino

## ✔ Preparar ambiente para AWS
- Glue
- S3
- Lambda
- EventBridge
- Step Functions
- Athena
- Redshift (opcional)
- Observabilidade

---

## 🧱 Evolução Futura: Data Quality com Great Expectations

O objetivo desta evolução é adicionar validação estruturada e automatizada nas camadas **Bronze**, **Silver** e **Gold**, elevando a confiabilidade do pipeline e garantindo governança e rastreamento de qualidade.

---

### 🎯 Benefícios Esperados

- ✔ Detectar dados inválidos antes de chegarem na camada Gold  
- ✔ Garantir rastreabilidade e documentação automática da qualidade dos dados  
- ✔ Gerar relatórios HTML detalhados via Great Expectations  
- ✔ Executar checkpoints diretamente nas DAGs do Airflow  
- ✔ Aderência a práticas modernas de DataOps e Data Quality Engineering  

---

### 📌 Onde o GE Entraria no Pipeline

1. **Após a ingestão da Bronze**  
2. **Antes da escrita na Silver**  
3. **Antes da publicação para o BI (camada Gold)**  

---

### 🧪 Testes Planejados

- **Schema Validation**
  - Colunas obrigatórias do Strava
  - Colunas opcionais com presença variável

- **Regras de Negócio**
  - Pace dentro de limites plausíveis  
  - Distância > 0 em treinos externos  
  - HR dentro de faixas realistas  

- **Integridade e Tipagem**
  - Tipos corretos (int, float, string, datetime)
  - Datas convertidas corretamente  

- **Consistência**
  - Registros duplicados (activity_id)
  - JSON estruturado

---

### 🚀 Integração com o Airflow

- Execução de `ge.checkpoint.run` dentro das DAGs  
- Validação obrigatória antes de avançar camadas  
- “Stop the line” quando a qualidade for crítica  
- Logging e rastreabilidade via OpenLineage/Marquez  

---

# 🧩 7. Conclusão

Apesar dos desafios — que envolveram compatibilidade, múltiplas ferramentas, documentações fragmentadas e decisões arquiteturais — o projeto entregou:

- Pipeline funcional,
- Extração contínua de atividades,
- Anotações integradas,
- Estrutura Bronze/Silver organizada,
- Aprendizados reais sobre orquestração distribuída,
- Base sólida para evoluções como:
  - incremental real,
  - delta lake,
  - webhook + pub/sub,
  - deploy em cloud.

O projeto está pronto para evoluir para uma próxima sprint com arquitetura mais robusta e moderna.
