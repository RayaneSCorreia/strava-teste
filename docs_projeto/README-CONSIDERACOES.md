
# 📌 Considerações Técnicas do Projeto  
### Pipeline Strava → MinIO → Airflow → Bronze/Silver → Anotações (Label Studio)  
### Visão geral, decisões, dificuldades, aprendizados e próximos passos

Este documento consolida **todos os aprendizados, problemas encontrados, decisões técnicas e melhorias planejadas** durante a construção do pipeline de ingestão e processamento de dados do Strava, incluindo o módulo de anotações via Label Studio.

O objetivo é registrar o histórico técnico e servir de base para evoluções futuras.

---

# 🧠 1. Motivação e Contexto  

O projeto começou com a necessidade de:

- Capturar **minhas atividades do Strava** diariamente.  
- Registrar inconsistências comuns de treino:
  - esteira sem velocidade,  
  - corridas sem GPS,  
  - atividades duplicadas,  
  - erros do app do Strava.  
- Criar um pipeline capaz de:
  - ingestão incremental,
  - padronização,
  - enriquecimento com metadados,
  - anotações manuais via Label Studio,
  - disponibilização para dashboards (Metabase/Power BI).

Durante a exploração da API do Strava, surgiram descobertas importantes:

- Atividades podem ser **alteradas ou deletadas**, exigindo estratégia incremental.  
- Os eventos de alteração **não possuem timestamp confiável**.  
- A API do Strava é bem documentada, mas a **estrutura JSON é extremamente complexa**.  
- Idealmente deveria existir um **Webhook + Pub/Sub** para deletions e updates.  

---

# 🚨 2. Problemas Encontrados

## 🪣 2.1 MinIO
- Erros de **deadlock** que apareciam mesmo com a escrita concluída.  
- Diferenças de comportamento entre Windows e Mac exigiram ajustes de rede e permissões.  
- Alguns erros eram randômicos e consumiam bastante tempo de troubleshooting.

## 📦 2.2 Bronze – Ausência de timestamp real
- O Strava não disponibiliza `updated_at` confiável para atividades.
- Solução temporária → usar **data de criação do arquivo Bronze** como timestamp técnico.

## 🟧 2.3 Label Studio
- SDK completamente instável:
  - quebras constantes,
  - incompatibilidade com Airflow,
  - impossibilidade de extrair token via SDK,
  - dependências conflitando com numpy/pandas.
- Necessidade de salvar layout YAML fora do Label Studio (interface separada).
- Se tornou um fluxo ETL independente dentro do projeto.

## 🔧 2.4 Compatibilidade entre containers e ambiente local
- Variáveis de ambiente não estavam padronizadas entre:
  - local,
  - Airflow,
  - scripts,
  - Label Studio.
- Divergência de libs em cada container.
- Ambiente Python do Airflow extremamente sensível a versões de pandas/numpy.

## 🔁 2.5 Full Load diário desnecessário
- Falta de campo técnico (`hub_transaction_date`) e ausência de incremental.
- Todo o pipeline roda **full** diariamente, gerando overhead e retrabalho.

## 🌀 2.6 API Strava
- JSON extremamente profundo e dinâmico.
- Campos opcionais variando por atividade.
- Diferenças grandes entre atividades indoor/outdoor → fontes de erros.

## 🗂 2.7 Power BI
- Não funciona nativamente no MacBook.  
→ análises tiveram que ser feitas no Metabase, aumentando curva de aprendizado.

## 🧱 2.8 Delta Lake
- Tentativas de uso do Delta Lake dentro dos containers geraram:
  - conflitos de versão,
  - erros de Java,
  - incompatibilidades com o Spark do Airflow.
- Decisão final: manter apenas Spark em Parquet; Delta como melhoria futura.

## 🐘 2.9 Airflow – Banco de Metadados
- Airflow não aceita replicação de logs entre máquinas.
- Ao trocar de máquina, foi necessário apagar pasta `/airflow/postgres_data`, zerando dashboards no Metabase.
- Solução futura: criar **bucket de dumps** para histórico do Airflow.

---

# 🛠️ 3. Decisões Tomadas Durante o Projeto

## ✔ 3.1 Substituição do SDK do Label Studio por `requests`
O SDK tornava o ambiente instável e inutilizável.  
A solução:  
- Autenticação via `/api/token/refresh`,  
- Requests diretos → Código mais previsível e seguro.

## ✔ 3.2 Bronze em Pandas
Como o volume diário é pequeno:

- Pandas é mais leve,
- Evita overhead do Spark,
- Facilita debug,
- Evita conflitos pesados do Delta.

## ✔ 3.3 Estratégia D-30
Mesmo sem incremental real:

- Cada execução coleta alterações retroativas dos últimos **30 dias**  
  (likes, comentários, correções, mudanças manuais).

## ✔ 3.4 Spark apenas na camada Gold
- Menos containers,
- Menos problemas de dependência,
- Menos efeito colateral no Airflow.

## ✔ 3.5 Design Modular
- Separação clara entre ingestão, bronze, silver e anotações.
- Permitiu depurar cada módulo sem derrubar o ambiente completo.

## ✔ 3.6 Documentação com foco no porquê
O desenho inicial não cobria todos os casos.  
A documentação foi evoluindo à medida que novos desafios apareciam.

---

# 🔍 4. Aprendizados Importantes

## 🎓 4.1 Containers exigem manutenção ativa
- Saber “entrar” no container e rodar comandos internos foi essencial.
- Instalar libs diretamente dentro do Airflow evitou rebuilds desnecessários.

## 📚 4.2 Pesquisar na documentação oficial > IA em certos casos
- A API do Strava e o Label Studio tinham especificações importantes que a IA não detalhava corretamente.

## 🧩 4.3 A arquitetura cresce sozinha
- Iniciar pequeno → tudo parece simples.  
- Conforme surgem inconsistências, novos módulos e decisões técnicas surgem naturalmente.  
- A arquitetura se expande baseada nas necessidades reais.

## 🪙 4.4 Orquestração distribuída exige disciplina
- Logs separados,
- Múltiplos containers,
- Várias redes Docker,
- Variáveis em múltiplos lugares.  
O processo de padronização foi essencial.

---

# ⚠️ 5. Déficits Mapeados

## ❌ 5.1 Módulo de Users
- `updated_at` não reflete alterações reais de perfil.
- Lógica precisa ser revista.

## ❌ 5.2 Falta de timestamp técnico na Bronze
Com isso:
- Não existe incremental verdadeiro,
- Full diário é obrigatório.

## ❌ 5.3 Falta de particionamento avançado
Hoje o particionamento é limitado.  
Ideal:
```
bronze/activities/hub_transaction_date=2025/11/19
```

## ❌ 5.4 Esquema rígido
Campos opcionais do Strava quebram pipeline.  
Necessário schema-on-read.

## ❌ 5.5 Notebooks de teste despadronizados
- Serviram para experimentação,
- Mas precisam ser reorganizados.

## ❌ 5.6 Airflow sem histórico portátil
- Logs não sobrevivem à troca de máquinas.

---

# 🚀 6. Melhorias Planejadas (Próxima Sprint)

## 🔧 Engenharia de Dados
- Padronizar nomeclaturas (inglês)
- Centralizar conexões MinIO/Strava
- Centralizar carregamento de variáveis .env
- Criar incremental verdadeiro
- Módulo de Users revisado
- Pipeline resiliente a novos campos
- Particionamento avançado por hub_transaction_date
- Reprocessamento seletivo por pasta
- Exclusão automática de partições antes de reprocessar
- Silver já em Delta
- Gold com uso de MERGE
- Delta Lake com compatibilidade garantida
- Ajustar módulos que geram pastas fora do padrão

## 🔌 Infraestrutura e Arquitetura
- Novo bucket para histórico do Airflow (dump)
- Ambiente de testes isolado e funcional
- Schedular com datas relativas no Airflow
- Container de Spark dedicado (modelo similar ao usado no Windows)

## ☁️ Cloud (AWS)
- Glue  
- S3  
- Lambda  
- EventBridge  
- Step Functions  
- Pub/Sub de verdade  
- Athena  
- Redshift (opcional)

## 🛰 Eventos (Webhook)
- Capturar updates e deletes em tempo real
- Sistema incremental perfeito

---

# 🧪 7. Data Quality – Integração com Great Expectations

## 🎯 Objetivos
- Garantir qualidade entre Bronze → Silver → Gold
- Detectar anomalias (pace, distância, HR, duplicados)
- Criar checkpoints integrados ao Airflow
- Gerar relatórios HTML automáticos
- Implementar DataOps moderno e governança

## 🔍 Onde validar?
1. Após Bronze  
2. Antes da Silver  
3. Antes da Gold

---

# 🧩 8. Conclusão

Apesar dos inúmeros desafios — envolvendo compatibilidade, containers, libs conflitantes, API complexa e múltiplas ferramentas — o projeto entregou:

- Pipeline funcional  
- Bronze e Silver estáveis  
- Integração com anotações  
- Estrutura robusta para evoluções  
- Aprendizado REAL de engenharia de dados na prática  
- Base pronta para incremental, Delta e Cloud  

A próxima sprint será focada em **robustez, escalabilidade e governança**.
