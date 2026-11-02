<div align="center">

# 🛠️ Service Desk Lakehouse

**Pipeline de dados ponta a ponta que transforma chamados de Service Desk em relatório gerencial — na arquitetura medallion, com PySpark + Delta Lake em AWS S3.**

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.0-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-lakehouse-00ADD8)
![AWS S3](https://img.shields.io/badge/AWS%20S3-storage-569A31?logo=amazons3&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-job-FF3621?logo=databricks&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-serving-4169E1?logo=postgresql&logoColor=white)
![pytest](https://img.shields.io/badge/pytest-19%20testes-0A9EDC?logo=pytest&logoColor=white)
![CI](https://img.shields.io/badge/GitHub%20Actions-CI-2088FF?logo=githubactions&logoColor=white)

</div>

---

## O problema

Relatório gerencial de chamados montado **à mão**: alguém exporta dados de mais de uma fonte, cola em planilha, cruza categoria com SLA e com técnico, confere e formata. É um trabalho de **~4 horas**, refeito a cada ciclo, sujeito a erro humano e impossível de auditar — e a planilha "quebra" em silêncio quando o dado vem sujo (duplicatas, caixa inconsistente, duração negativa, satisfação em branco).

## A solução

Um **lakehouse** na arquitetura medallion que ingere o dado cru, limpa, modela e publica as métricas automaticamente:

```
 raw (JSON)
   │  ingestão fiel + metadados de proveniência
   ▼
 BRONZE (Delta)  ──  dado como chegou, versionado
   │  limpeza, tipagem, dedup por ticket_id, flag de SLA
   ▼
 SILVER (Delta)  ──  dado confiável, particionado por ano/mês
   │  Spark SQL: CTEs + window functions (RANK, AVG OVER, média móvel 7d)
   ▼
 GOLD (Delta)    ──  fato_chamados + dim_tecnico + marts (SLA, volume, KPIs)
   │  carga da camada de consumo
   ▼
 PostgreSQL  ──▶  Dashboard (Streamlit)
```

A mesma base de código roda em três lugares, só mudando configuração:
- **local** (`parquet`) para desenvolver e no CI — sem dependência de nuvem;
- **local** (`delta`) com Postgres via Docker;
- **AWS S3 + Databricks** (`delta`) em produção — ver [`notebooks/databricks_job.py`](notebooks/databricks_job.py).

## O resultado

- Pipeline **roda ponta a ponta** (`generate → bronze → silver → quality → gold → postgres`) e recalcula o relatório em **segundos**, no lugar das ~4h manuais.
- Dado sujo é tratado de forma explícita: numa execução de exemplo, **3.071 registros brutos → 2.962 na silver** após deduplicação e remoção de durações inválidas.
- **Gate de qualidade** entre camadas (unicidade de chave, domínio de prioridade, faixa de satisfação, sem duração negativa) que **para o pipeline** antes de publicar dado ruim.
- **19 testes automatizados (pytest)** e **CI no GitHub Actions** rodando testes + smoke do pipeline a cada push.
- Camada gold entrega o relatório pronto: KPIs gerais, aderência a SLA por categoria, produtividade por técnico (com ranking) e volume diário com média móvel de 7 dias.

> **Nota sobre os dados:** a origem real (chamados corporativos) está sob NDA e não pode ser publicada. O projeto inclui um **simulador** ([`generate_data.py`](src/servicedesk_pipeline/generate_data.py)) que recria a estrutura e as imperfeições típicas desses dados — sem expor nada de cliente.

---

## 🚀 Como rodar

### Opção A — demonstração local, sem nada instalado além do Python (Parquet)

```bash
pip install -r requirements.txt
make pipeline-local          # roda tudo em ./data/lakehouse, formato parquet, sem Postgres
make test                    # 19 testes
```

### Opção B — local com Delta Lake + Postgres + dashboard

```bash
docker compose up -d postgres          # camada de consumo
cp .env.example .env                    # TABLE_FORMAT=delta
pip install -r requirements.txt
python -m servicedesk_pipeline.run_pipeline
streamlit run dashboard/app.py          # abre o painel em http://localhost:8501
```

> O primeiro run com `TABLE_FORMAT=delta` baixa os JARs do Delta Lake via Maven (precisa de internet).

### Opção C — produção: AWS S3 + Databricks

Adicione o repositório ao Databricks (*Repos*), configure o acesso ao S3 por *instance profile* e rode [`notebooks/databricks_job.py`](notebooks/databricks_job.py) como um **Job**. As tabelas Delta ficam em `s3a://<bucket>/servicedesk/{bronze,silver,gold}`.

## 🧱 Estrutura

```
src/servicedesk_pipeline/
├── config.py            # configuração (YAML + overrides de ambiente) e resolução de caminhos
├── spark_session.py     # SparkSession com Delta e s3a opcionais
├── io_utils.py          # leitura/escrita agnóstica de formato (delta|parquet)
├── generate_data.py     # simulador de chamados (dados sintéticos realistas)
├── ingest_bronze.py     # raw -> bronze
├── transform_silver.py  # bronze -> silver (limpeza, dedup, SLA)
├── transform_gold.py    # silver -> gold (Spark SQL: dim/fato + marts)
├── quality_checks.py    # gate de qualidade entre camadas
├── load_postgres.py     # gold -> PostgreSQL (camada de consumo)
└── run_pipeline.py      # orquestrador (CLI)
dashboard/app.py         # painel Streamlit
sql/gold_models.sql      # SQL analítico documentado (espelha o transform_gold)
tests/                   # 19 testes (pytest)
notebooks/databricks_job.py   # entry-point do Job no Databricks (S3/Delta)
.github/workflows/ci.yml # CI: testes + smoke do pipeline
```

## 🧰 Stack

| Camada | Tecnologia |
|---|---|
| Processamento | PySpark 4, Spark SQL |
| Lakehouse / formato | Delta Lake (Parquet como fallback sem JVM externa) |
| Storage | AWS S3 (`s3a`) · sistema de arquivos local · MinIO (S3 local) |
| Execução gerenciada | Databricks (Job) |
| Consumo | PostgreSQL · Streamlit |
| Qualidade / CI | pytest · GitHub Actions |

## 📄 Licença

[MIT](LICENSE).
