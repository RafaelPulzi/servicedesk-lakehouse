# Databricks notebook source
# MAGIC %md
# MAGIC # Service Desk Lakehouse — Job no Databricks
# MAGIC
# MAGIC Entry-point para rodar o mesmo pipeline como um **Databricks Job**, gravando
# MAGIC as camadas Delta em **AWS S3**. O código de transformação é exatamente o do
# MAGIC pacote `servicedesk_pipeline` (reaproveitado, não reescrito) — aqui só
# MAGIC configuramos o ambiente e disparamos as etapas.
# MAGIC
# MAGIC **Pré-requisitos no cluster:** `%pip install delta-spark PyYAML` (o Spark e os
# MAGIC JARs do Delta já vêm no Databricks Runtime) e o repositório adicionado via
# MAGIC *Repos*. Acesso ao S3 via *instance profile* (IAM role) do cluster.

# COMMAND ----------

import os

# No Databricks a SparkSession (`spark`) e o Delta já existem; apontamos o
# pipeline para o S3 e deixamos o backend usar a sessão corrente.
os.environ["STORAGE_BACKEND"] = "s3"
os.environ["TABLE_FORMAT"] = "delta"
os.environ["S3_BUCKET"] = dbutils.widgets.get("s3_bucket") if "dbutils" in dir() else "meu-bucket-lakehouse"  # noqa: F821
os.environ["S3_PREFIX"] = "servicedesk"
os.environ["PG_ENABLED"] = "false"  # no Databricks o consumo normalmente é via Unity Catalog / SQL Warehouse

# COMMAND ----------

from servicedesk_pipeline import (
    generate_data,
    ingest_bronze,
    quality_checks,
    transform_gold,
    transform_silver,
)
from servicedesk_pipeline.config import load_settings
from servicedesk_pipeline.io_utils import read_table

settings = load_settings()

# COMMAND ----------

# Em produção o 'generate' seria substituído pela ingestão real (Auto Loader,
# Kafka, etc.). Mantido aqui para o job rodar fim-a-fim em ambiente de demo.
tickets = generate_data.generate_tickets(settings.tickets_per_run)
generate_data.write_raw(tickets, settings.raw_path(), settings.raw_format)

# COMMAND ----------

ingest_bronze.run(spark, settings)          # noqa: F821  (spark é injetado pelo Databricks)
transform_silver.run(spark, settings)        # noqa: F821

silver = read_table(spark, settings.table_path("silver", "tickets"), settings)  # noqa: F821
quality_checks.assert_all(quality_checks.check_silver(silver))

transform_gold.run(spark, settings)          # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC As tabelas Delta ficam em `s3a://<bucket>/servicedesk/{bronze,silver,gold}`
# MAGIC e podem ser registradas no Unity Catalog para consumo via SQL Warehouse / BI.
