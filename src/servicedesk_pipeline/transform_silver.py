"""Camada SILVER — dado limpo, tipado, deduplicado e com regras de negócio.

O que a silver resolve, em cima da bronze:
  * padroniza ``category`` (trim + Title Case) e tipos de data;
  * remove duplicatas por ``ticket_id`` (mantém a ingestão mais recente);
  * descarta registros com duração inválida (resolution < 0);
  * deriva colunas úteis para análise (resolution_hours, partições de data) e
    a flag de SLA (``within_sla``) a partir da prioridade.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .config import Settings
from .io_utils import read_table, write_table


def _sla_expr(settings: Settings):
    """Monta a expressão CASE de horas de SLA por prioridade a partir do config."""
    expr = F
    col = F.col("priority")
    mapping = settings.sla_hours
    sla = F.lit(None)
    # Constrói when/otherwise encadeado de forma determinística.
    chain = None
    for prio, hours in mapping.items():
        cond = col == F.lit(prio)
        chain = F.when(cond, F.lit(hours)) if chain is None else chain.when(cond, F.lit(hours))
    return chain.otherwise(F.lit(None)) if chain is not None else sla


def clean(df: DataFrame, settings: Settings) -> DataFrame:
    """Aplica limpeza e deriva colunas. Função pura (testável sem IO)."""
    cleaned = (
        df
        # tipos
        .withColumn("opened_at", F.to_timestamp("opened_at"))
        .withColumn("closed_at", F.to_timestamp("closed_at"))
        .withColumn("resolution_minutes", F.col("resolution_minutes").cast("int"))
        .withColumn("first_response_minutes", F.col("first_response_minutes").cast("int"))
        .withColumn("satisfaction_score", F.col("satisfaction_score").cast("int"))
        # padroniza texto
        .withColumn("category", F.initcap(F.trim(F.col("category"))))
        .withColumn("priority", F.trim(F.col("priority")))
        .withColumn("status", F.trim(F.col("status")))
    )

    # Remove durações impossíveis (dado inválido injetado na origem).
    cleaned = cleaned.filter(
        F.col("resolution_minutes").isNull() | (F.col("resolution_minutes") >= 0)
    )

    # Deduplica por ticket_id, mantendo a ingestão mais recente.
    if "_ingested_at" in cleaned.columns:
        order = F.col("_ingested_at").desc()
    else:
        order = F.col("opened_at").desc()
    w = Window.partitionBy("ticket_id").orderBy(order)
    cleaned = (
        cleaned.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Colunas derivadas.
    cleaned = (
        cleaned.withColumn(
            "resolution_hours", F.round(F.col("resolution_minutes") / 60.0, 2)
        )
        .withColumn("opened_date", F.to_date("opened_at"))
        .withColumn("opened_year", F.year("opened_at"))
        .withColumn("opened_month", F.month("opened_at"))
        .withColumn("sla_hours", _sla_expr(settings))
        .withColumn(
            "within_sla",
            F.when(F.col("resolution_hours").isNull(), None).otherwise(
                F.col("resolution_hours") <= F.col("sla_hours")
            ),
        )
    )
    return cleaned


def run(spark, settings: Settings) -> DataFrame:
    bronze = read_table(spark, settings.table_path("bronze", "tickets"), settings)
    silver = clean(bronze, settings)
    target = settings.table_path("silver", "tickets")
    write_table(silver, target, settings, mode="overwrite", partition_by=["opened_year", "opened_month"])
    print(f"[silver] {silver.count()} linhas limpas -> {target}")
    return silver
