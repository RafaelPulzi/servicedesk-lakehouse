"""Camada SILVER — dado limpo, tipado, deduplicado e com regras de negócio.

O que a silver resolve, em cima da bronze:
  * padroniza ``category`` (trim + Title Case) e tipos de data;
  * reduz cada ``ticket_id`` à sua **última versão** (o mesmo chamado aparece
    em exports sucessivos com estados diferentes — vale o export mais recente);
  * descarta registros com duração inválida (resolution < 0);
  * deriva colunas de análise (resolution_hours, competência, partições) e a
    flag de SLA (``within_sla``) — que só existe para chamados Resolvidos:
    cancelados e abertos ficam fora do universo da métrica (ver GLOSSARY.md).

O gate de qualidade roda ANTES da escrita: se uma checagem crítica falha, nem
a silver é persistida (ADR 0001).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from . import quality_checks
from .config import Settings
from .domain import STATUS_RESOLVIDO
from .io_utils import read_table, write_table


def _sla_expr(settings: Settings):
    """Monta a expressão CASE de horas de SLA por prioridade a partir do config."""
    col = F.col("priority")
    chain = None
    for prio, hours in settings.sla_hours.items():
        cond = col == F.lit(prio)
        chain = F.when(cond, F.lit(hours)) if chain is None else chain.when(cond, F.lit(hours))
    return chain.otherwise(F.lit(None)) if chain is not None else F.lit(None)


def clean(df: DataFrame, settings: Settings) -> DataFrame:
    """Aplica limpeza e deriva colunas. Função pura (testável sem IO)."""
    cleaned = (
        df
        # tipos
        .withColumn("opened_at", F.to_timestamp("opened_at"))
        .withColumn("closed_at", F.to_timestamp("closed_at"))
        .withColumn("export_date", F.to_timestamp("export_date"))
        .withColumn("resolution_minutes", F.col("resolution_minutes").cast("int"))
        .withColumn("first_response_minutes", F.col("first_response_minutes").cast("int"))
        .withColumn("satisfaction_score", F.col("satisfaction_score").cast("int"))
        # padroniza texto
        .withColumn("category", F.initcap(F.trim(F.col("category"))))
        .withColumn("priority", F.trim(F.col("priority")))
        .withColumn("status", F.trim(F.col("status")))
    )

    # Remove durações impossíveis (dado inválido injetado na origem). Como isso
    # acontece ANTES do dedup, uma versão anterior válida do mesmo chamado
    # sobrevive no lugar da versão corrompida.
    cleaned = cleaned.filter(
        F.col("resolution_minutes").isNull() | (F.col("resolution_minutes") >= 0)
    )

    # Última versão de cada chamado: o export mais recente vence; empates
    # (reenvio do mesmo export) se resolvem pela ingestão mais recente.
    order = [F.col("export_date").desc_nulls_last()]
    if "_ingested_at" in cleaned.columns:
        order.append(F.col("_ingested_at").desc())
    w = Window.partitionBy("ticket_id").orderBy(*order)
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
        # competência: o ciclo (mês de abertura) ao qual o chamado pertence
        .withColumn("competencia", F.date_format("opened_at", "yyyy-MM"))
        .withColumn("sla_hours", _sla_expr(settings))
        # within_sla só para Resolvidos; cancelados/abertos ficam nulos (fora
        # do universo da métrica — ver GLOSSARY.md "Dentro do SLA").
        .withColumn(
            "within_sla",
            F.when(
                (F.col("status") == STATUS_RESOLVIDO)
                & F.col("resolution_hours").isNotNull()
                & F.col("sla_hours").isNotNull(),
                F.col("resolution_hours") <= F.col("sla_hours"),
            ),
        )
    )
    return cleaned


def run(spark, settings: Settings) -> DataFrame:
    bronze = read_table(spark, settings.table_path("bronze", "tickets"), settings)
    silver = clean(bronze, settings)

    # Gate de qualidade ANTES de persistir (ADR 0001): se falhar, a silver
    # anterior permanece intacta e nada a jusante é publicado.
    quality_checks.assert_all(quality_checks.check_silver(silver, settings))

    target = settings.table_path("silver", "tickets")
    write_table(silver, target, settings, mode="overwrite", partition_by=["opened_year", "opened_month"])
    print(f"[silver] {silver.count()} linhas limpas -> {target}")
    return silver
