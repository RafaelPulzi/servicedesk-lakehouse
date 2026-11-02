"""Camada GOLD — modelo dimensional e marts de consumo (Spark SQL).

Esta é a camada que substitui o relatório gerencial montado à mão: em vez de
alguém cruzar planilhas por ~4h, as marts abaixo são recalculadas pelo pipeline
e ficam prontas para o dashboard/BI.

As transformações são escritas em **Spark SQL** de propósito, para evidenciar
SQL analítico: CTEs e window functions (RANK, AVG OVER, média móvel de 7 dias).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .config import Settings
from .io_utils import read_table, write_table

# ---------------------------------------------------------------------------
# Consultas SQL (espelhadas em sql/gold_models.sql para documentação).
# A silver é registrada como a view temporária `silver_tickets`.
# ---------------------------------------------------------------------------

SQL_FATO_CHAMADOS = """
SELECT
    ticket_id,
    opened_at,
    closed_at,
    opened_date,
    channel,
    category,
    priority,
    technician_id,
    technician_name,
    site,
    status,
    reopened,
    first_response_minutes,
    resolution_minutes,
    resolution_hours,
    sla_hours,
    within_sla,
    satisfaction_score
FROM silver_tickets
"""

# dim_tecnico: estatísticas por técnico + ranking de produtividade (window fn).
SQL_DIM_TECNICO = """
WITH por_tecnico AS (
    SELECT
        technician_id,
        technician_name,
        COUNT(*)                                             AS total_chamados,
        ROUND(AVG(resolution_hours), 2)                      AS media_horas_resolucao,
        ROUND(AVG(CASE WHEN within_sla THEN 1.0 ELSE 0.0 END) * 100, 1) AS pct_dentro_sla,
        ROUND(AVG(CAST(satisfaction_score AS DOUBLE)), 2)    AS media_satisfacao,
        ROUND(AVG(CASE WHEN reopened THEN 1.0 ELSE 0.0 END) * 100, 1)   AS taxa_reabertura_pct
    FROM silver_tickets
    GROUP BY technician_id, technician_name
)
SELECT
    *,
    RANK() OVER (ORDER BY total_chamados DESC)  AS rank_volume,
    RANK() OVER (ORDER BY pct_dentro_sla DESC)  AS rank_sla
FROM por_tecnico
"""

# mart_sla_categoria: aderência a SLA por categoria + participação no volume.
SQL_MART_SLA_CATEGORIA = """
WITH base AS (
    SELECT
        category,
        COUNT(*)                                             AS total_chamados,
        ROUND(AVG(resolution_hours), 2)                      AS media_horas_resolucao,
        ROUND(AVG(CASE WHEN within_sla THEN 1.0 ELSE 0.0 END) * 100, 1) AS pct_dentro_sla
    FROM silver_tickets
    GROUP BY category
)
SELECT
    category,
    total_chamados,
    media_horas_resolucao,
    pct_dentro_sla,
    ROUND(100.0 * total_chamados / SUM(total_chamados) OVER (), 1) AS pct_do_volume_total
FROM base
ORDER BY total_chamados DESC
"""

# mart_volume_diario: volume por dia + média móvel de 7 dias (window fn).
SQL_MART_VOLUME_DIARIO = """
WITH diario AS (
    SELECT
        opened_date,
        COUNT(*)                                             AS total_chamados,
        ROUND(AVG(CASE WHEN within_sla THEN 1.0 ELSE 0.0 END) * 100, 1) AS pct_dentro_sla
    FROM silver_tickets
    WHERE opened_date IS NOT NULL
    GROUP BY opened_date
)
SELECT
    opened_date,
    total_chamados,
    pct_dentro_sla,
    ROUND(
        AVG(total_chamados) OVER (
            ORDER BY opened_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    ) AS media_movel_7d
FROM diario
ORDER BY opened_date
"""

# mart_kpi_geral: linha única com os indicadores de topo (o "cabeçalho" do relatório).
SQL_MART_KPI_GERAL = """
SELECT
    COUNT(*)                                                 AS total_chamados,
    ROUND(AVG(resolution_hours), 2)                          AS media_horas_resolucao,
    ROUND(AVG(CASE WHEN within_sla THEN 1.0 ELSE 0.0 END) * 100, 1) AS pct_dentro_sla,
    ROUND(AVG(CAST(satisfaction_score AS DOUBLE)), 2)        AS media_satisfacao,
    ROUND(AVG(CASE WHEN reopened THEN 1.0 ELSE 0.0 END) * 100, 1)   AS taxa_reabertura_pct,
    COUNT(DISTINCT technician_id)                            AS tecnicos_ativos
FROM silver_tickets
"""

GOLD_MODELS = {
    "fato_chamados": SQL_FATO_CHAMADOS,
    "dim_tecnico": SQL_DIM_TECNICO,
    "mart_sla_categoria": SQL_MART_SLA_CATEGORIA,
    "mart_volume_diario": SQL_MART_VOLUME_DIARIO,
    "mart_kpi_geral": SQL_MART_KPI_GERAL,
}


def build_gold(silver: DataFrame) -> dict[str, DataFrame]:
    """Constrói todas as marts a partir da silver. Função pura (testável)."""
    spark = silver.sparkSession
    silver.createOrReplaceTempView("silver_tickets")
    return {name: spark.sql(sql) for name, sql in GOLD_MODELS.items()}


def run(spark: SparkSession, settings: Settings) -> dict[str, DataFrame]:
    silver = read_table(spark, settings.table_path("silver", "tickets"), settings)
    models = build_gold(silver)
    for name, df in models.items():
        target = settings.table_path("gold", name)
        write_table(df, target, settings, mode="overwrite")
        print(f"[gold] {name}: {df.count()} linhas -> {target}")
    return models
