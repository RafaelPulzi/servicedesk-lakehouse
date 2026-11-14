"""Camada GOLD — modelo dimensional e marts de consumo (Spark SQL).

Esta é a camada que substitui o relatório gerencial montado à mão: em vez de
alguém cruzar planilhas por ~4h, as marts abaixo são recalculadas pelo pipeline
e ficam prontas para o dashboard/BI.

Semântica (ver GLOSSARY.md):
  * **competência** = mês de abertura do chamado; todas as marts agregam por
    ciclo, como o relatório mensal real;
  * métricas de SLA/resolução/satisfação contam só chamados **Resolvidos**
    (``within_sla`` já vem nulo para cancelados/abertos desde a silver);
  * **backlog** = chamados ainda abertos; **backlog estourado** = abertos cuja
    idade já passou do SLA, medida contra a data de referência do export mais
    recente (nunca contra o relógio de quem roda o pipeline).

As transformações são escritas em **Spark SQL** de propósito, para evidenciar
SQL analítico: CTEs e window functions (RANK, AVG OVER, média móvel de 7 dias).
As marts consomem a view ``fato_chamados`` (construída primeiro), que já traz
as colunas derivadas de backlog — uma fonte única para as regras.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .config import Settings
from .io_utils import read_table, write_table

# ---------------------------------------------------------------------------
# Consultas SQL (espelhadas em sql/gold_models.sql para documentação).
# A silver é registrada como `silver_tickets`; o fato, como `fato_chamados`.
# ---------------------------------------------------------------------------

# fato_chamados — grão: 1 linha por chamado (última versão), enriquecida com
# a situação de backlog em relação à data de referência do export mais recente.
SQL_FATO_CHAMADOS = """
WITH ref AS (
    SELECT MAX(export_date) AS data_referencia FROM silver_tickets
)
SELECT
    t.ticket_id,
    t.opened_at,
    t.closed_at,
    t.opened_date,
    t.competencia,
    t.channel,
    t.category,
    t.priority,
    t.technician_id,
    t.technician_name,
    t.site,
    t.status,
    t.reopened,
    t.first_response_minutes,
    t.resolution_minutes,
    t.resolution_hours,
    t.sla_hours,
    t.within_sla,
    t.satisfaction_score,
    r.data_referencia,
    CASE WHEN t.status IN ('Aberto', 'Em Andamento')
         THEN ROUND((unix_timestamp(r.data_referencia) - unix_timestamp(t.opened_at)) / 3600.0, 2)
    END AS horas_em_aberto,
    CASE WHEN t.status IN ('Aberto', 'Em Andamento')
         THEN (unix_timestamp(r.data_referencia) - unix_timestamp(t.opened_at)) / 3600.0 > t.sla_hours
         ELSE false
    END AS backlog_estourado
FROM silver_tickets t
CROSS JOIN ref r
"""

# mart_kpi_ciclo — o "cabeçalho" do relatório, uma linha por competência.
# AVG(CASE WHEN ... WHEN NOT ... END) preserva nulos: cancelados e abertos não
# entram no denominador do SLA (um ELSE 0 os contaria como "fora do SLA").
SQL_MART_KPI_CICLO = """
SELECT
    competencia,
    COUNT(*)                                                      AS total_chamados,
    SUM(CASE WHEN status = 'Resolvido' THEN 1 ELSE 0 END)         AS resolvidos,
    SUM(CASE WHEN status = 'Cancelado' THEN 1 ELSE 0 END)         AS cancelados,
    SUM(CASE WHEN status IN ('Aberto', 'Em Andamento') THEN 1 ELSE 0 END) AS backlog,
    SUM(CASE WHEN backlog_estourado THEN 1 ELSE 0 END)            AS backlog_estourado,
    ROUND(AVG(CASE WHEN within_sla THEN 1.0 WHEN NOT within_sla THEN 0.0 END) * 100, 1) AS pct_dentro_sla,
    ROUND(AVG(CASE WHEN status = 'Resolvido' THEN resolution_hours END), 2)             AS media_horas_resolucao,
    ROUND(AVG(CASE WHEN status = 'Resolvido' THEN CAST(satisfaction_score AS DOUBLE) END), 2) AS media_satisfacao,
    ROUND(AVG(CASE WHEN status = 'Resolvido' THEN CASE WHEN reopened THEN 1.0 ELSE 0.0 END END) * 100, 1) AS taxa_reabertura_pct,
    COUNT(DISTINCT technician_id)                                 AS tecnicos_ativos
FROM fato_chamados
GROUP BY competencia
ORDER BY competencia
"""

# mart_produtividade_tecnico — métricas por técnico DENTRO de cada ciclo,
# com ranking por ciclo (window function particionada pela competência).
SQL_MART_PRODUTIVIDADE_TECNICO = """
WITH por_tecnico AS (
    SELECT
        competencia,
        technician_id,
        technician_name,
        COUNT(*)                                              AS total_chamados,
        SUM(CASE WHEN status = 'Resolvido' THEN 1 ELSE 0 END) AS resolvidos,
        ROUND(AVG(CASE WHEN status = 'Resolvido' THEN resolution_hours END), 2) AS media_horas_resolucao,
        ROUND(AVG(CASE WHEN within_sla THEN 1.0 WHEN NOT within_sla THEN 0.0 END) * 100, 1) AS pct_dentro_sla,
        ROUND(AVG(CASE WHEN status = 'Resolvido' THEN CAST(satisfaction_score AS DOUBLE) END), 2) AS media_satisfacao,
        ROUND(AVG(CASE WHEN status = 'Resolvido' THEN CASE WHEN reopened THEN 1.0 ELSE 0.0 END END) * 100, 1) AS taxa_reabertura_pct
    FROM fato_chamados
    GROUP BY competencia, technician_id, technician_name
)
SELECT
    *,
    RANK() OVER (PARTITION BY competencia ORDER BY resolvidos DESC)               AS rank_volume,
    RANK() OVER (PARTITION BY competencia ORDER BY pct_dentro_sla DESC NULLS LAST) AS rank_sla
FROM por_tecnico
"""

# mart_sla_categoria — aderência a SLA por categoria dentro de cada ciclo.
SQL_MART_SLA_CATEGORIA = """
WITH base AS (
    SELECT
        competencia,
        category,
        COUNT(*)                                              AS total_chamados,
        ROUND(AVG(CASE WHEN status = 'Resolvido' THEN resolution_hours END), 2) AS media_horas_resolucao,
        ROUND(AVG(CASE WHEN within_sla THEN 1.0 WHEN NOT within_sla THEN 0.0 END) * 100, 1) AS pct_dentro_sla
    FROM fato_chamados
    GROUP BY competencia, category
)
SELECT
    competencia,
    category,
    total_chamados,
    media_horas_resolucao,
    pct_dentro_sla,
    ROUND(100.0 * total_chamados / SUM(total_chamados) OVER (PARTITION BY competencia), 1) AS pct_do_volume_no_ciclo
FROM base
ORDER BY competencia, total_chamados DESC
"""

# mart_volume_diario — volume por dia + média móvel de 7 dias (window fn).
SQL_MART_VOLUME_DIARIO = """
WITH diario AS (
    SELECT
        opened_date,
        COUNT(*)                                              AS total_chamados,
        ROUND(AVG(CASE WHEN within_sla THEN 1.0 WHEN NOT within_sla THEN 0.0 END) * 100, 1) AS pct_dentro_sla
    FROM fato_chamados
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

MART_MODELS = {
    "mart_kpi_ciclo": SQL_MART_KPI_CICLO,
    "mart_produtividade_tecnico": SQL_MART_PRODUTIVIDADE_TECNICO,
    "mart_sla_categoria": SQL_MART_SLA_CATEGORIA,
    "mart_volume_diario": SQL_MART_VOLUME_DIARIO,
}

GOLD_MODELS = {"fato_chamados": SQL_FATO_CHAMADOS, **MART_MODELS}


def build_gold(silver: DataFrame) -> dict[str, DataFrame]:
    """Constrói o fato e todas as marts a partir da silver. Função pura (testável)."""
    spark = silver.sparkSession
    silver.createOrReplaceTempView("silver_tickets")
    fato = spark.sql(SQL_FATO_CHAMADOS)
    fato.createOrReplaceTempView("fato_chamados")
    models: dict[str, DataFrame] = {"fato_chamados": fato}
    for name, sql in MART_MODELS.items():
        models[name] = spark.sql(sql)
    return models


def run(spark: SparkSession, settings: Settings) -> dict[str, DataFrame]:
    silver = read_table(spark, settings.table_path("silver", "tickets"), settings)
    models = build_gold(silver)
    for name, df in models.items():
        target = settings.table_path("gold", name)
        write_table(df, target, settings, mode="overwrite")
        print(f"[gold] {name}: {df.count()} linhas -> {target}")
    return models
