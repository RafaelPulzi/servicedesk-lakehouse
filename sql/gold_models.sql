-- ============================================================================
-- Modelos da camada GOLD (documentação do SQL analítico)
-- ----------------------------------------------------------------------------
-- Estas são as mesmas consultas executadas por transform_gold.py via Spark SQL.
-- A camada silver é exposta como a view `silver_tickets`.
-- Destaque: CTEs e window functions (RANK, AVG OVER, média móvel de 7 dias).
-- ============================================================================

-- fato_chamados — grão: 1 linha por chamado -------------------------------
SELECT
    ticket_id, opened_at, closed_at, opened_date, channel, category, priority,
    technician_id, technician_name, site, status, reopened,
    first_response_minutes, resolution_minutes, resolution_hours,
    sla_hours, within_sla, satisfaction_score
FROM silver_tickets;

-- dim_tecnico — estatísticas por técnico + ranking (window function) -------
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
    RANK() OVER (ORDER BY total_chamados DESC) AS rank_volume,
    RANK() OVER (ORDER BY pct_dentro_sla DESC) AS rank_sla
FROM por_tecnico;

-- mart_sla_categoria — aderência a SLA + participação no volume ------------
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
    category, total_chamados, media_horas_resolucao, pct_dentro_sla,
    ROUND(100.0 * total_chamados / SUM(total_chamados) OVER (), 1) AS pct_do_volume_total
FROM base
ORDER BY total_chamados DESC;

-- mart_volume_diario — volume diário + média móvel de 7 dias ---------------
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
    opened_date, total_chamados, pct_dentro_sla,
    ROUND(
        AVG(total_chamados) OVER (
            ORDER BY opened_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    ) AS media_movel_7d
FROM diario
ORDER BY opened_date;

-- mart_kpi_geral — indicadores de topo (linha única) ----------------------
SELECT
    COUNT(*)                                                 AS total_chamados,
    ROUND(AVG(resolution_hours), 2)                          AS media_horas_resolucao,
    ROUND(AVG(CASE WHEN within_sla THEN 1.0 ELSE 0.0 END) * 100, 1) AS pct_dentro_sla,
    ROUND(AVG(CAST(satisfaction_score AS DOUBLE)), 2)        AS media_satisfacao,
    ROUND(AVG(CASE WHEN reopened THEN 1.0 ELSE 0.0 END) * 100, 1)   AS taxa_reabertura_pct,
    COUNT(DISTINCT technician_id)                            AS tecnicos_ativos
FROM silver_tickets;
