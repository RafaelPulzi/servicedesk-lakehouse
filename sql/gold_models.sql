-- ============================================================================
-- Modelos da camada GOLD (documentação do SQL analítico)
-- ----------------------------------------------------------------------------
-- Estas são as mesmas consultas executadas por transform_gold.py via Spark SQL.
-- A camada silver é exposta como `silver_tickets`; o fato, como `fato_chamados`.
-- Destaque: CTEs e window functions (RANK particionado, AVG OVER, média móvel).
--
-- Semântica (GLOSSARY.md):
--   * competência = mês de ABERTURA do chamado (o ciclo do relatório);
--   * métricas de SLA/resolução/satisfação contam só Resolvidos — o padrão
--     AVG(CASE WHEN x THEN 1.0 WHEN NOT x THEN 0.0 END) preserva nulos, para
--     que cancelados/abertos não entrem no denominador;
--   * backlog estourado é medido contra a data de referência do export mais
--     recente, nunca contra o relógio de quem roda a consulta.
-- ============================================================================

-- fato_chamados — grão: 1 linha por chamado (última versão) -----------------
WITH ref AS (
    SELECT MAX(export_date) AS data_referencia FROM silver_tickets
)
SELECT
    t.ticket_id, t.opened_at, t.closed_at, t.opened_date, t.competencia,
    t.channel, t.category, t.priority, t.technician_id, t.technician_name,
    t.site, t.status, t.reopened, t.first_response_minutes,
    t.resolution_minutes, t.resolution_hours, t.sla_hours, t.within_sla,
    t.satisfaction_score, r.data_referencia,
    CASE WHEN t.status IN ('Aberto', 'Em Andamento')
         THEN ROUND((unix_timestamp(r.data_referencia) - unix_timestamp(t.opened_at)) / 3600.0, 2)
    END AS horas_em_aberto,
    CASE WHEN t.status IN ('Aberto', 'Em Andamento')
         THEN (unix_timestamp(r.data_referencia) - unix_timestamp(t.opened_at)) / 3600.0 > t.sla_hours
         ELSE false
    END AS backlog_estourado
FROM silver_tickets t
CROSS JOIN ref r;

-- mart_kpi_ciclo — o "cabeçalho" do relatório, 1 linha por competência ------
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
ORDER BY competencia;

-- mart_produtividade_tecnico — ranking POR CICLO (window particionada) ------
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
    RANK() OVER (PARTITION BY competencia ORDER BY resolvidos DESC)                AS rank_volume,
    RANK() OVER (PARTITION BY competencia ORDER BY pct_dentro_sla DESC NULLS LAST) AS rank_sla
FROM por_tecnico;

-- mart_sla_categoria — aderência a SLA por categoria dentro do ciclo --------
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
    competencia, category, total_chamados, media_horas_resolucao, pct_dentro_sla,
    ROUND(100.0 * total_chamados / SUM(total_chamados) OVER (PARTITION BY competencia), 1) AS pct_do_volume_no_ciclo
FROM base
ORDER BY competencia, total_chamados DESC;

-- mart_volume_diario — volume diário + média móvel de 7 dias ----------------
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
    opened_date, total_chamados, pct_dentro_sla,
    ROUND(
        AVG(total_chamados) OVER (
            ORDER BY opened_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    ) AS media_movel_7d
FROM diario
ORDER BY opened_date;
