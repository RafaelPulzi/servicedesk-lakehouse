"""Testes das marts gold (modelo dimensional + window functions)."""
from __future__ import annotations

from servicedesk_pipeline.transform_gold import build_gold
from servicedesk_pipeline.transform_silver import clean


def _silver(spark, settings):
    rows = [
        {
            "ticket_id": f"CH{i}", "opened_at": f"2026-03-0{d}T09:00:00",
            "closed_at": f"2026-03-0{d}T10:00:00", "channel": "Portal",
            "category": cat, "priority": prio, "technician_id": tech,
            "technician_name": tech, "requester_id": "U1", "site": "SP",
            "status": "Resolvido", "first_response_minutes": 5,
            "resolution_minutes": mins, "satisfaction_score": sat,
            "reopened": reopened, "_ingested_at": "2026-03-10T00:00:00",
        }
        for i, (d, cat, prio, tech, mins, sat, reopened) in enumerate(
            [
                (1, "Rede", "Crítica", "T100", 120, 5, False),   # fora SLA? 2h<=4h -> dentro
                (1, "Rede", "Crítica", "T100", 600, 4, True),    # 10h>4h -> fora, reaberto
                (2, "Software", "Média", "T101", 120, 3, False), # 2h<=24h -> dentro
                (2, "Software", "Média", "T100", 100, 5, False), # dentro
                (3, "Rede", "Baixa", "T101", 100, 2, False),     # dentro (72h)
            ]
        )
    ]
    return clean(spark.createDataFrame(rows), settings)


def test_marts_existem(spark, settings):
    models = build_gold(_silver(spark, settings))
    assert set(models) == {
        "fato_chamados", "dim_tecnico", "mart_sla_categoria",
        "mart_volume_diario", "mart_kpi_geral",
    }


def test_fato_tem_grao_de_ticket(spark, settings):
    silver = _silver(spark, settings)
    fato = build_gold(silver)["fato_chamados"]
    assert fato.count() == silver.count()


def test_dim_tecnico_agrega_e_rankeia(spark, settings):
    dim = build_gold(_silver(spark, settings))["dim_tecnico"]
    por_tech = {r["technician_id"]: r for r in dim.collect()}
    # T100 tem 3 chamados, T101 tem 2 -> T100 é rank 1 de volume
    assert por_tech["T100"]["total_chamados"] == 3
    assert por_tech["T101"]["total_chamados"] == 2
    assert por_tech["T100"]["rank_volume"] == 1


def test_sla_categoria_participacao_soma_100(spark, settings):
    mart = build_gold(_silver(spark, settings))["mart_sla_categoria"]
    total_pct = sum(float(r["pct_do_volume_total"]) for r in mart.collect())
    assert abs(total_pct - 100.0) < 0.5  # tolerância de arredondamento


def test_kpi_geral_linha_unica(spark, settings):
    kpi = build_gold(_silver(spark, settings))["mart_kpi_geral"].collect()
    assert len(kpi) == 1
    assert kpi[0]["total_chamados"] == 5
    assert kpi[0]["tecnicos_ativos"] == 2


def test_volume_diario_tem_media_movel(spark, settings):
    mart = build_gold(_silver(spark, settings))["mart_volume_diario"]
    assert "media_movel_7d" in mart.columns
    # 3 dias distintos (01, 02, 03 de março)
    assert mart.count() == 3
