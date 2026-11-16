"""Testes das marts gold (marts por ciclo + window functions)."""
from __future__ import annotations

from servicedesk_pipeline.transform_gold import build_gold
from servicedesk_pipeline.transform_silver import clean


def _row(**overrides) -> dict:
    base = {
        "ticket_id": "CH0", "opened_at": "2026-03-01T09:00:00",
        "closed_at": "2026-03-01T11:00:00", "channel": "Portal",
        "category": "Rede", "priority": "Média", "technician_id": "T100",
        "technician_name": "T100", "requester_id": "U1", "site": "SP",
        "status": "Resolvido", "first_response_minutes": 5,
        "resolution_minutes": 120, "satisfaction_score": 5, "reopened": False,
        "export_date": "2026-04-30T23:59:59", "_ingested_at": "2026-05-01T00:00:00",
    }
    base.update(overrides)
    return base


def _silver(spark, settings):
    rows = [
        # ----- competência 2026-03 -----
        # dentro do SLA (Crítica: 2h <= 4h)
        _row(ticket_id="CH1", priority="Crítica", technician_id="T100",
             technician_name="T100", resolution_minutes=120),
        # fora do SLA (Crítica: 10h > 4h), reaberto
        _row(ticket_id="CH2", priority="Crítica", technician_id="T100",
             technician_name="T100", closed_at="2026-03-01T19:00:00",
             resolution_minutes=600, satisfaction_score=4, reopened=True),
        # dentro do SLA (Média: 2h <= 24h)
        _row(ticket_id="CH3", category="Software", technician_id="T101",
             technician_name="T101", satisfaction_score=3),
        # cancelado: conta no volume, fora das métricas de SLA/satisfação
        _row(ticket_id="CH4", status="Cancelado", technician_id="T101",
             technician_name="T101", resolution_minutes=None,
             satisfaction_score=None),
        # ----- competência 2026-04 -----
        # aberto há ~360h (desde 15/abr até a referência 30/abr): estourou a Média (24h)
        _row(ticket_id="CH5", opened_at="2026-04-15T12:00:00", status="Aberto",
             closed_at=None, first_response_minutes=None,
             resolution_minutes=None, satisfaction_score=None,
             technician_id="T101", technician_name="T101"),
        # aberto há ~12h (30/abr 11:00 -> 23:59): ainda dentro do prazo da Média
        _row(ticket_id="CH6", opened_at="2026-04-30T11:00:00", status="Aberto",
             closed_at=None, first_response_minutes=None,
             resolution_minutes=None, satisfaction_score=None,
             technician_id="T100", technician_name="T100"),
        # resolvido dentro do SLA em abril
        _row(ticket_id="CH7", opened_at="2026-04-02T09:00:00",
             closed_at="2026-04-02T10:00:00", resolution_minutes=60,
             technician_id="T100", technician_name="T100"),
    ]
    return clean(spark.createDataFrame(rows), settings)


def test_marts_existem(spark, settings):
    models = build_gold(_silver(spark, settings))
    assert set(models) == {
        "fato_chamados", "mart_kpi_ciclo", "mart_produtividade_tecnico",
        "mart_sla_categoria", "mart_volume_diario",
    }


def test_fato_tem_grao_de_ticket_e_colunas_de_backlog(spark, settings):
    silver = _silver(spark, settings)
    fato = build_gold(silver)["fato_chamados"]
    assert fato.count() == silver.count()
    assert {"competencia", "data_referencia", "horas_em_aberto", "backlog_estourado"} <= set(fato.columns)


def test_backlog_estourado_mede_contra_data_de_referencia(spark, settings):
    fato = build_gold(_silver(spark, settings))["fato_chamados"]
    por_id = {r["ticket_id"]: r for r in fato.collect()}
    assert por_id["CH5"]["backlog_estourado"] is True    # ~360h > 24h
    assert por_id["CH6"]["backlog_estourado"] is False   # ~13h <= 24h
    assert por_id["CH6"]["horas_em_aberto"] is not None
    # encerrados não são backlog
    assert por_id["CH1"]["backlog_estourado"] is False
    assert por_id["CH1"]["horas_em_aberto"] is None


def test_kpi_ciclo_uma_linha_por_competencia(spark, settings):
    kpi = build_gold(_silver(spark, settings))["mart_kpi_ciclo"]
    por_ciclo = {r["competencia"]: r for r in kpi.collect()}
    assert set(por_ciclo) == {"2026-03", "2026-04"}
    marco = por_ciclo["2026-03"]
    assert marco["total_chamados"] == 4
    assert marco["resolvidos"] == 3
    assert marco["cancelados"] == 1
    assert marco["backlog"] == 0
    abril = por_ciclo["2026-04"]
    assert abril["total_chamados"] == 3
    assert abril["backlog"] == 2
    assert abril["backlog_estourado"] == 1


def test_cancelados_fora_do_universo_do_sla(spark, settings):
    kpi = build_gold(_silver(spark, settings))["mart_kpi_ciclo"]
    marco = {r["competencia"]: r for r in kpi.collect()}["2026-03"]
    # 3 resolvidos: CH1 dentro, CH2 fora, CH3 dentro -> 66.7%. Se o cancelado
    # CH4 entrasse no denominador como "fora", daria 50%.
    assert abs(float(marco["pct_dentro_sla"]) - 66.7) < 0.1


def test_produtividade_rankeia_dentro_do_ciclo(spark, settings):
    mart = build_gold(_silver(spark, settings))["mart_produtividade_tecnico"]
    por_chave = {(r["competencia"], r["technician_id"]): r for r in mart.collect()}
    # março: T100 resolveu 2, T101 resolveu 1
    assert por_chave[("2026-03", "T100")]["resolvidos"] == 2
    assert por_chave[("2026-03", "T100")]["rank_volume"] == 1
    assert por_chave[("2026-03", "T101")]["rank_volume"] == 2
    # abril: T100 resolveu 1, T101 nenhum — ranking é por ciclo, não global
    assert por_chave[("2026-04", "T100")]["rank_volume"] == 1


def test_sla_categoria_participacao_soma_100_por_ciclo(spark, settings):
    mart = build_gold(_silver(spark, settings))["mart_sla_categoria"]
    somas: dict[str, float] = {}
    for r in mart.collect():
        somas[r["competencia"]] = somas.get(r["competencia"], 0.0) + float(r["pct_do_volume_no_ciclo"])
    for ciclo, total in somas.items():
        assert abs(total - 100.0) < 0.5, ciclo  # tolerância de arredondamento


def test_volume_diario_tem_media_movel(spark, settings):
    mart = build_gold(_silver(spark, settings))["mart_volume_diario"]
    assert "media_movel_7d" in mart.columns
    # dias distintos: 01/mar, 02/abr, 15/abr, 30/abr
    assert mart.count() == 4
