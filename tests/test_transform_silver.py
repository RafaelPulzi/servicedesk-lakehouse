"""Testes da limpeza silver, com DataFrames pequenos e controlados."""
from __future__ import annotations

from servicedesk_pipeline.transform_silver import clean


def _row(**overrides) -> dict:
    base = {
        "ticket_id": "CH0", "opened_at": "2026-01-10T08:00:00",
        "closed_at": "2026-01-10T12:00:00", "channel": "Portal",
        "category": "Hardware", "priority": "Alta", "technician_id": "T100",
        "technician_name": "Ana", "requester_id": "U1", "site": "SP",
        "status": "Resolvido", "first_response_minutes": 10,
        "resolution_minutes": 240, "satisfaction_score": 5, "reopened": False,
        "export_date": "2026-01-31T23:59:59", "_ingested_at": "2026-02-01T00:00:00",
    }
    base.update(overrides)
    return base


def _rows():
    # Casos de borda montados à mão para asserções determinísticas.
    return [
        # CH1 em dois exports: Em Andamento em jan, Resolvido em fev
        # -> deve sobrar a versão do export mais recente (Resolvido)
        _row(
            ticket_id="CH1", status="Em Andamento", closed_at=None,
            resolution_minutes=None, satisfaction_score=None,
            category="  hardware ",
            export_date="2026-01-31T23:59:59", _ingested_at="2026-02-01T00:00:00",
        ),
        _row(
            ticket_id="CH1", status="Resolvido",
            closed_at="2026-02-02T12:00:00", resolution_minutes=240,
            satisfaction_score=4,
            export_date="2026-02-28T23:59:59", _ingested_at="2026-03-01T00:00:00",
        ),
        # CH2: duração negativa -> versão descartada
        _row(
            ticket_id="CH2", opened_at="2026-01-10T09:00:00",
            closed_at="2026-01-10T08:00:00", resolution_minutes=-60,
            priority="Crítica", satisfaction_score=3,
        ),
        # CH3: dentro do SLA (Crítica=4h, resolveu em 2h)
        _row(
            ticket_id="CH3", opened_at="2026-02-01T09:00:00",
            closed_at="2026-02-01T11:00:00", priority="Crítica",
            category="Vpn", resolution_minutes=120,
            export_date="2026-02-28T23:59:59",
        ),
        # CH4: fora do SLA (Baixa=72h, resolveu em 100h), reaberto
        _row(
            ticket_id="CH4", opened_at="2026-02-01T09:00:00",
            closed_at="2026-02-05T13:00:00", priority="Baixa",
            category="Outros", resolution_minutes=6000,
            satisfaction_score=2, reopened=True,
            export_date="2026-02-28T23:59:59",
        ),
        # CH5: cancelado -> fora do universo do SLA
        _row(
            ticket_id="CH5", status="Cancelado",
            closed_at="2026-01-12T10:00:00", resolution_minutes=None,
            satisfaction_score=None,
        ),
        # CH6: ainda aberto -> sem resultado de SLA
        _row(
            ticket_id="CH6", status="Aberto", closed_at=None,
            first_response_minutes=None,
            resolution_minutes=None, satisfaction_score=None,
            export_date="2026-02-28T23:59:59",
        ),
    ]


def test_dedup_mantem_versao_do_export_mais_recente(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    ch1 = df.filter(df.ticket_id == "CH1").collect()
    assert len(ch1) == 1
    # a versão de fevereiro (Resolvido) vence a de janeiro (Em Andamento)
    assert ch1[0]["status"] == "Resolvido"
    assert ch1[0]["satisfaction_score"] == 4


def test_descarta_duracao_negativa(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    assert df.filter(df.ticket_id == "CH2").count() == 0


def test_padroniza_categoria(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    cats = {r["category"] for r in df.select("category").collect()}
    # "  hardware " -> "Hardware"; nenhuma categoria com espaço nas bordas
    assert "Hardware" in cats
    assert all(c == c.strip() for c in cats)


def test_flag_within_sla_para_resolvidos(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    por_id = {r["ticket_id"]: r for r in df.collect()}
    assert por_id["CH3"]["within_sla"] is True   # 2h <= 4h
    assert por_id["CH4"]["within_sla"] is False  # 100h > 72h


def test_within_sla_nulo_para_cancelado_e_aberto(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    por_id = {r["ticket_id"]: r for r in df.collect()}
    # cancelados e abertos ficam fora do universo da métrica (GLOSSARY.md)
    assert por_id["CH5"]["within_sla"] is None
    assert por_id["CH6"]["within_sla"] is None


def test_deriva_competencia_pelo_mes_de_abertura(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    por_id = {r["ticket_id"]: r for r in df.collect()}
    assert por_id["CH3"]["competencia"] == "2026-02"
    # CH1 abriu em janeiro e resolveu em fevereiro: competência é janeiro
    assert por_id["CH1"]["competencia"] == "2026-01"


def test_deriva_resolution_hours(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    ch3 = df.filter(df.ticket_id == "CH3").collect()[0]
    assert ch3["resolution_hours"] == 2.0
