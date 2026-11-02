"""Testes da limpeza silver, com DataFrames pequenos e controlados."""
from __future__ import annotations

from servicedesk_pipeline.transform_silver import clean


def _rows():
    # Casos de borda montados à mão para asserções determinísticas.
    return [
        # duplicata de CH1 (duas ingestões) -> deve sobrar a mais recente
        {
            "ticket_id": "CH1", "opened_at": "2026-01-10T08:00:00",
            "closed_at": "2026-01-10T12:00:00", "channel": "Portal",
            "category": "  hardware ", "priority": "Alta", "technician_id": "T100",
            "technician_name": "Ana", "requester_id": "U1", "site": "SP",
            "status": "Resolvido", "first_response_minutes": 10,
            "resolution_minutes": 240, "satisfaction_score": 5, "reopened": False,
            "_ingested_at": "2026-01-11T00:00:00",
        },
        {
            "ticket_id": "CH1", "opened_at": "2026-01-10T08:00:00",
            "closed_at": "2026-01-10T12:00:00", "channel": "Portal",
            "category": "Hardware", "priority": "Alta", "technician_id": "T100",
            "technician_name": "Ana", "requester_id": "U1", "site": "SP",
            "status": "Resolvido", "first_response_minutes": 10,
            "resolution_minutes": 240, "satisfaction_score": 4, "reopened": False,
            "_ingested_at": "2026-01-12T00:00:00",  # mais recente
        },
        # duração negativa -> deve ser descartada
        {
            "ticket_id": "CH2", "opened_at": "2026-01-10T09:00:00",
            "closed_at": "2026-01-10T08:00:00", "channel": "Chat",
            "category": "Rede", "priority": "Crítica", "technician_id": "T101",
            "technician_name": "Bruno", "requester_id": "U2", "site": "SP",
            "status": "Resolvido", "first_response_minutes": 5,
            "resolution_minutes": -60, "satisfaction_score": 3, "reopened": False,
            "_ingested_at": "2026-01-11T00:00:00",
        },
        # dentro do SLA (Crítica=4h, resolveu em 2h)
        {
            "ticket_id": "CH3", "opened_at": "2026-02-01T09:00:00",
            "closed_at": "2026-02-01T11:00:00", "channel": "Telefone",
            "category": "Vpn", "priority": "Crítica", "technician_id": "T102",
            "technician_name": "Carla", "requester_id": "U3", "site": "RJ",
            "status": "Resolvido", "first_response_minutes": 5,
            "resolution_minutes": 120, "satisfaction_score": 5, "reopened": False,
            "_ingested_at": "2026-02-02T00:00:00",
        },
        # fora do SLA (Baixa=72h, resolveu em 100h)
        {
            "ticket_id": "CH4", "opened_at": "2026-02-01T09:00:00",
            "closed_at": "2026-02-05T13:00:00", "channel": "E-mail",
            "category": "Outros", "priority": "Baixa", "technician_id": "T103",
            "technician_name": "Diego", "requester_id": "U4", "site": "BH",
            "status": "Reaberto", "first_response_minutes": 30,
            "resolution_minutes": 6000, "satisfaction_score": 2, "reopened": True,
            "_ingested_at": "2026-02-06T00:00:00",
        },
    ]


def test_remove_duplicatas_mantendo_mais_recente(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    ch1 = df.filter(df.ticket_id == "CH1").collect()
    assert len(ch1) == 1
    # a mais recente tinha satisfaction_score=4
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


def test_flag_within_sla(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    por_id = {r["ticket_id"]: r for r in df.collect()}
    assert por_id["CH3"]["within_sla"] is True   # 2h <= 4h
    assert por_id["CH4"]["within_sla"] is False  # 100h > 72h


def test_deriva_resolution_hours(spark, settings):
    df = clean(spark.createDataFrame(_rows()), settings)
    ch3 = df.filter(df.ticket_id == "CH3").collect()[0]
    assert ch3["resolution_hours"] == 2.0
