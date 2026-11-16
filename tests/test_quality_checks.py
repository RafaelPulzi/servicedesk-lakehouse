"""Testes das checagens de qualidade de dados."""
from __future__ import annotations

import pytest

from servicedesk_pipeline.quality_checks import (
    DataQualityError,
    assert_all,
    check_silver,
)
from servicedesk_pipeline.transform_silver import clean


def _base_rows():
    return [
        {
            "ticket_id": "CH1", "opened_at": "2026-01-10T08:00:00",
            "closed_at": "2026-01-10T12:00:00", "channel": "Portal",
            "category": "Hardware", "priority": "Alta", "technician_id": "T100",
            "technician_name": "Ana", "requester_id": "U1", "site": "SP",
            "status": "Resolvido", "first_response_minutes": 10,
            "resolution_minutes": 240, "satisfaction_score": 5, "reopened": False,
            "export_date": "2026-01-31T23:59:59", "_ingested_at": "2026-02-01T00:00:00",
        },
        {
            "ticket_id": "CH2", "opened_at": "2026-01-11T08:00:00",
            "closed_at": None, "channel": "Chat",
            "category": "Rede", "priority": "Média", "technician_id": "T101",
            "technician_name": "Bruno", "requester_id": "U2", "site": "RJ",
            "status": "Aberto", "first_response_minutes": None,
            "resolution_minutes": None, "satisfaction_score": None, "reopened": False,
            "export_date": "2026-01-31T23:59:59", "_ingested_at": "2026-02-01T00:00:00",
        },
        {
            "ticket_id": "CH3", "opened_at": "2026-01-12T08:00:00",
            "closed_at": "2026-01-13T09:00:00", "channel": "Portal",
            "category": "Acesso", "priority": "Baixa", "technician_id": "T102",
            "technician_name": "Carla", "requester_id": "U3", "site": "SP",
            "status": "Cancelado", "first_response_minutes": None,
            "resolution_minutes": None, "satisfaction_score": None, "reopened": False,
            "export_date": "2026-01-31T23:59:59", "_ingested_at": "2026-02-01T00:00:00",
        },
    ]


def test_silver_limpa_passa_em_todas(spark, settings):
    silver = clean(spark.createDataFrame(_base_rows()), settings)
    results = check_silver(silver, settings)
    assert all(r.passed for r in results)
    assert_all(results)  # não deve levantar


def test_detecta_satisfacao_fora_da_faixa(spark, settings):
    rows = _base_rows()
    rows[0]["satisfaction_score"] = 9  # inválido
    silver = clean(spark.createDataFrame(rows), settings)
    results = {r.name: r for r in check_silver(silver, settings)}
    assert results["satisfacao_na_faixa"].passed is False


def test_detecta_prioridade_fora_do_dominio_do_config(spark, settings):
    rows = _base_rows()
    rows[0]["priority"] = "Urgentíssima"  # não existe em sla_hours
    silver = clean(spark.createDataFrame(rows), settings)
    results = {r.name: r for r in check_silver(silver, settings)}
    assert results["dominio_prioridade"].passed is False


def test_detecta_status_fora_do_dominio(spark, settings):
    rows = _base_rows()
    rows[0]["status"] = "Reaberto"  # reabertura é flag, não status
    silver = clean(spark.createDataFrame(rows), settings)
    results = {r.name: r for r in check_silver(silver, settings)}
    assert results["dominio_status"].passed is False


def test_detecta_aberto_com_encerramento(spark, settings):
    rows = _base_rows()
    rows[1]["closed_at"] = "2026-01-12T08:00:00"  # aberto não pode ter closed_at
    silver = clean(spark.createDataFrame(rows), settings)
    results = {r.name: r for r in check_silver(silver, settings)}
    assert results["aberto_sem_encerramento"].passed is False


def test_detecta_cancelado_com_resolucao(spark, settings):
    rows = _base_rows()
    rows[2]["resolution_minutes"] = 90  # cancelado não tem resolução
    silver = clean(spark.createDataFrame(rows), settings)
    results = {r.name: r for r in check_silver(silver, settings)}
    assert results["cancelado_sem_resolucao"].passed is False


def test_assert_all_levanta_em_falha(spark, settings):
    rows = _base_rows()
    rows[0]["satisfaction_score"] = 42
    silver = clean(spark.createDataFrame(rows), settings)
    with pytest.raises(DataQualityError):
        assert_all(check_silver(silver, settings))
