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
            "_ingested_at": "2026-01-11T00:00:00",
        },
        {
            "ticket_id": "CH2", "opened_at": "2026-01-11T08:00:00",
            "closed_at": "2026-01-11T09:00:00", "channel": "Chat",
            "category": "Rede", "priority": "Média", "technician_id": "T101",
            "technician_name": "Bruno", "requester_id": "U2", "site": "RJ",
            "status": "Resolvido", "first_response_minutes": 5,
            "resolution_minutes": 60, "satisfaction_score": 4, "reopened": False,
            "_ingested_at": "2026-01-12T00:00:00",
        },
    ]


def test_silver_limpa_passa_em_todas(spark, settings):
    silver = clean(spark.createDataFrame(_base_rows()), settings)
    results = check_silver(silver)
    assert all(r.passed for r in results)
    assert_all(results)  # não deve levantar


def test_detecta_satisfacao_fora_da_faixa(spark, settings):
    rows = _base_rows()
    rows[0]["satisfaction_score"] = 9  # inválido
    silver = clean(spark.createDataFrame(rows), settings)
    results = {r.name: r for r in check_silver(silver)}
    assert results["satisfacao_na_faixa"].passed is False


def test_assert_all_levanta_em_falha(spark, settings):
    rows = _base_rows()
    rows[0]["satisfaction_score"] = 42
    silver = clean(spark.createDataFrame(rows), settings)
    with pytest.raises(DataQualityError):
        assert_all(check_silver(silver))
