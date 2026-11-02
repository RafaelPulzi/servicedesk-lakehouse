"""Testes do simulador de dados (Python puro, não precisa de Spark)."""
from __future__ import annotations

from servicedesk_pipeline.generate_data import (
    CATEGORIES,
    PRIORITIES,
    generate_tickets,
)


def test_gera_quantidade_minima():
    tickets = generate_tickets(500, seed=1)
    # Pode haver duplicatas injetadas, então o total é >= n.
    assert len(tickets) >= 500


def test_determinismo_com_seed():
    a = generate_tickets(200, seed=7)
    b = generate_tickets(200, seed=7)
    assert a == b  # mesma seed -> mesma saída


def test_schema_dos_registros():
    campos = {
        "ticket_id", "opened_at", "closed_at", "channel", "category",
        "priority", "technician_id", "technician_name", "requester_id",
        "site", "status", "first_response_minutes", "resolution_minutes",
        "satisfaction_score", "reopened",
    }
    for t in generate_tickets(100, seed=3):
        assert set(t.keys()) == campos


def test_injeta_imperfeicoes_esperadas():
    tickets = generate_tickets(5000, seed=11)
    # duplicatas de ticket_id existem
    ids = [t["ticket_id"] for t in tickets]
    assert len(ids) != len(set(ids))
    # algum satisfaction_score nulo
    assert any(t["satisfaction_score"] is None for t in tickets)
    # alguma duração negativa (dado inválido proposital)
    assert any(t["resolution_minutes"] < 0 for t in tickets)
    # alguma categoria "suja" (espaços/caixa) fora do conjunto canônico
    assert any(t["category"] not in CATEGORIES for t in tickets)


def test_prioridades_dentro_do_dominio():
    for t in generate_tickets(300, seed=5):
        assert t["priority"] in PRIORITIES
