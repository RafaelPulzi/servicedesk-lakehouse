"""Testes do simulador de exports (Python puro, não precisa de Spark)."""
from __future__ import annotations

from servicedesk_pipeline.domain import (
    OPEN_STATUSES,
    STATUS_CANCELADO,
    STATUS_DOMAIN,
    STATUS_RESOLVIDO,
)
from servicedesk_pipeline.generate_data import CATEGORIES, PRIORITIES, generate_exports


def _by_id(export: dict) -> dict:
    """Indexa os tickets de um export por id (primeira ocorrência)."""
    out = {}
    for t in export["tickets"]:
        out.setdefault(t["ticket_id"], t)
    return out


def test_gera_um_export_por_ciclo():
    exports = generate_exports(300, cycles=4, seed=1)
    assert len(exports) == 4
    datas = [e["export_date"] for e in exports]
    assert datas == sorted(datas)  # ordem cronológica


def test_determinismo_com_seed():
    a = generate_exports(200, cycles=3, seed=7)
    b = generate_exports(200, cycles=3, seed=7)
    assert a == b  # mesma seed -> mesma saída


def test_export_e_completo_e_cumulativo():
    exports = generate_exports(500, cycles=3, seed=2)
    anteriores: set[str] = set()
    for export in exports:
        ids = set(t["ticket_id"] for t in export["tickets"])
        # todo chamado que já apareceu continua aparecendo (export completo)
        assert anteriores <= ids
        anteriores = ids
    # o último export contém todos os chamados gerados
    assert len(anteriores) == 500


def test_schema_dos_registros():
    campos = {
        "ticket_id", "opened_at", "closed_at", "channel", "category",
        "priority", "technician_id", "technician_name", "requester_id",
        "site", "status", "first_response_minutes", "resolution_minutes",
        "satisfaction_score", "reopened", "export_date",
    }
    for export in generate_exports(100, cycles=2, seed=3):
        for t in export["tickets"]:
            assert set(t.keys()) == campos


def test_ciclo_de_vida_coerente():
    for export in generate_exports(800, cycles=3, seed=5):
        for t in export["tickets"]:
            assert t["status"] in STATUS_DOMAIN
            if t["status"] in OPEN_STATUSES:
                assert t["closed_at"] is None
                assert t["resolution_minutes"] is None
                assert t["satisfaction_score"] is None
            elif t["status"] == STATUS_RESOLVIDO:
                assert t["closed_at"] is not None
                assert t["resolution_minutes"] is not None
            elif t["status"] == STATUS_CANCELADO:
                assert t["closed_at"] is not None
                assert t["resolution_minutes"] is None
                assert t["satisfaction_score"] is None


def test_versoes_divergentes_entre_exports():
    exports = generate_exports(800, cycles=4, seed=11)
    primeiro, ultimo = _by_id(exports[0]), _by_id(exports[-1])
    mudaram = [
        tid for tid in primeiro
        if primeiro[tid]["status"] != ultimo[tid]["status"]
    ]
    # chamados abertos no 1º export foram resolvidos/cancelados depois
    assert len(mudaram) > 0


def test_reabertura_e_flag_cumulativa():
    exports = generate_exports(2000, cycles=6, seed=13)
    ultimo = _by_id(exports[-1])
    # existem chamados que foram reabertos e terminaram resolvidos:
    # a flag sobrevive à resolução final (não é status)
    resolvidos_reabertos = [
        t for t in ultimo.values()
        if t["reopened"] and t["status"] == STATUS_RESOLVIDO
    ]
    assert len(resolvidos_reabertos) > 0
    # uma vez True em um export, nunca volta a False nos seguintes
    vistos_true: set[str] = set()
    for export in exports:
        atual = _by_id(export)
        for tid in vistos_true:
            assert atual[tid]["reopened"] is True
        vistos_true |= {tid for tid, t in atual.items() if t["reopened"]}


def test_injeta_imperfeicoes_esperadas():
    exports = generate_exports(3000, cycles=3, seed=17)
    tickets = [t for e in exports for t in e["tickets"]]
    # reenvios exatos dentro de um export
    algum_export = exports[-1]["tickets"]
    ids = [t["ticket_id"] for t in algum_export]
    assert len(ids) != len(set(ids))
    # algum satisfaction_score nulo em resolvidos
    assert any(
        t["satisfaction_score"] is None and t["status"] == STATUS_RESOLVIDO
        for t in tickets
    )
    # alguma duração negativa (dado inválido proposital)
    assert any(
        t["resolution_minutes"] is not None and t["resolution_minutes"] < 0
        for t in tickets
    )
    # alguma categoria "suja" (espaços/caixa) fora do conjunto canônico
    assert any(t["category"] not in CATEGORIES for t in tickets)


def test_prioridades_dentro_do_dominio():
    for export in generate_exports(300, cycles=2, seed=5):
        for t in export["tickets"]:
            assert t["priority"] in PRIORITIES
