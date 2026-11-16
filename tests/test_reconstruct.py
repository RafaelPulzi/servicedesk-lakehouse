"""Testes da reconstrução "como publicado" (bronze + ciclo -> edição).

Exercitam só o comportamento externo da seam única (`reconstruct_edition`):
DataFrames bronze pequenos montados à mão, com exports sucessivos, e
asserções determinísticas por ticket_id — nunca os passos internos da
composição silver/gold.
"""
from __future__ import annotations

import pytest

from servicedesk_pipeline.reconstruct import CicloSemExportError, reconstruct_edition
from servicedesk_pipeline.transform_gold import build_gold
from servicedesk_pipeline.transform_silver import clean

EXPORT_JAN = "2026-01-31T23:59:59"
EXPORT_FEV = "2026-02-28T23:59:59"


def _row(**overrides) -> dict:
    base = {
        "ticket_id": "CH0", "opened_at": "2026-01-10T08:00:00",
        "closed_at": "2026-01-10T12:00:00", "channel": "Portal",
        "category": "Hardware", "priority": "Média", "technician_id": "T100",
        "technician_name": "Ana", "requester_id": "U1", "site": "SP",
        "status": "Resolvido", "first_response_minutes": 10,
        "resolution_minutes": 240, "satisfaction_score": 5, "reopened": False,
        "export_date": EXPORT_JAN, "_ingested_at": "2026-02-01T00:00:00",
    }
    base.update(overrides)
    return base


def _bronze(spark):
    """Bronze append-only com dois exports (ciclos 2026-01 e 2026-02)."""
    rows = [
        # ----- export de janeiro (ciclo 2026-01) -----
        # CH1: em andamento no ciclo de janeiro...
        _row(ticket_id="CH1", status="Em Andamento", closed_at=None,
             resolution_minutes=None, satisfaction_score=None,
             first_response_minutes=None),
        # CH2: aberto em 31/jan às 10h -> ~14h em aberto na referência de
        # janeiro (dentro do prazo da Média, 24h)
        _row(ticket_id="CH2", opened_at="2026-01-31T10:00:00", status="Aberto",
             closed_at=None, resolution_minutes=None, satisfaction_score=None,
             first_response_minutes=None),
        # CH3: resolvido em janeiro, sem reabertura até o export de janeiro
        _row(ticket_id="CH3", status="Resolvido", reopened=False),
        # ----- export de fevereiro (ciclo 2026-02) -----
        # CH1: resolvido no ciclo seguinte
        _row(ticket_id="CH1", status="Resolvido",
             closed_at="2026-02-10T12:00:00", resolution_minutes=120,
             satisfaction_score=4, first_response_minutes=15,
             export_date=EXPORT_FEV, _ingested_at="2026-03-01T00:00:00"),
        # CH2: continua aberto; na referência de fevereiro já estourou a Média
        _row(ticket_id="CH2", opened_at="2026-01-31T10:00:00", status="Aberto",
             closed_at=None, resolution_minutes=None, satisfaction_score=None,
             first_response_minutes=None,
             export_date=EXPORT_FEV, _ingested_at="2026-03-01T00:00:00"),
        # CH3: reaberto DEPOIS do ciclo de janeiro (evento futuro p/ a edição)
        _row(ticket_id="CH3", status="Resolvido", reopened=True,
             export_date=EXPORT_FEV, _ingested_at="2026-03-01T00:00:00"),
        # CH4: chamado novo do ciclo de fevereiro
        _row(ticket_id="CH4", opened_at="2026-02-05T09:00:00",
             closed_at="2026-02-05T11:00:00", resolution_minutes=120,
             export_date=EXPORT_FEV, _ingested_at="2026-03-01T00:00:00"),
    ]
    return spark.createDataFrame(rows)


def _fato_por_id(edicao):
    return {r["ticket_id"]: r for r in edicao.models["fato_chamados"].collect()}


def test_chamado_em_andamento_no_ciclo_aparece_no_backlog_da_edicao(spark, settings):
    bronze = _bronze(spark)
    jan = _fato_por_id(reconstruct_edition(bronze, "2026-01", settings))
    fev = _fato_por_id(reconstruct_edition(bronze, "2026-02", settings))
    # edição de janeiro: CH1 ainda em andamento (backlog), como foi publicado
    assert jan["CH1"]["status"] == "Em Andamento"
    assert jan["CH1"]["horas_em_aberto"] is not None
    # edição de fevereiro: o mesmo chamado aparece resolvido
    assert fev["CH1"]["status"] == "Resolvido"
    assert fev["CH1"]["horas_em_aberto"] is None
    # CH4 (aberto em fevereiro) não existia na edição de janeiro
    assert "CH4" not in jan


def test_backlog_estourado_medido_contra_a_referencia_do_ciclo(spark, settings):
    bronze = _bronze(spark)
    jan = _fato_por_id(reconstruct_edition(bronze, "2026-01", settings))
    fev = _fato_por_id(reconstruct_edition(bronze, "2026-02", settings))
    # a data de referência da edição é a do export do ciclo, não a mais recente
    assert jan["CH2"]["data_referencia"].strftime("%Y-%m-%d") == "2026-01-31"
    # na época (~14h em aberto) CH2 ainda estava dentro do prazo da Média (24h)
    assert jan["CH2"]["backlog_estourado"] is False
    # contra a referência de fevereiro (~28 dias) já tinha estourado
    assert fev["CH2"]["backlog_estourado"] is True


def test_reabertura_posterior_ao_ciclo_nao_aparece_na_edicao(spark, settings):
    bronze = _bronze(spark)
    jan = _fato_por_id(reconstruct_edition(bronze, "2026-01", settings))
    fev = _fato_por_id(reconstruct_edition(bronze, "2026-02", settings))
    # na edição de janeiro CH3 ainda não tinha sido reaberto
    assert jan["CH3"]["reopened"] is False
    # a reabertura só entra na edição do ciclo em que foi exportada
    assert fev["CH3"]["reopened"] is True


def test_edicao_do_ciclo_mais_recente_igual_a_visao_atual(spark, settings):
    bronze = _bronze(spark)
    edicao = reconstruct_edition(bronze, "2026-02", settings)
    atual = build_gold(clean(bronze, settings))
    assert set(edicao.models) == set(atual)
    chave = lambda r: tuple(str(v) for v in r)  # noqa: E731
    for nome, df_atual in atual.items():
        linhas_atual = sorted(chave(r) for r in df_atual.collect())
        linhas_edicao = sorted(chave(r) for r in edicao.models[nome].collect())
        assert linhas_edicao == linhas_atual, nome


def test_expoe_a_silver_reconstruida(spark, settings):
    edicao = reconstruct_edition(_bronze(spark), "2026-01", settings)
    por_id = {r["ticket_id"]: r for r in edicao.silver.collect()}
    # a silver da edição tem a última versão DENTRO do universo do ciclo
    assert set(por_id) == {"CH1", "CH2", "CH3"}
    assert por_id["CH1"]["status"] == "Em Andamento"


def test_ciclo_sem_export_levanta_erro_com_ciclos_disponiveis(spark, settings):
    with pytest.raises(CicloSemExportError) as exc:
        reconstruct_edition(_bronze(spark), "2026-07", settings)
    msg = str(exc.value)
    assert "2026-07" in msg
    assert "2026-01" in msg and "2026-02" in msg
