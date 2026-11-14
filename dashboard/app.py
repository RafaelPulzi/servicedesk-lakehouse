"""Dashboard de chamados de Service Desk (Streamlit).

Lê as tabelas da camada gold. Por padrão lê do PostgreSQL (camada de consumo);
se o banco não estiver disponível, cai para a leitura direta dos Parquet/Delta
da gold no sistema de arquivos — assim o dashboard sobe mesmo sem Postgres.

O painel é por **ciclo** (mês de competência = mês de abertura do chamado),
como o relatório gerencial que ele substitui. Ver GLOSSARY.md.

Execução:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Permite importar o pacote quando rodado via `streamlit run`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from servicedesk_pipeline.config import load_settings  # noqa: E402

st.set_page_config(page_title="Service Desk — Painel", layout="wide", page_icon="🛠️")
SETTINGS = load_settings()


@st.cache_data(ttl=300)
def load_table(name: str) -> pd.DataFrame:
    """Carrega uma tabela gold do Postgres; se falhar, lê os arquivos direto."""
    if SETTINGS.pg_enabled:
        try:
            from sqlalchemy import create_engine

            engine = create_engine(SETTINGS.pg_url)
            schema = SETTINGS.pg["schema"]
            df = pd.read_sql_table(name, engine, schema=schema)
            engine.dispose()
            return df
        except Exception as exc:  # noqa: BLE001
            st.sidebar.warning(f"Postgres indisponível ({exc}); lendo arquivos gold.")

    path = SETTINGS.table_path("gold", name)
    # Leitura via pandas/pyarrow (parquet). Para Delta, use delta-rs/connector.
    return pd.read_parquet(path)


st.title("🛠️ Painel de Chamados — Service Desk")
st.caption(
    "Relatório gerencial automatizado pelo pipeline lakehouse "
    "(bronze → silver → gold). Substitui a consolidação manual em planilhas."
)

try:
    kpis = load_table("mart_kpi_ciclo").sort_values("competencia")
    ciclos = list(kpis["competencia"])
    ciclo = st.sidebar.selectbox(
        "Ciclo (mês de competência)", ciclos, index=len(ciclos) - 1
    )
    st.sidebar.caption(
        "Competência = mês de **abertura** do chamado. Chamados abertos no "
        "ciclo e resolvidos depois atualizam o relatório retroativamente."
    )
    kpi = kpis[kpis["competencia"] == ciclo].iloc[0]

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Chamados", f"{int(kpi['total_chamados']):,}".replace(",", "."))
    c2.metric("Backlog", int(kpi["backlog"]))
    c3.metric(
        "Backlog estourado",
        int(kpi["backlog_estourado"]),
        help="Chamados ainda abertos cuja idade já passou do SLA da prioridade — onde agir primeiro.",
    )
    c4.metric(
        "Dentro do SLA",
        "—" if pd.isna(kpi["pct_dentro_sla"]) else f"{kpi['pct_dentro_sla']:.1f}%",
        help="Só chamados resolvidos; cancelados e abertos ficam fora do universo da métrica.",
    )
    c5.metric(
        "Satisfação média",
        "—" if pd.isna(kpi["media_satisfacao"]) else f"{kpi['media_satisfacao']:.2f}/5",
    )
    c6.metric(
        "Taxa de reabertura",
        "—" if pd.isna(kpi["taxa_reabertura_pct"]) else f"{kpi['taxa_reabertura_pct']:.1f}%",
    )

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Evolução por ciclo")
        evolucao = kpis.set_index("competencia")[
            ["total_chamados", "resolvidos", "backlog", "backlog_estourado"]
        ]
        st.line_chart(evolucao)
    with col_b:
        st.subheader(f"Volume diário em {ciclo} (média móvel 7d)")
        vol = load_table("mart_volume_diario")
        vol["opened_date"] = vol["opened_date"].astype(str)
        vol_ciclo = vol[vol["opened_date"].str.startswith(ciclo)].sort_values("opened_date")
        st.line_chart(vol_ciclo.set_index("opened_date")[["total_chamados", "media_movel_7d"]])

    col_c, col_d = st.columns(2)
    with col_c:
        st.subheader(f"Aderência a SLA por categoria — {ciclo}")
        sla = load_table("mart_sla_categoria")
        sla_ciclo = sla[sla["competencia"] == ciclo].drop(columns=["competencia"])
        st.dataframe(sla_ciclo, use_container_width=True, hide_index=True)
    with col_d:
        st.subheader(f"Produtividade por técnico — {ciclo} (top 10)")
        tech = load_table("mart_produtividade_tecnico")
        tech_ciclo = (
            tech[tech["competencia"] == ciclo]
            .sort_values("rank_volume")
            .drop(columns=["competencia"])
            .head(10)
        )
        st.dataframe(tech_ciclo, use_container_width=True, hide_index=True)

    st.subheader(f"🔥 Backlog estourado — {ciclo}")
    fato = load_table("fato_chamados")
    estourados = fato[
        (fato["competencia"] == ciclo) & (fato["backlog_estourado"] == True)  # noqa: E712
    ][
        ["ticket_id", "opened_at", "category", "priority", "technician_name",
         "sla_hours", "horas_em_aberto"]
    ].sort_values("horas_em_aberto", ascending=False)
    if estourados.empty:
        st.success("Nenhum chamado aberto além do prazo neste ciclo. 🎉")
    else:
        st.dataframe(estourados, use_container_width=True, hide_index=True)
except Exception as exc:  # noqa: BLE001
    st.error(
        "Não foi possível carregar as tabelas gold. Rode o pipeline primeiro:\n\n"
        "`python -m servicedesk_pipeline.run_pipeline`\n\n"
        f"Detalhe: {exc}"
    )
