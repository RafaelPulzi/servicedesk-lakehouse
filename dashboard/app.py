"""Dashboard de chamados de Service Desk (Streamlit).

Lê as marts da camada gold. Por padrão lê do PostgreSQL (camada de consumo);
se o banco não estiver disponível, cai para a leitura direta dos Parquet/Delta
da gold no sistema de arquivos — assim o dashboard sobe mesmo sem Postgres.

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
def load_mart(name: str) -> pd.DataFrame:
    """Carrega uma mart do Postgres; se falhar, lê os arquivos gold direto."""
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
    fmt = SETTINGS.table_format
    # Leitura via pandas/pyarrow (parquet). Para Delta, use delta-rs/connector.
    return pd.read_parquet(path) if fmt == "parquet" else pd.read_parquet(path)


st.title("🛠️ Painel de Chamados — Service Desk")
st.caption(
    "Relatório gerencial automatizado pelo pipeline lakehouse "
    "(bronze → silver → gold). Substitui a consolidação manual em planilhas."
)

try:
    kpi = load_mart("mart_kpi_geral").iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Chamados", f"{int(kpi['total_chamados']):,}".replace(",", "."))
    c2.metric("Horas médias p/ resolução", f"{kpi['media_horas_resolucao']:.1f}h")
    c3.metric("Dentro do SLA", f"{kpi['pct_dentro_sla']:.1f}%")
    c4.metric("Satisfação média", f"{kpi['media_satisfacao']:.2f}/5")
    c5.metric("Taxa de reabertura", f"{kpi['taxa_reabertura_pct']:.1f}%")

    st.subheader("Volume diário e média móvel (7 dias)")
    vol = load_mart("mart_volume_diario").sort_values("opened_date")
    vol = vol.set_index("opened_date")[["total_chamados", "media_movel_7d"]]
    st.line_chart(vol)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Aderência a SLA por categoria")
        sla = load_mart("mart_sla_categoria")
        st.dataframe(sla, use_container_width=True, hide_index=True)
    with col_b:
        st.subheader("Produtividade por técnico (top 10)")
        tech = load_mart("dim_tecnico").sort_values("total_chamados", ascending=False).head(10)
        st.dataframe(tech, use_container_width=True, hide_index=True)
except Exception as exc:  # noqa: BLE001
    st.error(
        "Não foi possível carregar as marts. Rode o pipeline primeiro:\n\n"
        "`python -m servicedesk_pipeline.run_pipeline`\n\n"
        f"Detalhe: {exc}"
    )
