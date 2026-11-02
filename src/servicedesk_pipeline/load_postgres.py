"""Camada de consumo — carga das marts gold no PostgreSQL.

Usa pandas + SQLAlchemy (em vez de Spark JDBC) para não depender de um JAR de
driver: as marts gold são pequenas (agregadas), então cabem em memória com
folga. Em produção/Databricks, com volume maior, trocaria-se por Spark JDBC
(`df.write.jdbc(...)`) — o ponto de troca está isolado nesta função.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .config import Settings
from .io_utils import read_table

# Marts publicadas no banco de consumo.
SERVED_TABLES = (
    "mart_kpi_geral",
    "mart_sla_categoria",
    "mart_volume_diario",
    "dim_tecnico",
)


def run(spark: SparkSession, settings: Settings) -> None:
    if not settings.pg_enabled:
        print("[postgres] desabilitado (serving.postgres.enabled=false) — pulando.")
        return

    from sqlalchemy import create_engine, text

    engine = create_engine(settings.pg_url)
    schema = settings.pg["schema"]
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    for table in SERVED_TABLES:
        sdf = read_table(spark, settings.table_path("gold", table), settings)
        pdf = sdf.toPandas()
        pdf.to_sql(
            table,
            engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )
        print(f"[postgres] {table}: {len(pdf)} linhas -> {schema}.{table}")

    engine.dispose()
