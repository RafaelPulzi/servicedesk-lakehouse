"""Camada BRONZE — ingestão crua, fiel à origem, com metadados de carga.

Regra da camada bronze: ingerir o dado como ele chega, sem limpeza de regra de
negócio. Só adicionamos metadados de proveniência (quando/onde foi ingerido),
o que permite reprocessar e auditar.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .io_utils import write_table


def read_raw(spark: SparkSession, settings: Settings) -> DataFrame:
    raw_path = settings.raw_path()
    if settings.raw_format == "json":
        # cada arquivo é JSON lines
        return spark.read.json(raw_path)
    return spark.read.option("header", True).csv(raw_path)


def to_bronze(df: DataFrame, source_path: str) -> DataFrame:
    """Anexa metadados de ingestão ao dado cru."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source_path))
        .withColumn("_source_file", F.input_file_name())
    )


def run(spark: SparkSession, settings: Settings) -> DataFrame:
    raw = read_raw(spark, settings)
    bronze = to_bronze(raw, settings.raw_path())
    target = settings.table_path("bronze", "tickets")
    write_table(bronze, target, settings, mode="overwrite")
    print(f"[bronze] {bronze.count()} linhas -> {target}")
    return bronze
