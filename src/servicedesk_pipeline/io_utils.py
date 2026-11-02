"""Leitura/escrita agnóstica de formato (Delta ou Parquet).

Todo o pipeline chama ``write_table`` / ``read_table`` em vez de fixar o
formato, de modo que trocar ``table_format`` no settings (delta <-> parquet)
não exige mudança de código.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .config import Settings


def write_table(
    df: DataFrame,
    path: str,
    settings: Settings,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    writer = df.write.format(settings.table_format).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if settings.table_format == "delta":
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


def read_table(spark: SparkSession, path: str, settings: Settings) -> DataFrame:
    return spark.read.format(settings.table_format).load(path)
