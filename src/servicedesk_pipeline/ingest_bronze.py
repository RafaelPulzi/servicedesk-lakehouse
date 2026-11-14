"""Camada BRONZE — ingestão crua, fiel à origem, com metadados de carga.

Regras da camada bronze (ADR 0002):
  * ingerir o dado como ele chega, sem limpeza de regra de negócio;
  * **append-only**: cada export é anexado como uma nova carga, nunca
    sobrescrito — a bronze acumula a história dos exports e, com isso, todas
    as versões de cada chamado (auditável e reprocessável);
  * só adicionamos metadados de proveniência (quando/onde foi ingerido).

Reingerir o mesmo export gera cargas repetidas na bronze — por design: a
bronze registra o que chegou, e a silver reduz tudo à última versão de cada
chamado.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .io_utils import write_table


def list_export_files(settings: Settings) -> list[str]:
    """Arquivos de export no diretório raw, em ordem cronológica.

    No backend local listamos os arquivos para ingerir export a export; no S3
    devolvemos o diretório inteiro (o Spark lista por baixo) — a identidade de
    cada export segue garantida pela coluna ``export_date`` do próprio dado.
    """
    if settings.backend == "s3":
        return [settings.raw_path()]
    pattern = "*.jsonl" if settings.raw_format == "json" else "*.csv"
    return sorted(str(f) for f in Path(settings.raw_path()).glob(pattern))


def read_raw(spark: SparkSession, settings: Settings, source: str) -> DataFrame:
    if settings.raw_format == "json":
        # cada arquivo é JSON lines
        return spark.read.json(source)
    return spark.read.option("header", True).csv(source)


def to_bronze(df: DataFrame, source_path: str) -> DataFrame:
    """Anexa metadados de ingestão ao dado cru."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source_path))
        .withColumn("_source_file", F.input_file_name())
    )


def run(spark: SparkSession, settings: Settings) -> None:
    target = settings.table_path("bronze", "tickets")
    sources = list_export_files(settings)
    if not sources:
        raise FileNotFoundError(f"Nenhum export encontrado em {settings.raw_path()}")

    total = 0
    for source in sources:
        bronze = to_bronze(read_raw(spark, settings, source), source)
        write_table(bronze, target, settings, mode="append")
        n = bronze.count()
        total += n
        print(f"[bronze] +{n} linhas ({Path(source).name}) -> {target}")
    print(f"[bronze] carga concluída: {total} linhas anexadas em {len(sources)} export(s)")
