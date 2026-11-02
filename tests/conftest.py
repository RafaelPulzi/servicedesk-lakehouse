"""Fixtures de teste: SparkSession local e Settings em modo Parquet.

Os testes rodam em Parquet (sem dependência do JAR do Delta), o que os torna
hermeticos e rápidos no CI. A lógica de transformação é a mesma em Delta.
"""
from __future__ import annotations

import pytest

from servicedesk_pipeline.config import load_settings


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    sess = (
        SparkSession.builder.appName("tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .getOrCreate()
    )
    sess.sparkContext.setLogLevel("ERROR")
    yield sess
    sess.stop()


@pytest.fixture(scope="session")
def settings():
    s = load_settings()
    # Garante que os testes não dependam de Delta nem de S3.
    s.raw["storage"]["table_format"] = "parquet"
    s.raw["storage"]["backend"] = "local"
    return s
