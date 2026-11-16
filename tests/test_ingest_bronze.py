"""Testes da ingestão bronze (append-only, export a export)."""
from __future__ import annotations

from datetime import datetime

import pytest

from servicedesk_pipeline.config import load_settings
from servicedesk_pipeline.generate_data import generate_exports, write_raw
from servicedesk_pipeline.ingest_bronze import list_export_files, run
from servicedesk_pipeline.io_utils import read_table


@pytest.fixture()
def tmp_settings(tmp_path):
    s = load_settings()
    s.raw["storage"]["table_format"] = "parquet"
    s.raw["storage"]["backend"] = "local"
    s.raw["storage"]["local_root"] = str(tmp_path)
    return s


def _write_exports(settings, cycles=2, n=60):
    exports = generate_exports(n, cycles=cycles, seed=9, start=datetime(2026, 1, 1, 8))
    return write_raw(exports, settings.raw_path(), settings.raw_format)


def test_lista_exports_em_ordem_cronologica(tmp_settings):
    _write_exports(tmp_settings, cycles=3)
    files = list_export_files(tmp_settings)
    assert len(files) == 3
    assert files == sorted(files)


def test_bronze_e_append_only(spark, tmp_settings):
    _write_exports(tmp_settings, cycles=2)
    run(spark, tmp_settings)
    bronze = read_table(spark, tmp_settings.table_path("bronze", "tickets"), tmp_settings)
    primeira_carga = bronze.count()
    assert primeira_carga > 0

    # Reingerir NÃO sobrescreve: a bronze acumula a história das cargas
    # (ADR 0002); é a silver que reduz à última versão de cada chamado.
    run(spark, tmp_settings)
    bronze = read_table(spark, tmp_settings.table_path("bronze", "tickets"), tmp_settings)
    assert bronze.count() == 2 * primeira_carga


def test_bronze_carrega_metadados_de_proveniencia(spark, tmp_settings):
    _write_exports(tmp_settings, cycles=2)
    run(spark, tmp_settings)
    bronze = read_table(spark, tmp_settings.table_path("bronze", "tickets"), tmp_settings)
    assert {"_ingested_at", "_source", "_source_file"} <= set(bronze.columns)
    # cada export entrou como uma carga distinta
    fontes = {r["_source"] for r in bronze.select("_source").distinct().collect()}
    assert len(fontes) == 2
