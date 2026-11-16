"""Orquestrador do pipeline ponta a ponta.

Uso:
    python -m servicedesk_pipeline.run_pipeline                 # fluxo completo
    python -m servicedesk_pipeline.run_pipeline --steps bronze silver gold
    python -m servicedesk_pipeline.run_pipeline --no-postgres   # sem camada de consumo
    python -m servicedesk_pipeline.run_pipeline --reconstruir-ciclo 2026-01

Etapas: generate -> bronze -> silver -> quality -> gold -> postgres.
Com --reconstruir-ciclo executa SOMENTE a reconstrução "como publicado" da
edição do ciclo (bronze -> gate de qualidade -> gold sufixada pelo ciclo),
sem tocar a visão atual nem o Postgres.
Em produção este mesmo módulo é chamado como um Databricks Job (ver
notebooks/databricks_job.py) ou agendado via Airflow.
"""
from __future__ import annotations

import argparse
import time

from . import generate_data, ingest_bronze, load_postgres, quality_checks
from . import reconstruct, transform_gold, transform_silver
from .config import load_settings
from .io_utils import read_table
from .spark_session import build_spark

ALL_STEPS = ["generate", "bronze", "silver", "quality", "gold", "postgres"]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline lakehouse de chamados de Service Desk")
    parser.add_argument("--steps", nargs="+", choices=ALL_STEPS, default=ALL_STEPS)
    parser.add_argument("--no-postgres", action="store_true", help="pula a carga no Postgres")
    parser.add_argument("--config", default=None, help="caminho alternativo de settings.yaml")
    parser.add_argument(
        "--reconstruir-ciclo",
        metavar="YYYY-MM",
        default=None,
        help="reconstrói a edição 'como publicado' do ciclo e grava na gold "
        "sufixada pelo ciclo; executa somente a reconstrução",
    )
    args = parser.parse_args(argv)

    settings = load_settings(args.config)

    if args.reconstruir_ciclo:
        ciclo = args.reconstruir_ciclo
        print(f"== reconstrução da edição {ciclo} | backend={settings.backend} "
              f"formato={settings.table_format} ==")
        t0 = time.time()
        spark = build_spark(settings)
        try:
            reconstruct.run(spark, settings, ciclo)
        finally:
            spark.stop()
        print(f"== concluído em {time.time() - t0:.1f}s ==")
        return

    steps = [s for s in args.steps if not (args.no_postgres and s == "postgres")]

    print(f"== pipeline | backend={settings.backend} formato={settings.table_format} ==")
    t0 = time.time()

    # 'generate' não precisa de Spark.
    if "generate" in steps:
        exports = generate_data.generate_exports(settings.tickets_per_run, cycles=settings.cycles)
        targets = generate_data.write_raw(exports, settings.raw_path(), settings.raw_format)
        total = sum(len(e["tickets"]) for e in exports)
        print(f"[generate] {len(targets)} exports, {total} registros -> {settings.raw_path()}")

    spark = build_spark(settings)
    try:
        if "bronze" in steps:
            ingest_bronze.run(spark, settings)

        if "silver" in steps:
            transform_silver.run(spark, settings)

        if "quality" in steps:
            # Revalida a silver persistida. O gate principal roda DENTRO da
            # etapa silver, antes do write (ADR 0001); este passo existe para
            # auditar a tabela publicada ou rodar as checagens isoladamente.
            silver = read_table(spark, settings.table_path("silver", "tickets"), settings)
            quality_checks.assert_all(quality_checks.check_silver(silver, settings))

        if "gold" in steps:
            transform_gold.run(spark, settings)

        if "postgres" in steps:
            load_postgres.run(spark, settings)
    finally:
        spark.stop()

    print(f"== concluído em {time.time() - t0:.1f}s ==")


if __name__ == "__main__":
    main()
