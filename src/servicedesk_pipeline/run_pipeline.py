"""Orquestrador do pipeline ponta a ponta.

Uso:
    python -m servicedesk_pipeline.run_pipeline                 # fluxo completo
    python -m servicedesk_pipeline.run_pipeline --steps bronze silver gold
    python -m servicedesk_pipeline.run_pipeline --no-postgres   # sem camada de consumo

Etapas: generate -> bronze -> silver -> quality -> gold -> postgres.
Em produção este mesmo módulo é chamado como um Databricks Job (ver
notebooks/databricks_job.py) ou agendado via Airflow.
"""
from __future__ import annotations

import argparse
import time

from . import generate_data, ingest_bronze, load_postgres, quality_checks
from . import transform_gold, transform_silver
from .config import load_settings
from .io_utils import read_table
from .spark_session import build_spark

ALL_STEPS = ["generate", "bronze", "silver", "quality", "gold", "postgres"]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline lakehouse de chamados de Service Desk")
    parser.add_argument("--steps", nargs="+", choices=ALL_STEPS, default=ALL_STEPS)
    parser.add_argument("--no-postgres", action="store_true", help="pula a carga no Postgres")
    parser.add_argument("--config", default=None, help="caminho alternativo de settings.yaml")
    args = parser.parse_args(argv)

    settings = load_settings(args.config)
    steps = [s for s in args.steps if not (args.no_postgres and s == "postgres")]

    print(f"== pipeline | backend={settings.backend} formato={settings.table_format} ==")
    t0 = time.time()

    # 'generate' não precisa de Spark.
    if "generate" in steps:
        tickets = generate_data.generate_tickets(settings.tickets_per_run)
        target = generate_data.write_raw(tickets, settings.raw_path(), settings.raw_format)
        print(f"[generate] {len(tickets)} chamados -> {target}")

    spark = build_spark(settings)
    try:
        if "bronze" in steps:
            ingest_bronze.run(spark, settings)

        if "silver" in steps:
            transform_silver.run(spark, settings)

        if "quality" in steps:
            silver = read_table(spark, settings.table_path("silver", "tickets"), settings)
            quality_checks.assert_all(quality_checks.check_silver(silver))

        if "gold" in steps:
            transform_gold.run(spark, settings)

        if "postgres" in steps:
            load_postgres.run(spark, settings)
    finally:
        spark.stop()

    print(f"== concluído em {time.time() - t0:.1f}s ==")


if __name__ == "__main__":
    main()
