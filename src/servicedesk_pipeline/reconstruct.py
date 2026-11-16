"""Reconstrução "como publicado" — a edição do relatório de um ciclo passado.

A competência de um chamado é o mês da sua abertura, então a edição de um
ciclo muda retroativamente a cada recálculo (ver spec: issue #1). A bronze é
append-only de exports completos (ADR 0002), logo toda versão de todo chamado
está preservada — reconstruir a edição é só restringir o universo de exports:

  1. localizar o export do ciclo alvo: a maior ``export_date`` da bronze cuja
     competência (mês da data de referência) é o ciclo;
  2. cortar a bronze em ``export_date <=`` essa data;
  3. compor as transformações puras já existentes (``clean`` da silver e
     ``build_gold`` da gold) sobre a bronze cortada.

Nenhuma regra de negócio é duplicada: a dedup "última versão por chamado"
passa a valer dentro do universo cortado, e a ``data_referencia`` do fato
(``MAX(export_date)`` da silver) cai naturalmente no export do ciclo — então
backlog e backlog estourado são medidos contra o relógio da época.

A edição expõe também a silver reconstruída, porque o gate de qualidade
(ADR 0001) roda sobre ela antes de qualquer escrita.
"""
from __future__ import annotations

from typing import NamedTuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import Settings
from .transform_gold import build_gold
from .transform_silver import clean

# Formato de ciclo/competência: "YYYY-MM" (o mesmo da coluna `competencia`).
_CICLO_FMT = "yyyy-MM"


class CicloSemExportError(ValueError):
    """O ciclo pedido não tem export correspondente na bronze."""


class Edicao(NamedTuple):
    """O relatório de um ciclo como publicado: silver, fato e marts."""

    ciclo: str
    silver: DataFrame
    models: dict[str, DataFrame]


def reconstruct_edition(bronze: DataFrame, ciclo: str, settings: Settings) -> Edicao:
    """Reconstrói a edição do `ciclo` a partir da bronze. Função pura (sem IO)."""
    exports = (
        bronze.select(F.to_timestamp("export_date").alias("export_date"))
        .where(F.col("export_date").isNotNull())
        .withColumn("ciclo", F.date_format("export_date", _CICLO_FMT))
        .groupBy("ciclo")
        .agg(F.max("export_date").alias("export_do_ciclo"))
        .collect()
    )
    por_ciclo = {r["ciclo"]: r["export_do_ciclo"] for r in exports}
    if ciclo not in por_ciclo:
        disponiveis = ", ".join(sorted(por_ciclo)) or "nenhum"
        raise CicloSemExportError(
            f"Ciclo {ciclo} sem export na bronze. Ciclos disponíveis: {disponiveis}."
        )

    # Universo da edição: só o que era conhecido até o export do ciclo alvo.
    corte = por_ciclo[ciclo]
    bronze_cortada = bronze.where(F.to_timestamp("export_date") <= F.lit(corte))

    silver = clean(bronze_cortada, settings)
    return Edicao(ciclo=ciclo, silver=silver, models=build_gold(silver))
