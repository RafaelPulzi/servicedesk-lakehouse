"""Checagens de qualidade de dados entre as camadas.

Rodam como um "gate": se uma asserção crítica falha, o pipeline para antes de
publicar dado ruim na camada de consumo. Cada checagem devolve um resultado
estruturado, o que facilita logar e, no futuro, exportar para observabilidade.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


class DataQualityError(RuntimeError):
    """Levantada quando uma checagem crítica falha."""


def check_silver(df: DataFrame) -> list[CheckResult]:
    """Conjunto de checagens sobre a camada silver."""
    results: list[CheckResult] = []

    total = df.count()
    results.append(CheckResult("silver_nao_vazia", total > 0, f"{total} linhas"))

    # ticket_id único (dedup funcionou).
    distinct_ids = df.select("ticket_id").distinct().count()
    results.append(
        CheckResult(
            "ticket_id_unico",
            distinct_ids == total,
            f"{distinct_ids} ids distintos de {total} linhas",
        )
    )

    # ticket_id nunca nulo.
    null_ids = df.filter(F.col("ticket_id").isNull()).count()
    results.append(CheckResult("ticket_id_nao_nulo", null_ids == 0, f"{null_ids} nulos"))

    # Nenhuma duração negativa sobreviveu à limpeza.
    negativos = df.filter(F.col("resolution_minutes") < 0).count()
    results.append(
        CheckResult("sem_duracao_negativa", negativos == 0, f"{negativos} negativos")
    )

    # Domínio de prioridade esperado.
    prioridades_validas = {"Baixa", "Média", "Alta", "Crítica"}
    achadas = {r["priority"] for r in df.select("priority").distinct().collect()}
    invalidas = achadas - prioridades_validas
    results.append(
        CheckResult(
            "dominio_prioridade",
            len(invalidas) == 0,
            f"inesperadas: {sorted(invalidas)}" if invalidas else "ok",
        )
    )

    # satisfaction_score, quando presente, entre 1 e 5.
    fora_faixa = df.filter(
        F.col("satisfaction_score").isNotNull()
        & ~F.col("satisfaction_score").between(1, 5)
    ).count()
    results.append(
        CheckResult("satisfacao_na_faixa", fora_faixa == 0, f"{fora_faixa} fora de 1..5")
    )

    return results


def assert_all(results: list[CheckResult]) -> None:
    """Loga cada checagem e levanta erro se alguma falhou."""
    falhas = [r for r in results if not r.passed]
    for r in results:
        marca = "OK " if r.passed else "FALHOU"
        print(f"[quality] {marca} {r.name}: {r.detail}")
    if falhas:
        nomes = ", ".join(r.name for r in falhas)
        raise DataQualityError(f"Checagens de qualidade falharam: {nomes}")
