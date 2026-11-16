"""Checagens de qualidade de dados entre as camadas.

Rodam como um "gate": se uma asserção crítica falha, o pipeline **para** antes
de persistir a camada — escolhemos "nenhum relatório" em vez de "relatório
errado" (ADR 0001). Cada checagem devolve um resultado estruturado, o que
facilita logar e, no futuro, exportar para observabilidade.

Os domínios de valor vêm de uma fonte única: status de ``domain.py`` e
prioridades das chaves de ``sla_hours`` no config.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import Settings
from .domain import OPEN_STATUSES, STATUS_CANCELADO, STATUS_DOMAIN, STATUS_RESOLVIDO


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


class DataQualityError(RuntimeError):
    """Levantada quando uma checagem crítica falha."""


def check_silver(df: DataFrame, settings: Settings) -> list[CheckResult]:
    """Conjunto de checagens sobre a camada silver (antes de persistir)."""
    results: list[CheckResult] = []

    total = df.count()
    results.append(CheckResult("silver_nao_vazia", total > 0, f"{total} linhas"))

    # ticket_id único (dedup por versão funcionou).
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

    # Domínio de prioridade: deriva do config (fonte única, junto do SLA).
    prioridades_validas = set(settings.sla_hours)
    achadas = {r["priority"] for r in df.select("priority").distinct().collect()}
    invalidas = achadas - prioridades_validas
    results.append(
        CheckResult(
            "dominio_prioridade",
            len(invalidas) == 0,
            f"inesperadas: {sorted(invalidas)}" if invalidas else "ok",
        )
    )

    # Domínio de status (ciclo de vida canônico).
    status_achados = {r["status"] for r in df.select("status").distinct().collect()}
    status_invalidos = status_achados - set(STATUS_DOMAIN)
    results.append(
        CheckResult(
            "dominio_status",
            len(status_invalidos) == 0,
            f"inesperados: {sorted(status_invalidos)}" if status_invalidos else "ok",
        )
    )

    # Coerência do ciclo de vida: aberto não tem encerramento nem duração.
    abertos_incoerentes = df.filter(
        F.col("status").isin(*OPEN_STATUSES)
        & (F.col("closed_at").isNotNull() | F.col("resolution_minutes").isNotNull())
    ).count()
    results.append(
        CheckResult(
            "aberto_sem_encerramento",
            abertos_incoerentes == 0,
            f"{abertos_incoerentes} abertos com closed_at/duração",
        )
    )

    # Resolvido sempre tem encerramento e duração.
    resolvidos_incoerentes = df.filter(
        (F.col("status") == STATUS_RESOLVIDO)
        & (F.col("closed_at").isNull() | F.col("resolution_minutes").isNull())
    ).count()
    results.append(
        CheckResult(
            "resolvido_com_encerramento",
            resolvidos_incoerentes == 0,
            f"{resolvidos_incoerentes} resolvidos sem closed_at/duração",
        )
    )

    # Cancelado: encerrado, mas sem resolução (ver GLOSSARY.md).
    cancelados_incoerentes = df.filter(
        (F.col("status") == STATUS_CANCELADO)
        & (F.col("closed_at").isNull() | F.col("resolution_minutes").isNotNull())
    ).count()
    results.append(
        CheckResult(
            "cancelado_sem_resolucao",
            cancelados_incoerentes == 0,
            f"{cancelados_incoerentes} cancelados incoerentes",
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

    # export_date presente em todas as linhas (identidade da carga).
    sem_export = df.filter(F.col("export_date").isNull()).count()
    results.append(
        CheckResult("export_date_presente", sem_export == 0, f"{sem_export} sem export_date")
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
