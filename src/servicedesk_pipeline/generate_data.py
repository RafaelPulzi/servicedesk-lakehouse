"""Simulador de dados de chamados de Service Desk.

Por que dados sintéticos: a experiência real de origem (chamados corporativos)
está sob NDA e não pode ser publicada. Este gerador recria a *estrutura* e as
imperfeições típicas desses dados (duplicatas, nulos, caixa inconsistente,
durações inválidas), para que o pipeline exercite limpeza e qualidade de forma
realista — sem expor nenhum dado de cliente.

Como a origem real entrega: um **export completo por ciclo mensal** (nunca
deltas), com data de referência explícita. O simulador reproduz isso: sorteia o
ciclo de vida completo de cada chamado (a "verdade") e tira uma *foto* dele na
data de cada export. O mesmo `ticket_id` aparece em exports sucessivos com
estados diferentes (Aberto -> Em Andamento -> Resolvido, às vezes reaberto no
meio) — são essas versões divergentes que a silver deduplica.

O gerador é determinístico (seed fixa) para que os testes sejam reprodutíveis.
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from .domain import (
    STATUS_ABERTO,
    STATUS_CANCELADO,
    STATUS_EM_ANDAMENTO,
    STATUS_RESOLVIDO,
)

CHANNELS = ["Telefone", "E-mail", "Portal", "Chat"]
CATEGORIES = ["Hardware", "Software", "Rede", "Acesso", "E-mail", "Impressora", "VPN", "Outros"]
PRIORITIES = ["Baixa", "Média", "Alta", "Crítica"]
# Peso para tornar o volume por prioridade realista (muita coisa Média/Baixa).
PRIORITY_WEIGHTS = [0.30, 0.42, 0.20, 0.08]
SITES = ["SP-Matriz", "SP-FreguesiaDoO", "RJ-Centro", "BH-Savassi", "Remoto"]

# Técnicos fictícios (id -> nome).
TECHNICIANS = {
    f"T{100 + i}": name
    for i, name in enumerate(
        [
            "Ana Souza", "Bruno Lima", "Carla Dias", "Diego Rocha", "Elisa Faria",
            "Felipe Nunes", "Gabriela Reis", "Hugo Martins", "Isabela Cruz", "João Prado",
            "Karina Melo", "Lucas Amaral",
        ]
    )
}

# Tempo-base de resolução por prioridade (minutos), antes de ruído.
BASE_RESOLUTION_MIN = {"Crítica": 90, "Alta": 240, "Média": 600, "Baixa": 1800}

# Probabilidades do ciclo de vida e das imperfeições injetadas.
P_CANCELADO = 0.08          # chamado encerrado sem resolução
P_REABERTURA = 0.06         # resolvido que volta a ficar ativo
P_DUPLICATA = 0.02          # reenvio exato do mesmo registro dentro do export
P_SATISFACAO_NULA = 0.03    # satisfação não informada
P_DURACAO_NEGATIVA = 0.01   # relógio/registro inconsistente na origem


def _month_ends(start: datetime, cycles: int) -> list[datetime]:
    """Datas de referência dos exports: o último instante de cada mês."""
    ends: list[datetime] = []
    year, month = start.year, start.month
    for _ in range(cycles):
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
        ends.append(datetime(year, month, 1) - timedelta(seconds=1))
    return ends


def _dirty_category(category: str, rng: random.Random) -> str:
    """Introduz inconsistência de caixa/espaços, como em dado real."""
    roll = rng.random()
    if roll < 0.08:
        return f"  {category.lower()} "   # espaços + minúsculo
    if roll < 0.14:
        return category.upper()
    return category


def _simulate_lifecycle(i: int, rng: random.Random, start: datetime, end: datetime) -> dict:
    """Sorteia a "verdade" do ciclo de vida completo de um chamado.

    A verdade não aparece no raw: cada export recebe só a foto do chamado na
    data de referência (ver ``_snapshot``).
    """
    span_min = int((end - start).total_seconds() // 60)
    opened = start + timedelta(minutes=rng.randint(0, span_min - 1))
    priority = rng.choices(PRIORITIES, weights=PRIORITY_WEIGHTS, k=1)[0]
    tech_id = rng.choice(list(TECHNICIANS))

    truth: dict = {
        "ticket_id": f"CH{1_000_000 + i}",
        "opened": opened,
        "channel": rng.choice(CHANNELS),
        "category": rng.choice(CATEGORIES),
        "priority": priority,
        "technician_id": tech_id,
        "technician_name": TECHNICIANS[tech_id],
        "requester_id": f"U{rng.randint(2000, 9000)}",
        "site": rng.choice(SITES),
        "cancel_at": None,
        "first_response_min": None,
        "closed_first": None,
        "reopen_at": None,
        "closed_final": None,
        "resolution_first_min": None,
        "resolution_total_min": None,
        "satisfaction": None,
    }

    if rng.random() < P_CANCELADO:
        # Encerrado sem resolução (desistência / abertura indevida).
        truth["cancel_at"] = opened + timedelta(minutes=rng.randint(30, 7 * 24 * 60))
        return truth

    base = BASE_RESOLUTION_MIN[priority]
    res1 = max(5, int(rng.gauss(base, base * 0.4)))
    truth["first_response_min"] = max(1, int(res1 * rng.uniform(0.05, 0.3)))
    truth["closed_first"] = opened + timedelta(minutes=res1)
    truth["resolution_first_min"] = res1

    if rng.random() < P_REABERTURA:
        # A solução não se sustentou: reabre e resolve de novo.
        reopen_at = truth["closed_first"] + timedelta(minutes=rng.randint(60, 72 * 60))
        res2 = max(5, int(rng.gauss(base * 0.6, base * 0.3)))
        truth["reopen_at"] = reopen_at
        truth["closed_final"] = reopen_at + timedelta(minutes=res2)
        truth["resolution_total_min"] = int(
            (truth["closed_final"] - opened).total_seconds() // 60
        )
    else:
        truth["closed_final"] = truth["closed_first"]
        truth["resolution_total_min"] = res1

    score = rng.choice([3, 4, 4, 5, 5, 5, 2, 1])
    truth["satisfaction"] = None if rng.random() < P_SATISFACAO_NULA else score
    return truth


def _snapshot(truth: dict, as_of: datetime, rng: random.Random) -> dict | None:
    """A foto do chamado na data de referência de um export (ou None se ainda não existia)."""
    opened: datetime = truth["opened"]
    if opened > as_of:
        return None

    status = STATUS_ABERTO
    closed_at: datetime | None = None
    resolution_min: int | None = None
    satisfaction: int | None = None
    first_response: int | None = None
    reopened = False

    if truth["cancel_at"] is not None:
        if truth["cancel_at"] <= as_of:
            status, closed_at = STATUS_CANCELADO, truth["cancel_at"]
        # antes do cancelamento o chamado aparece como Aberto
    else:
        first_response_at = opened + timedelta(minutes=truth["first_response_min"])
        if truth["reopen_at"] is not None and as_of >= truth["closed_final"]:
            status = STATUS_RESOLVIDO
            closed_at = truth["closed_final"]
            resolution_min = truth["resolution_total_min"]
            reopened = True
        elif truth["reopen_at"] is not None and as_of >= truth["reopen_at"]:
            status, reopened = STATUS_EM_ANDAMENTO, True
        elif as_of >= truth["closed_first"]:
            status = STATUS_RESOLVIDO
            closed_at = truth["closed_first"]
            resolution_min = truth["resolution_first_min"]
        elif as_of >= first_response_at:
            status = STATUS_EM_ANDAMENTO
        if as_of >= first_response_at:
            first_response = truth["first_response_min"]

    if status == STATUS_RESOLVIDO:
        satisfaction = truth["satisfaction"]
        if resolution_min is not None and rng.random() < P_DURACAO_NEGATIVA:
            resolution_min = -resolution_min  # dado inválido proposital

    return {
        "ticket_id": truth["ticket_id"],
        "opened_at": opened.isoformat(),
        "closed_at": closed_at.isoformat() if closed_at else None,
        "channel": truth["channel"],
        "category": _dirty_category(truth["category"], rng),
        "priority": truth["priority"],
        "technician_id": truth["technician_id"],
        "technician_name": truth["technician_name"],
        "requester_id": truth["requester_id"],
        "site": truth["site"],
        "status": status,
        "first_response_minutes": first_response,
        "resolution_minutes": resolution_min,
        "satisfaction_score": satisfaction,
        "reopened": reopened,
        "export_date": as_of.isoformat(),
    }


def generate_exports(
    n: int,
    cycles: int = 6,
    seed: int = 42,
    start: datetime | None = None,
) -> list[dict]:
    """Gera a sequência de exports mensais de ``n`` chamados.

    Devolve uma lista de ``{"export_date": datetime, "tickets": [dict, ...]}``,
    um item por ciclo, em ordem cronológica. Cada export é *completo* (todos os
    chamados abertos até a data de referência, no estado daquele momento) —
    fiel a como a origem real entrega.
    """
    rng = random.Random(seed)
    start = start or datetime(2026, 1, 1, 8, 0, 0)
    export_dates = _month_ends(start, cycles)

    truths = [_simulate_lifecycle(i, rng, start, export_dates[-1]) for i in range(n)]

    exports: list[dict] = []
    for as_of in export_dates:
        tickets: list[dict] = []
        for truth in truths:
            snap = _snapshot(truth, as_of, rng)
            if snap is None:
                continue
            tickets.append(snap)
            if rng.random() < P_DUPLICATA:
                tickets.append(dict(snap))  # reenvio exato do mesmo registro
        exports.append({"export_date": as_of, "tickets": tickets})
    return exports


def write_raw(exports: list[dict], raw_path: str, raw_format: str = "json") -> list[str]:
    """Grava um arquivo raw por export (JSON lines ou CSV), nomeado pela data de referência.

    Usa o sistema de arquivos local; para S3 o ingest usa o próprio Spark.
    """
    out_dir = Path(raw_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    targets: list[str] = []

    for export in exports:
        stamp = export["export_date"].strftime("%Y%m%d")
        tickets = export["tickets"]
        if raw_format == "json":
            target = out_dir / f"tickets_{stamp}.jsonl"
            with open(target, "w", encoding="utf-8") as fh:
                for t in tickets:
                    fh.write(json.dumps(t, ensure_ascii=False) + "\n")
        else:
            import csv

            target = out_dir / f"tickets_{stamp}.csv"
            with open(target, "w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(tickets[0].keys()))
                writer.writeheader()
                writer.writerows(tickets)
        targets.append(str(target))

    return targets


def main() -> None:
    from .config import load_settings

    settings = load_settings()
    exports = generate_exports(settings.tickets_per_run, cycles=settings.cycles)
    targets = write_raw(exports, settings.raw_path(), settings.raw_format)
    total = sum(len(e["tickets"]) for e in exports)
    print(f"[generate_data] {len(exports)} exports, {total} registros -> {targets[-1]}")


if __name__ == "__main__":
    main()
