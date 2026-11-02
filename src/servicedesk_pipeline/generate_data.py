"""Simulador de dados de chamados de Service Desk.

Por que dados sintéticos: a experiência real de origem (chamados corporativos)
está sob NDA e não pode ser publicada. Este gerador recria a *estrutura* e as
imperfeições típicas desses dados (duplicatas, nulos, caixa inconsistente,
durações inválidas), para que o pipeline exercite limpeza e qualidade de forma
realista — sem expor nenhum dado de cliente.

O gerador é determinístico (seed fixa) para que os testes sejam reprodutíveis.
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

CHANNELS = ["Telefone", "E-mail", "Portal", "Chat"]
CATEGORIES = ["Hardware", "Software", "Rede", "Acesso", "E-mail", "Impressora", "VPN", "Outros"]
PRIORITIES = ["Baixa", "Média", "Alta", "Crítica"]
# Peso para tornar o volume por prioridade realista (muita coisa Média/Baixa).
PRIORITY_WEIGHTS = [0.30, 0.42, 0.20, 0.08]
SITES = ["SP-Matriz", "SP-FreguesiaDoO", "RJ-Centro", "BH-Savassi", "Remoto"]
STATUSES = ["Resolvido", "Resolvido", "Resolvido", "Reaberto", "Cancelado"]

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


def _dirty_category(category: str, rng: random.Random) -> str:
    """Introduz inconsistência de caixa/espaços, como em dado real."""
    roll = rng.random()
    if roll < 0.08:
        return f"  {category.lower()} "   # espaços + minúsculo
    if roll < 0.14:
        return category.upper()
    return category


def generate_tickets(n: int, seed: int = 42, start: datetime | None = None) -> list[dict]:
    """Gera ``n`` chamados sintéticos como lista de dicionários.

    Imperfeições injetadas de propósito:
      * ~2% de duplicatas exatas de ticket_id (reenvio do mesmo evento);
      * ~3% de satisfaction_score nulo;
      * ~1% de resolution_minutes negativo (relógio/registro inconsistente);
      * caixa e espaços inconsistentes em category.
    """
    rng = random.Random(seed)
    start = start or datetime(2026, 1, 1, 8, 0, 0)
    tickets: list[dict] = []

    for i in range(n):
        opened = start + timedelta(minutes=rng.randint(0, 60 * 24 * 300))
        priority = rng.choices(PRIORITIES, weights=PRIORITY_WEIGHTS, k=1)[0]
        base = BASE_RESOLUTION_MIN[priority]
        resolution_min = max(5, int(rng.gauss(base, base * 0.4)))
        first_response_min = max(1, int(resolution_min * rng.uniform(0.05, 0.3)))
        tech_id = rng.choice(list(TECHNICIANS))
        status = rng.choice(STATUSES)
        reopened = status == "Reaberto"

        score = rng.choice([3, 4, 4, 5, 5, 5, 2, 1])
        if rng.random() < 0.03:
            score = None  # satisfação não informada

        if rng.random() < 0.01:
            resolution_min = -resolution_min  # dado inválido proposital

        closed = opened + timedelta(minutes=abs(resolution_min))

        ticket = {
            "ticket_id": f"CH{1_000_000 + i}",
            "opened_at": opened.isoformat(),
            "closed_at": closed.isoformat() if status != "Cancelado" else None,
            "channel": rng.choice(CHANNELS),
            "category": _dirty_category(rng.choice(CATEGORIES), rng),
            "priority": priority,
            "technician_id": tech_id,
            "technician_name": TECHNICIANS[tech_id],
            "requester_id": f"U{rng.randint(2000, 9000)}",
            "site": rng.choice(SITES),
            "status": status,
            "first_response_minutes": first_response_min,
            "resolution_minutes": resolution_min,
            "satisfaction_score": score,
            "reopened": reopened,
        }
        tickets.append(ticket)

        if rng.random() < 0.02:  # duplicata exata
            tickets.append(dict(ticket))

    return tickets


def write_raw(tickets: list[dict], raw_path: str, raw_format: str = "json") -> str:
    """Grava os chamados brutos no diretório raw (JSON lines ou CSV).

    Usa o sistema de arquivos local; para S3 o ingest usa o próprio Spark.
    """
    out_dir = Path(raw_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

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

    return str(target)


def main() -> None:
    from .config import load_settings

    settings = load_settings()
    tickets = generate_tickets(settings.tickets_per_run)
    target = write_raw(tickets, settings.raw_path(), settings.raw_format)
    print(f"[generate_data] {len(tickets)} chamados -> {target}")


if __name__ == "__main__":
    main()
