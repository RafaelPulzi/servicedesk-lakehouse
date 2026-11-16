"""Constantes do domínio de chamados (vocabulário canônico em GLOSSARY.md).

Fonte única dos domínios de valor compartilhados pelo simulador, pela limpeza
silver e pelo gate de qualidade — os três precisam concordar sobre o que é um
status válido, e duplicar o conjunto em cada módulo é divergência esperando
acontecer. O domínio de prioridades NÃO mora aqui: ele deriva das chaves de
``sla_hours`` no config, que é a fonte única de verdade das prioridades.
"""
from __future__ import annotations

# Ciclo de vida do chamado. "Reaberto" não é status: reabertura é uma flag
# cumulativa (`reopened`) — uma vez reaberto, o chamado conta como reaberto
# para sempre, mesmo que volte a ser Resolvido (ver GLOSSARY.md).
STATUS_ABERTO = "Aberto"
STATUS_EM_ANDAMENTO = "Em Andamento"
STATUS_RESOLVIDO = "Resolvido"
STATUS_CANCELADO = "Cancelado"

OPEN_STATUSES = (STATUS_ABERTO, STATUS_EM_ANDAMENTO)
STATUS_DOMAIN = (*OPEN_STATUSES, STATUS_RESOLVIDO, STATUS_CANCELADO)
