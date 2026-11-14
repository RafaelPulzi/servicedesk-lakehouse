# Service Desk Lakehouse

Pipeline que transforma chamados de Service Desk em relatório gerencial. O vocabulário abaixo é a linguagem canônica do domínio — as métricas e regras valem tanto para o dado real (sob NDA) quanto para o simulado.

## Language

**Chamado**:
Uma solicitação registrada no Service Desk, identificada por `ticket_id`, com categoria, prioridade, técnico responsável e ciclo de vida próprio (aberto → resolvido/cancelado, podendo ser reaberto).
_Avoid_: ticket, incidente, registro

**Versão de chamado**:
O estado de um chamado em um dado momento. Um mesmo `ticket_id` pode aparecer mais de uma vez na origem porque o chamado mudou de estado; a versão mais recente é a válida. Um reenvio idêntico do mesmo evento é só um caso particular disso.
_Avoid_: duplicata (sugere erro; mudança de estado é comportamento normal da origem)

**Export**:
A entrega completa da origem em um ciclo: todos os chamados, no estado em que estão naquele momento, com uma data de referência explícita (a data da extração). A origem não entrega deltas. A idade do backlog se mede contra a data de referência do export mais recente.
_Avoid_: carga incremental, delta

**Ciclo**:
O período de competência do relatório gerencial: um mês. Cada ciclo corresponde a um export da origem e a uma edição do relatório.
_Avoid_: rodada, sprint

**SLA**:
O prazo máximo de resolução de um chamado, em horas, determinado exclusivamente pela sua prioridade.

**Dentro do SLA**:
Um chamado *encerrado como resolvido* cujo tempo de resolução não ultrapassou o SLA da sua prioridade. Chamados cancelados e chamados ainda abertos não são "dentro" nem "fora" do SLA: ficam fora do universo da métrica (o aberto ainda não tem resultado).
_Avoid_: aderente, no prazo

**Competência**:
O ciclo ao qual um chamado pertence para fins de relatório: o mês da sua **abertura**. Um chamado aberto em janeiro e resolvido em fevereiro pertence ao relatório de janeiro — que por isso pode mudar após o fim do mês.
_Avoid_: mês de fechamento, período de resolução

**Backlog**:
Os chamados ainda abertos de um ciclo (ainda sem resolução nem cancelamento). É um indicador do relatório tão importante quanto o volume.
_Avoid_: pendências, fila

**Chamado cancelado**:
Um chamado encerrado sem resolução (desistência, abertura indevida). Conta no volume total, mas é excluído das métricas de SLA, tempo de resolução e satisfação.

**Reabertura**:
Um chamado que foi dado como resolvido e voltou a ficar ativo porque a solução não se sustentou. É um atributo permanente do chamado ("foi reaberto alguma vez"), não um status: um chamado reaberto e resolvido de novo continua contando como reaberto. A taxa de reabertura mede qualidade da resolução, não volume.
_Avoid_: status "Reaberto"

**Backlog estourado**:
Os chamados do backlog cuja idade já ultrapassou o SLA da sua prioridade — ainda abertos e já fora do prazo. É o indicador mais acionável do relatório: aponta onde agir antes de o chamado se tornar "fora do SLA".
_Avoid_: atrasados, vencidos

**Técnico**:
A pessoa do Service Desk responsável pela resolução de um chamado. Unidade de análise das métricas de produtividade.
_Avoid_: analista, agente, atendente
