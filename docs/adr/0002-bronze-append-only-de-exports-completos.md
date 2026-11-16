# Bronze append-only de exports completos

A origem só entrega **export completo por ciclo** (nunca deltas), então "ingestão incremental" aqui significa: cada export é **anexado** à bronze como uma nova carga (com `_ingested_at`), nunca sobrescrito. A bronze acumula a história dos exports e, com isso, cada versão de cada chamado — é o que torna o dado auditável e reprocessável.

A silver continua sendo recomputada por inteiro a cada run: a deduplicação "última versão por `ticket_id`" faz o papel de upsert. Consideramos MERGE incremental na silver e rejeitamos: o volume é pequeno e o recompute total é mais simples de auditar; seria otimização prematura.
