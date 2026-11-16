# Gate de qualidade que para o pipeline

Quando uma checagem crítica de qualidade falha (chave duplicada, prioridade fora do domínio, duração negativa, satisfação fora de 1..5), o pipeline **para** — nenhuma camada a jusante é publicada. A alternativa considerada foi quarentena: segregar as linhas ruins, publicar as boas e reportar o desvio. Escolhemos parar porque o consumidor é um relatório gerencial: um relatório parcial publicado em silêncio é pior do que relatório nenhum, e o custo de uma intervenção manual é baixo neste domínio.

A validação roda sobre o DataFrame **antes** da escrita da silver: se falhar, nem a silver é persistida — "parar antes de publicar dado ruim" vale para todas as camadas, não só para a de consumo.
