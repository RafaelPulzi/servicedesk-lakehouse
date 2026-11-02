.PHONY: help setup test lint pipeline pipeline-local dashboard up down clean

help:
	@echo "Alvos disponíveis:"
	@echo "  setup           instala as dependências (requirements.txt)"
	@echo "  test            roda a suíte de testes (pytest, formato parquet)"
	@echo "  pipeline        roda o pipeline completo (usa config/settings.yaml)"
	@echo "  pipeline-local  roda o pipeline em parquet, sem Postgres (demo rápida)"
	@echo "  dashboard       sobe o dashboard Streamlit"
	@echo "  up / down       sobe / derruba Postgres + MinIO (docker compose)"
	@echo "  clean           apaga ./data/lakehouse e caches"

setup:
	pip install -r requirements.txt

test:
	TABLE_FORMAT=parquet STORAGE_BACKEND=local pytest

pipeline:
	python -m servicedesk_pipeline.run_pipeline

# Demonstração sem dependências externas (sem Maven, sem Postgres).
pipeline-local:
	TABLE_FORMAT=parquet STORAGE_BACKEND=local \
	python -m servicedesk_pipeline.run_pipeline --no-postgres

dashboard:
	streamlit run dashboard/app.py

up:
	docker compose up -d postgres

down:
	docker compose down

clean:
	rm -rf data/lakehouse .pytest_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
