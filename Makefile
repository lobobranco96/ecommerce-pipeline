.PHONY: up down restart

# Inicia todos os serviços
up:
	docker compose -f services/datalake_dwh.yaml up -d
	docker compose -f services/orchestration.yaml up -d
	docker compose -f services/processing.yaml up -d

down:
	docker compose -f services/datalake_dwh.yaml down
	docker compose -f services/orchestration.yaml down
	docker compose -f services/processing.yaml down

# Reinicia o Airflow e os serviços de observabilidade
restart: down up docker compose -f services/observability.yaml up -d 	docker compose -f services/observability.yaml down
