# Makefile para gerenciamento de containers com Docker Compose

.PHONY: up stop restart


# Inicia todos os serviços
up:
	docker compose -f services/observability.yaml up
	docker compose -f services/datalake_dwh.yaml up
	docker compose -f services/orchestration.yaml up

down:
	docker compose -f services/observability.yaml down
	docker compose -f services/datalake_dwh.yaml down
  docker compose -f services/orchestration.yaml down

# Reinicia o Airflow e os serviços de observabilidade
restart: stop up