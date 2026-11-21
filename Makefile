MES ?= 2025-03
PYTHON ?= python3

.PHONY: all up down logs test dbt-test trigger clean

# Levanta toda la plataforma persistente (Dagster + Dozzle + Prometheus + Grafana)
up:
	mkdir -p data/dagster_home data/triggers
	touch RESULTADOS.md
	docker compose up -d --build
	@echo "Esperando a que Dagster Webserver esté listo en http://localhost:3000 ..."
	@until curl -sf http://localhost:3000/server_info > /dev/null; do sleep 2; done
	@echo "Plataforma activa:"
	@echo "  - Dagster UI : http://localhost:3000"
	@echo "  - Dozzle Logs: http://localhost:8080"
	@echo "  - Grafana    : http://localhost:3001"
	@echo "  - Prometheus : http://localhost:9090"

# Comando único exigido por la consigna: asegura servicios vivos y dispara la corrida
all: up
	docker compose exec -T dagster bash -c '\
		echo '\''{"ops":{"raw_taxi_data":{"config":{"mes":"$(MES)"}},"quality_validated_data":{"config":{"mes":"$(MES)"}},"dimensional_star_schema":{"config":{"mes":"$(MES)"}},"business_results_markdown":{"config":{"mes":"$(MES)"}}}}'\'' > /tmp/run_config.json && \
		dagster job execute -f src/dagster_orchestrator.py -j nyc_taxi_pipeline_job -c /tmp/run_config.json'
	@echo "Corrida completada. Los servicios permanecen vivos para inspección."

# Dispara una corrida reactiva mediante el Sensor de Dagster (sin bloquear terminal)
trigger:
	mkdir -p data/triggers
	touch data/triggers/$(MES).trigger
	@echo "Archivo disparador data/triggers/$(MES).trigger creado. El sensor de Dagster lo procesará en <=15s."

test:
	docker compose exec -T dagster pytest tests/ -v

dbt-test:
	docker compose exec -T dagster bash -c "cd dbt-project && dbt test --profiles-dir ."

logs:
	docker compose logs -f dagster

down:
	docker compose down

clean:
	docker compose down -v
	rm -rf data/ dbt-project/target/ .pytest_cache/
