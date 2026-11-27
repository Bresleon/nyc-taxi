MES ?= 2025-03
PYTHON ?= python3

.PHONY: init-dirs all up down logs test dbt-test trigger clean

# Crea toda la estructura de volúmenes en el host con el usuario actual (evita que Docker los cree como root)
init-dirs:
	mkdir -p data/raw data/curated data/quarantine data/warehouse data/dagster_home data/triggers data/metabase
	touch RESULTADOS.md

# Levanta toda la plataforma persistente (Dagster + Dozzle + Prometheus + Grafana + Metabase)
up: init-dirs
	docker compose up -d --build
	@echo "Esperando a que Dagster Webserver esté listo en http://localhost:3000 ..."
	@until curl -sf http://localhost:3000/server_info > /dev/null; do sleep 2; done
	@echo "Plataforma activa:"
	@echo "  - Dagster UI : http://localhost:3000"
	@echo "  - Dozzle Logs: http://localhost:8080"
	@echo "  - Grafana    : http://localhost:3001"
	@echo "  - Prometheus : http://localhost:9090"
	@echo "  - Metabase   : http://localhost:3002"

# Comando único: asegura carpetas con permisos de usuario, servicios vivos y dispara la corrida
all: init-dirs
	docker compose up -d --build
	docker compose stop metabase
	trap 'docker compose start metabase' EXIT; \
	docker compose exec -T dagster bash -c '\
		echo '\''{"ops":{"raw_taxi_data":{"config":{"mes":"$(MES)"}},"quality_validated_data":{"config":{"mes":"$(MES)"}},"dimensional_star_schema":{"config":{"mes":"$(MES)"}},"business_results_markdown":{"config":{"mes":"$(MES)"}}}}'\'' > /tmp/run_config.json && \
		dagster job execute -f src/dagster_orchestrator.py -j nyc_taxi_pipeline_job -c /tmp/run_config.json'
	@echo "Corrida completada. Los servicios permanecen vivos para inspección."

# Dispara una corrida reactiva mediante el Sensor de Dagster (sin bloquear terminal)
trigger: init-dirs
	docker compose stop metabase
	touch data/triggers/$(MES).trigger
	@echo "Disparador $(MES).trigger creado. Cuando el run termine en Dagster UI, reactiva BI con: docker compose start metabase"

test:
	docker compose exec -T dagster python3 -m pytest tests/ -v

dbt-test:
	docker compose exec -T dagster bash -c "cd dbt-project && dbt test --profiles-dir ."

logs:
	docker compose logs -f dagster

down:
	docker compose down

clean:
	docker compose down -v
	rm -rf data/ dbt-project/target/ .pytest_cache/ 2>/dev/null || sudo rm -rf data/ dbt-project/target/ .pytest_cache/
