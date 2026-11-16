MES ?= 2025-03
PYTHON ?= python3

.PHONY: all setup ingest quality dbt test clean

setup:
	$(PYTHON) -m pip install -r requirements.txt

dbt-run:
	cd dbt-project && dbt run --profiles-dir .

dbt-test:
	cd dbt-project && dbt test --profiles-dir .

all:
	$(PYTHON) -m src.dagster_orchestrator --mes $(MES)

ui:
	dagster dev -f src/dagster_orchestrator.py

test:
	$(PYTHON) -m pytest tests/ -v

clean:
	rm -rf data/ dbt-project/target/ dbt-project/dbt_packages/ .pytest_cache/
