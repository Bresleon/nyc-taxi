FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /opt/dagster/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dagster_home/dagster.yaml ${DAGSTER_HOME}/dagster.yaml
COPY contracts/ ./contracts/
COPY dbt-project/ ./dbt-project/
COPY src/ ./src/
COPY tests/ ./tests/

EXPOSE 3000

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app && chmod -R 777 /opt/dagster

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-f", "src/dagster_orchestrator.py"]
