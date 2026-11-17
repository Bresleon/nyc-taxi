# Reporte de Observabilidad y Uso de Recursos (`MES=2025-02`)

- **Run ID (Dagster):** `bc05012e`
- **Presupuesto de Hardware Evaluado:** `8,192.00 MB` (8.00 GB RAM)
- **Pico Máximo Global de RAM (RSS):** `1929.50 MB` (`23.55%` del límite de 8 GB)
- **Tiempo Total Medido por Etapas:** `59.20 s`

## Desglose por Etapa del Pipeline

| Etapa | Duración (s) | RAM Inicio (MB) | RAM Pico (MB) | % de 8 GB | CPU User (s) | Spill Disco (MB) | Filas | Throughput (filas/s) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1_ingestion_raw` | 11.88 | 151.36 | **1929.50** | 23.55% | 18.00 | 0.00 | 20,752,804 | 1,746,556 filas/s |
| `2_schema_quality_and_curated` | 24.05 | 208.04 | **1776.33** | 21.68% | 135.08 | 0.00 | 6,585,069 | 273,756 filas/s |
| `3_dbt_staging_and_marts` | 23.26 | 211.39 | **211.39** | 2.58% | 1.19 | 0.00 | 6,156,424 | 264,655 filas/s |
