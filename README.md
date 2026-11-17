# Pipeline Batch Reproducible: NYC Yellow Taxi (TLC)

### **Integrantes del grupo:**
- Brahim, Camila Ayelén - 56066
- Breslauer, León - 56340
### **Cátedra:** Fundamentos de Ingeniería de Datos — UTN FRT

---

Pipeline batch reproducible de ingeniería de datos diseñado para ingerir, validar contra contrato, filtrar mediante reglas de calidad con cuarentena, almacenar en formato columnar particionado (Hive) y modelar dimensionalmente los registros de viajes de taxis amarillos de Nueva York (NYC Taxi & Limousine Commission - TLC), comparando un mes objetivo contra su homólogo del año anterior y respondiendo preguntas de negocio sobre el modelo estrella.

---

## 1. Instalación y Ejecución

### Requisitos Previos
* **Python:** 3.10 o superior (probado en Python 3.12 sobre Linux Mint XFCE).
* **Memoria RAM:** 8.00 GB disponibles (el pipeline está optimizado para operar con un pico inferior a 2.00 GB).
* **Conectividad:** Acceso a internet para la descarga inicial desde los buckets públicos de CloudFront de la TLC.

### Configuración del Entorno Virtual
```bash
git clone <URL_DEL_REPOSITORIO>
cd nyc-taxi
python3 -m venv .venv
source .venv/bin/activate
make setup
```

### Ejecución Completa (Comando Único)
Para ejecutar de punta a punta las 5 etapas del pipeline (ingesta idempotente, validación de esquema, calidad con cuarentena, capa curada particionada, modelado en dbt, consultas de negocio y perfilado de recursos) para el mes asignado y su comparación interanual:
```bash
make all MES=2025-02
```
*(El parámetro `MES` acepta cualquier período válido en formato `AAAA-MM`, por ejemplo `MES=2025-03`)*.

### Comandos Auxiliares
* **Ejecutar suite de pruebas automatizadas (`pytest`):**
  ```bash
  make test
  ```
* **Ejecutar pruebas de integridad referencial de dbt (`dbt test`):**
  ```bash
  make dbt-test
  ```
* **Levantar la interfaz gráfica de Dagster para inspeccionar el linaje y métricas:**
  ```bash
  make ui
  ```
* **Limpiar artefactos locales generados (`data/`, caches y compilados de dbt):**
  ```bash
  make clean
  ```

---

## 2. Arquitectura del Sistema

El pipeline desacopla el procesamiento columnar de bajo nivel (DuckDB) de la orquestación declarativa (Dagster) y el modelado dimensional (dbt Core), garantizando una huella mínima de memoria y total trazabilidad:

```text
                         ORQUESTADOR: DAGSTER (Software-Defined Assets)
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │  raw_taxi_data ──► quality_validated_data ──► dimensional_star_schema ──► observability│
  │    (Etapa 1)          (Etapas 2, 3 y 4)              (Etapa 5: dbt)         (Métricas) │
  └───────────┬────────────────────┬────────────────────────────┬───────────────────┬──────┘
              │                    │                            │                   │
              ▼                    ▼                            ▼                   ▼
   ┌────────────────────┐ ┌────────────────────┐ ┌──────────────────────┐ ┌──────────────────┐
   │    1. INGESTA      │ │ 2. SCHEMA & QUALITY│ │  5. MODELADO (dbt)   │ │ 6. REPORTERÍA Y  │
   │  (Python/Requests) │ │ (DuckDB in-process)│ │     (dbt-duckdb)     │ │   OBSERVABILIDAD │
   └──────────┬─────────┘ └─────────┬──────────┘ └──────────┬───────────┘ └─────────┬────────┘
              │ Streaming 64KB      │ Invariante:           │ 3 Schemas en DuckDB:  │
              │ + Hash SHA-256      │ Leídas = Cur. + Cuar. │ • staging (Tables)    │
              ▼                     ▼                       │ • intermediate (Views)│
    data/raw/yellow/        ┌───────────────┴────────────┐  │ • marts (Tables)      │
    └── year=AAAA/month=MM/ │                            │  └──────────┬────────────┘
        └── *.parquet       ▼                            ▼             │
    data/raw/zones/   [RECHAZADAS]                 [ACEPTADAS]         ▼
    └── lookup.csv    data/quarantine/             data/curated/  RESULTADOS.md
                      └── year=AAAA/month=MM/      └── year=AAAA/ OBSERVABILIDAD.md
                          └── *.parquet                └── *.parquet
```

### Estructura del Repositorio
```text
nyc-taxi/
├── Makefile                        # Automatización de comandos (all, test, dbt-test, ui, clean)
├── README.md                       # Documentación técnica, decisiones de diseño y benchmarks
├── RESULTADOS.md                   # Respuestas SQL e interpretaciones a las 6 preguntas de negocio
├── OBSERVABILIDAD.md               # Perfil de consumo de RAM, CPU y throughput por etapa
├── requirements.txt                # Dependencias fijadas del proyecto
├── contracts/
│   └── yellow_schema.yaml          # Contrato de esquema destino versionado (mapeos y tipos)
├── src/
│   ├── __init__.py                 # API pública y metadatos del paquete
│   ├── config.py                   # Rutas, parámetros de memoria DuckDB y parseo de períodos
│   ├── step1_ingest.py             # Descarga streaming, hash SHA-256, backoff e ingestion_log
│   ├── step2_schema.py             # Validación contra contrato, snake_case y auditoría de drift
│   ├── step3_quality.py            # 9 reglas de calidad, cuarentena Parquet/SQL y reconciliación
│   ├── step4_transform.py          # Columnas derivadas, enriquecimiento y particionado Hive
│   ├── step5_model_and_queries.py  # Ejecución de dbt Core, consultas analíticas y RESULTADOS.md
│   ├── telemetry.py                # Monitor daemon con psutil (muestreo cada 250 ms)
│   └── dagster_orchestrator.py     # Definición de Software-Defined Assets y runner CLI
├── dbt-project/
│   ├── dbt_project.yml             # Configuración de materializaciones y esquemas por capa
│   ├── profiles.yml                # Perfil local DuckDB portable sin rutas absolutas
│   ├── macros/
│   │   └── generate_schema_name.sql# Macro para respetar nombres exactos de esquemas
│   └── models/
│       ├── sources.yml             # Declaración de fuentes externas (Parquet curado y CSV zonas)
│       ├── staging/                # Tablas limpias y tipadas (stg_tlc__yellow_trips, stg_tlc__taxi_zones)
│       ├── intermediate/           # Vistas de negocio (int_trips_enriched, int_zone_hourly_metrics)
│       └── marts/                  # Modelo estrella (dim_zone, dim_date, fact_trips, agg_zone_hour, mart_yoy)
├── sql/
│   └── queries/                    # Consultas SQL individuales para cada pregunta de negocio
└── tests/
    ├── conftest.py                 # Fixtures en memoria y datasets sintéticos
    ├── test_quality_rules.py       # Validación de reglas REJECT vs WARNING
    ├── test_idempotency.py         # Verificación de idempotencia en ingesta y logs
    ├── test_reconciliation.py      # Prueba de invariante matemática y aborto ante fugas
    └── test_schema_contract.py     # Validación de evolución de esquema y mapeo snake_case
```

---

## 3. Decisiones Técnicas y Justificaciones

### A. Elección del Stack y Cuidado de Memoria (< 8 GB RAM)
* **Motor Analítico (`DuckDB` + `PyArrow`):** En lugar de `pandas` (que expande los strings y timestamps en RAM entre 5x y 10x provocando errores *Out-Of-Memory* al cruzar más de 6 millones de filas) o `PySpark` (cuya JVM consume ~1.5 GB de overhead inicial), se eligió **DuckDB**. DuckDB opera mediante ejecución vectorizada por bloques y soporta *out-of-core spilling* a disco. Además, se configuró explícitamente `SET memory_limit = '4GB'` y `SET preserve_insertion_order = false` para reducir un 40% la memoria intermedia en ordenamientos y agregaciones.
* **Descarte de servicios en contenedores (`Airbyte`, `Prometheus`, `Grafana`):** Desplegar Airbyte localmente exige entre 2.5 GB y 4 GB de RAM en reposo, y un stack de Prometheus + Grafana suma otro ~1.2 GB. Sumados al sistema operativo (~2.2 GB), volverían inviable la ejecución dentro del límite de 8 GB. Por ello se implementó ingesta en streaming por chunks de 64 KB en Python y telemetría nativa embebida con `psutil` (< 2 MB de overhead).
* **Orquestación sin demonios (`Dagster execute_in_process`):** El pipeline se modela mediante *Software-Defined Assets* de Dagster, pero se ejecuta en proceso desde el `Makefile` sin levantar servidores web ni demonios en segundo plano, garantizando ejecución reproducible con un único comando y visualización opcional bajo demanda (`make ui`).

### B. Evolución de Esquema y Tratamiento de Nulos (`contracts/yellow_schema.yaml`)
* **Valor de `cbd_congestion_fee` en 2024 (`0.0` vs. `NULL`):** Se decidió asignar **`0.0` (`DOUBLE`)** a todos los registros de 2024. El cargo por congestión del Distrito Comercial Central (*Central Business District Congestion Toll*) entró en vigencia en Nueva York en **enero de 2025**. En 2024 el cargo no es un dato perdido (*missing data*), sino una tasa regulada que fácticamente valía `$0.00 USD`. Imputar `0.0` preserva la integridad aditiva en SQL evitando que sumas aritméticas de componentes tarifarios devuelvan `NULL`.
* **Política ante columnas nuevas no declaradas (`extra_columns_policy: warn_and_ignore`):** Si la TLC incorpora mañana una columna imprevista en sus archivos Parquet, el validador detecta la deriva (*schema drift*), la registra como advertencia en `main.schema_audit_log` y la excluye en la proyección `SELECT` hacia la capa curada. De este modo, el pipeline batch no se interrumpe ante cambios aguas arriba y ningún campo no gobernado contamina el modelo estrella.

### C. Reglas de Calidad, Severidades y Reconciliación
Se implementaron **9 reglas explícitas** (las 7 obligatorias de la consigna más 2 reglas propias descubiertas explorando los datos de la TLC). Las violaciones de integridad física, temporal, contable o referencial se clasifican como `REJECT` (enviando la fila completa a `data/quarantine/` junto con el arreglo de reglas violadas), mientras que las anomalías operativas de carga manual que no invalidan el hecho económico se clasifican como `WARNING` (conservando la fila en la capa curada con una etiqueta en `warning_flags`):

| ID | Regla de Calidad | Severidad | Umbral y Justificación |
| :--- | :--- | :--- | :--- |
| **R01** | `OUT_OF_MONTH_PICKUP` | **REJECT** | `YEAR(pickup) != año_archivo OR MONTH(pickup) != mes_archivo`. Filtra registros rezagados o errores de reloj del taxímetro que contaminarían particiones Hive de otros períodos. |
| **R02** | `DROPOFF_BEFORE_OR_EQUAL_PICKUP` | **REJECT** | `dropoff <= pickup` (duración $\le 0\text{ s}$). Imposibilidad física temporal; evita duraciones negativas y divisiones por cero en el cálculo de velocidad. |
| **R03** | `INVALID_TRIP_DISTANCE` | **REJECT** | `trip_distance <= 0 OR trip_distance > 200.0` millas. Distancias $\le 0$ son viajes cancelados; el máximo de 200 millas cubre traslados interurbanos largos filtrando fallas de odómetro (ej. > 10,000 mi). |
| **R04** | `NEGATIVE_TOTAL_OR_FARE` | **REJECT** | `total_amount < 0 OR fare_amount < 0`. Excluye asientos contables de contracargos o disputas que subestimarían la tarifa media y duplicarían el conteo físico de viajes. |
| **R05** | `NULL_OR_ZERO_PASSENGERS` | **WARNING** | `passenger_count IS NULL OR passenger_count <= 0`. Alrededor del 10% de los viajes reales (especialmente despachos por app/e-hail) carecen de carga manual de pasajeros. Descartarlos sesgaría los ingresos y el análisis de congestión. |
| **R06** | `UNKNOWN_TAXI_ZONE` | **REJECT** | `pu_location_id` o `do_location_id` inexistentes en `taxi_zone_lookup.csv` o correspondientes a zonas desconocidas (`264`, `265`). Garantiza integridad referencial estricta con `dim_zone`. |
| **R07** | `EXACT_DUPLICATE_ROW` | **REJECT** | `ROW_NUMBER() > 1` sobre todas las columnas del evento. Conserva la primera instancia y envía las retransmisiones duplicadas del taxímetro a cuarentena. |
| **R08** | `IMPOSSIBLE_DURATION_OR_SPEED` *(Propia 1)* | **REJECT** | `duracion > 720 min` (12 h) o `velocidad_media > 100 mph`. Descarta viajes donde el conductor olvidó cerrar el taxímetro por días o saltos de GPS con velocidades físicamente imposibles. |
| **R09** | `EXCESSIVE_PASSENGER_COUNT` *(Propia 2)* | **WARNING** | `passenger_count > 6`. Excede la capacidad máxima legal de taxis amarillos regulados por la TLC; se marca como error de tipeo sin descartar el viaje. |

**Invariante de Reconciliación:** En cada ejecución se verifica matemáticamente que:
$$\text{filas\_leídas} = \text{filas\_aceptadas} + \text{filas\_cuarentena}$$
Si existe una diferencia de siquiera 1 registro, el pipeline lanza un `RuntimeError` y detiene la ejecución.

---

## 4. Modelado Dimensional y Granularidad (dbt Core)

El almacén analítico (`data/warehouse/nyc_taxi.duckdb`) está organizado en esquemas separados por responsabilidad mediante **dbt Core**:
* `main`: Tablas de control y auditoría (`ingestion_log`, `schema_audit_log`, `quality_summary_report`, `quality_rule_counts`, `quarantine_trips`, `pipeline_metrics_log`).
* `staging` *(Tablas)*: `stg_tlc__yellow_trips` y `stg_tlc__taxi_zones`. Castea tipos de datos, genera claves determinísticas `MD5` y elimina atributos redundantes de zona para respetar la 3FN antes del modelado dimensional.
* `intermediate` *(Vistas)*: `int_trips_enriched` e `int_zone_hourly_metrics`. Encapsula reglas reutilizables (*Role-Playing Joins* de zonas, decodificación de `payment_type` y clasificación de aeropuertos).
* `marts` *(Tablas)*: Modelo dimensional estrella final:
  * **`marts.fact_trips`:** **Granularidad:** 1 fila por cada viaje individual de taxi amarillo validado por la compuerta de calidad.
  * **`marts.dim_zone`:** **Granularidad:** 1 fila por cada zona oficial de la TLC (265 zonas). Actúa como *Role-Playing Dimension* para origen (`pu_location_id`) y destino (`do_location_id`).
  * **`marts.dim_date`:** **Granularidad:** 1 fila por cada día calendario único presente en los períodos analizados.
  * **`marts.agg_zone_hour`:** **Granularidad:** 1 fila por cada combinación única de `(pu_location_id, pickup_date, pickup_hour)`.
  * **`marts.mart_yoy_zone_performance`:** **Granularidad:** 1 fila por zona de origen comparando métricas agregadas interanuales.

---

## 5. Idempotencia y Reprocesamiento de Particiones

La capa curada se almacena en formato Parquet particionado estilo Hive:
```text
data/curated/yellow/
├── year=2024/month=02/curated_2024-02.parquet
└── year=2025/month=02/curated_2025-02.parquet
```
Al reprocesar un período, `step4_transform.py` construye el archivo en un directorio oculto temporal (`.tmp_year=AAAA_month=MM`) y reemplaza atómicamente (`shutil.move`) únicamente la carpeta de esa partición, dejando intactos los demás meses.

**Evidencia de ejecución repetida (`MES=2025-02`):**
* **Conteo de filas en `year=2024/month=02` antes del reproceso:** `2,869,925` filas.
* **Conteo de filas en `year=2024/month=02` después del reproceso:** `2,869,925` filas (archivo y timestamp inalterados).
* **Registros en `main.ingestion_log` tras correr dos veces:** 1 único registro por archivo con estado `ok` (sin descargas redundantes ni duplicación de filas).

---

## 6. Rendimiento y Uso de Recursos

El módulo `src/telemetry.py` muestrea cada 250 ms el consumo de memoria física (RSS) y tiempo de CPU durante toda la ejecución (detallado en `OBSERVABILIDAD.md`):

* **Hardware de prueba:** Notebook con 8.00 GB de RAM bajo Linux Mint XFCE (x86_64).
* **Tiempo total de ejecución (2 meses completos + dbt + consultas):** `59.20 segundos`.
* **Pico máximo global de memoria RAM (RSS):** `1,929.50 MB` (**23.55%** del límite de 8.00 GB).
* **Spilling a disco requerido:** `0.00 MB` (todo el procesamiento vectorizado cabió holgadamente dentro del presupuesto de RAM).

---

## 7. Limitaciones Conocidas

1. **Censura en propinas en efectivo (`payment_type = 2`):** Los datos publicados por la TLC no registran propinas entregadas en efectivo al conductor, reportando `$0.00` sistemáticamente para ese medio de pago.
2. **Concurrencia de escritura única en DuckDB:** Mientras se ejecuta `make all` o `make dbt-test`, no debe mantenerse abierta una conexión de escritura sobre `data/warehouse/nyc_taxi.duckdb` desde clientes externos (como DBeaver), o debe configurarse en modo *Read-Only*.
3. **Procesamiento batch local:** El pipeline está diseñado para ejecución local en un solo nodo; para escalar a múltiples años completos simultáneos se requeriría migrar el almacenamiento a un Object Storage (S3/MinIO) y ejecutar dbt sobre un motor distribuido.

---

## 8. Declaración de Uso de Inteligencia Artificial

En cumplimiento con las normas de la cátedra, se declara que se utilizó **Gemini** como herramienta de asistencia durante el desarrollo del trabajo para:
1. El diseño estructural inicial de los módulos Python, el contrato YAML y la integración de Dagster con dbt Core.
2. La redacción de casos de prueba unitarios con datos sintéticos en `pytest` y la optimización de consultas SQL analíticas.

Toda la arquitectura, umbrales de calidad, reglas de negocio, pruebas de ejecución y consultas dimensionales fueron auditados, probados y comprendidos integralmente por los autores para su defensa oral.
