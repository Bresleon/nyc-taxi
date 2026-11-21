# Pipeline Batch Reproducible: NYC Yellow Taxi (TLC)

### **Integrantes del grupo:**
- Brahim, Camila Ayelén - 56066
- Breslauer, León - 56340
### **Cátedra:** Fundamentos de Ingeniería de Datos — UTN FRT

---

Plataforma de ingeniería de datos *end-to-end* contenedorizada para ingerir de forma transaccional, validar contra contrato, filtrar mediante reglas de calidad con cuarentena, almacenar en formato columnar particionado (Hive) y modelar dimensionalmente los registros de viajes de taxis amarillos de Nueva York (NYC Taxi & Limousine Commission - TLC). El sistema compara un mes objetivo contra su homólogo del año anterior (YoY), responde preguntas de negocio sobre el modelo estrella y expone métricas de infraestructura en tiempo real bajo un presupuesto estricto de **8.00 GB de RAM**.

---

## 1. Instalación, Despliegue y Ejecución

### Requisitos Previos
* **Sistema Operativo:** Linux (probado sobre Linux Mint XFCE con *cgroups v2*).
* **Contenedores:** Docker Engine y Docker Compose v2 (`docker compose version`).
* **Automatización:** GNU Make (`make`).
* **Memoria RAM:** 8.00 GB disponibles (el stack completo de contenedores + ejecución pico opera por debajo de los 4.20 GB).
* **Conectividad:** Acceso a internet para la descarga inicial desde los buckets públicos de CloudFront de la TLC.

### Clonado e Inicio Rápido (Comando Único)
```bash
git clone <URL_DEL_REPOSITORIO>
cd nyc-taxi
make all MES=2025-03
```
*(El parámetro `MES` acepta cualquier período válido en formato `AAAA-MM`, por ejemplo `MES=2025-02` o `MES=2025-03`)*.

Al ejecutar `make all`, Docker Compose construye e inicia los **5 servicios persistentes** en segundo plano, espera a que el servidor web de Dagster esté saludable y dispara el grafo completo de assets dentro del contenedor vivo. **Al finalizar la corrida, los contenedores permanecen activos** para inspeccionar el linaje, los logs en vivo, las métricas históricas y la base de datos.

### Ejecución Reactiva mediante Sensor de Dagster (`make trigger`)
Para disparar una corrida orientada a eventos sin bloquear la terminal:
1. Abre **Dagster UI** (`http://localhost:3000`) $\rightarrow$ **Deployment** $\rightarrow$ **Sensors** y activa el interruptor de **`new_month_trigger_sensor`**.
2. Crea el archivo disparador desde la consola:
   ```bash
   make trigger MES=2025-02
   ```
3. En $\le 15$ segundos, el demonio de Dagster detectará `data/triggers/2025-02.trigger`, lo consumirá y ejecutará todo el pipeline de forma autónoma.

### Comandos Auxiliares del `Makefile`
* **Levantar la plataforma persistente sin disparar una corrida:**
  ```bash
  make up
  ```
* **Ejecutar suite de pruebas automatizadas (`pytest`):**
  ```bash
  make test
  ```
* **Ejecutar pruebas de integridad referencial de dbt (`dbt test`):**
  ```bash
  make dbt-test
  ```
* **Seguir los logs del contenedor principal de Dagster en consola:**
  ```bash
  make logs
  ```
* **Detener todos los servicios de Docker Compose (conservando `data/`):**
  ```bash
  make down
  ```
* **Limpiar contenedores, volúmenes y artefactos locales generados (`data/`, `dbt-project/target/`):**
  ```bash
  make clean
  ```

### Interfaces Web y Puertos Expuestos

| Servicio | URL Local | Descripción |
| :--- | :--- | :--- |
| **Dagster UI** | `http://localhost:3000` | Grafo de *Software-Defined Assets*, historial de corridas, metadatos por etapa, *Schedules* y *Sensors*. |
| **Grafana** | `http://localhost:3001` | Dashboard **NYC Taxi Pipeline - Monitoreo de Recursos (8GB Limit)** (acceso anónimo habilitado). |
| **Dozzle** | `http://localhost:8080` | Streaming de logs en tiempo real de todos los contenedores y barras instantáneas de CPU/RAM. |
| **Prometheus** | `http://localhost:9090` | Servidor de series temporales y explorador PromQL (*scrape* cada 5 segundos). |

---

## 2. Arquitectura del Sistema

La plataforma desacopla la ingesta ELT declarativa (`dlt`), el procesamiento columnar *out-of-core* (`DuckDB` + `PyArrow`), el modelado dimensional (`dbt Core`), la orquestación persistente (`Dagster`) y la observabilidad externa (`Dozzle` + `Prometheus` + `Grafana`):

```text
+-----------------------------------------------------------------------------------+
|                        DOCKER COMPOSE PLATFORM (< 4.2 GB RAM)                     |
|                                                                                   |
|  +-----------------------------------------------------------------------------+  |
|  |                     CONTENEDOR: nyc_taxi_dagster (:3000)                    |  |
|  |  Dagster Webserver + Daemon (Monthly Schedule & File Trigger Sensor)        |  |
|  |                                                                             |  |
|  |  [1. Ingesta ELT]        [2. Calidad & Curación]      [3. Modelado Star]    |  |
|  |  dlt (data load tool) -> Contrato YAML + 9 Reglas  -> dbt Core (3 capas)    |  |
|  |  - SHA-256 Idempotente   - Cuarentena Parquet         - staging (tables)    |  |
|  |  - PyArrow Batches       - Curated Hive Parquet       - intermediate (views)|  |
|  |  - _dlt_loads / version  - Reconciliación Contable    - marts (Star Schema) |  |
|  +------------------------------------+----------------------------------------+  |
|                                       |                                           |
|                                       v                                           |
|                        +-----------------------------+                            |
|                        | DuckDB Analytical Warehouse |                            |
|                        | data/warehouse/nyc_taxi.db  |                            |
|                        +-----------------------------+                            |
|                                                                                   |
|  +-----------------------------------------------------------------------------+  |
|  |                        STACK DE OBSERVABILIDAD EN VIVO                      |  |
|  |                                                                             |  |
|  |  Dozzle (:8080)  ---> Streaming de logs y consumo instantáneo por servicio  |  |
|  |  Docker Exporter ---> Métricas cgroups v2 (/var/run/docker.sock) [~18 MB]   |  |
|  |  Prometheus (:9090) > Series temporales (scrape cada 5s, retención 2d)      |  |
|  |  Grafana (:3001) ---> Dashboards auto-provisionados de RAM y CPU (< 8 GB)   |  |
|  +-----------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------+
```

### Estructura del Repositorio
```text
nyc-taxi/
├── Dockerfile                        # Imagen Python 3.12 con Dagster, dlt, dbt-duckdb y PyArrow
├── docker-compose.yml                # Orquestación de 5 servicios con topes explícitos de RAM
├── Makefile                          # Automatización unificada (all, up, trigger, test, dbt-test, down)
├── README.md                         # Documentación técnica, decisiones de diseño y guía de operación
├── RESULTADOS.md                     # Respuestas SQL e interpretaciones a las 6 preguntas de negocio
├── requirements.txt                  # Dependencias fijadas del proyecto
├── dagster_home/
│   └── dagster.yaml                  # Storage SQLite y cola de concurrencia única (max_concurrent_runs: 1)
├── contracts/
│   └── yellow_schema.yaml            # Contrato de esquema destino versionado (mapeos y tipos)
├── monitoring/
│   ├── docker_exporter.py            # Exportador liviano (~18 MB RAM) compatible con cgroups v2
│   ├── prometheus.yml                # Configuración de scraping cada 5s hacia el exportador
│   └── grafana/provisioning/         # Datasource Prometheus y dashboard JSON auto-provisionados
├── src/
│   ├── __init__.py                   # API pública y metadatos del paquete
│   ├── config.py                     # Rutas, parámetros de memoria DuckDB y parseo de períodos
│   ├── step1_ingest.py               # Pipeline declarativo dlt con PyArrow batches, SHA-256 y _dlt_loads
│   ├── step2_schema.py               # Validación contra contrato, snake_case y auditoría de drift
│   ├── step3_quality.py              # 9 reglas de calidad, cuarentena Parquet/SQL y reconciliación
│   ├── step4_transform.py            # Columnas derivadas, enriquecimiento y particionado Hive
│   ├── step5_model_and_queries.py    # Ejecución de dbt Core, consultas analíticas y RESULTADOS.md
│   └── dagster_orchestrator.py       # Assets, Job, Schedule mensual y Sensor de archivos
├── dbt-project/
│   ├── dbt_project.yml               # Configuración de materializaciones y esquemas por capa
│   ├── profiles.yml                  # Perfil local DuckDB portable sin rutas absolutas
│   ├── macros/
│   │   └── generate_schema_name.sql  # Macro para respetar nombres exactos de esquemas
│   └── models/
│       ├── sources.yml               # Declaración de fuentes externas (Parquet curado y CSV zonas)
│       ├── staging/                  # Tablas limpias y tipadas (stg_tlc__yellow_trips, stg_tlc__taxi_zones)
│       ├── intermediate/             # Vistas de negocio (int_trips_enriched, int_zone_hourly_metrics)
│       └── marts/                    # Modelo estrella (dim_zone, dim_date, fact_trips, agg_zone_hour, mart_yoy)
├── sql/
│   └── queries/                      # Consultas SQL individuales para cada pregunta de negocio
└── tests/
    ├── conftest.py                   # Fixtures en memoria y datasets sintéticos
    ├── test_quality_rules.py         # Validación de reglas REJECT vs WARNING
    ├── test_idempotency.py           # Pruebas de idempotencia dlt, corrupción SHA-256 y fallos de red
    ├── test_reconciliation.py        # Prueba de invariante matemática y aborto ante fugas
    └── test_schema_contract.py       # Validación de evolución de esquema y mapeo snake_case
```

---

## 3. Decisiones Técnicas y Justificaciones

### A. Elección del Stack y Cuidado de Memoria (< 8 GB RAM)
* **Ingesta ELT con `dlt` (data load tool) vs. Airbyte / scripts manuales:** Desplegar Airbyte localmente consume entre 2.5 GB y 4 GB de RAM en reposo, volviéndolo inviables en un host de 8 GB. Por otro lado, los scripts manuales carecen de trazabilidad transaccional y versionado de esquema. Se adoptó **`dlt`**, una librería ELT nativa en Python que corre dentro del contenedor de Dagster (~350 MB en carga) utilizando `@dlt.transformer` sobre bloques columnares **`PyArrow RecordBatches`** de 100,000 filas (evitando materializar diccionarios en Python). `dlt` genera automáticamente en DuckDB las tablas de auditoría `raw_dlt._dlt_loads` (estado y timestamps de cada paquete de carga) y `raw_dlt._dlt_version` (evolución de esquema), manteniendo además la descarga cruda intacta en `data/raw/` con verificación por hash **SHA-256**.
* **Motor Analítico (`DuckDB` + `PyArrow`):** En lugar de `pandas` (que expande strings y timestamps en RAM entre 5x y 10x provocando errores *Out-Of-Memory* al cruzar más de 6 millones de filas) o `PySpark` (cuya JVM consume ~1.5 GB de overhead inicial), se eligió **DuckDB**. Opera mediante ejecución vectorizada por bloques y soporta *out-of-core spilling* a disco, configurado explícitamente con `SET memory_limit = '4GB'` y `SET preserve_insertion_order = false`.
* **Orquestación Viva (`Dagster Webserver + Daemon`):** El servicio `nyc_taxi_dagster` se ejecuta de forma continua mediante `dagster dev`, habilitando:
  1. **Automatización temporal (`monthly_tlc_schedule`):** Ejecuta el pipeline el día 15 de cada mes calculando el último período publicado por la TLC (rezago de 2 meses).
  2. **Automatización reactiva (`new_month_trigger_sensor`):** Monitorea `data/triggers/*.trigger` cada 15 segundos para disparar corridas ante nuevos eventos.
  3. **Control de concurrencia (`max_concurrent_runs: 1` con almacenamiento SQLite):** Evita levantar un contenedor extra de PostgreSQL (~300 MB RAM) y garantiza que dos corridas nunca intenten abrir `nyc_taxi.duckdb` en escritura simultáneamente.
* **Observabilidad Externa Liviana (`Dozzle` + `Docker Exporter` + `Prometheus` + `Grafana`):** En reemplazo de métricas internas estáticas (`psutil`), se implementó un stack externo gobernado por *Infrastructure as Code* con topes de memoria (`mem_limit`) y retención acotada (`--storage.tsdb.retention.time=2d`):
  * **`Dozzle` (`64 MB` límite):** Streaming de logs en vivo y medidores instantáneos de CPU/RAM.
  * **`Docker Exporter` en Python (`64 MB` límite, ~18 MB reales):** Reemplaza al binario de `cAdvisor` para garantizar compatibilidad total con *cgroups v2* en Linux Mint leyendo directamente `/var/run/docker.sock`.
  * **`Prometheus` (`256 MB` límite) + `Grafana` (`256 MB` límite):** Recolectan series cada 5 segundos y auto-provisionan el dashboard con techo visual en `8,192 MB`.

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

El almacén analítico (`data/warehouse/nyc_taxi.duckdb`) está organizado en **5 esquemas** separados por responsabilidad mediante `dlt` y **dbt Core**:
* `raw_dlt`: Tablas crudas ingestadas por `dlt` (`yellow_trips_raw`, `taxi_zones`, `ingestion_log`) junto con sus tablas nativas de control y estado (`_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state`).
* `main`: Vista `main.ingestion_log` (vinculada a `_dlt_load_id`) y tablas de control y auditoría (`schema_audit_log`, `quality_summary_report`, `quality_rule_counts`, `quarantine_trips`).
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
* **Conteo de filas en `year=2024/month=02` antes y después del reproceso:** `2,873,690` filas (partición reemplazada atómicamente sin duplicados).
* **Conteo de filas en `year=2025/month=02` antes y después del reproceso:** `3,282,734` filas (`6,156,424` viajes totales en `marts.fact_trips`).
* **Registros en `main.ingestion_log` tras correr dos veces:** 1 único registro por archivo (`3` filas en total: zonas + 2 meses) con estado `ok` y 0 descargas de red redundantes gracias a la verificación previa de SHA-256 en `dlt`.

---

## 6. Rendimiento y Presupuesto de Recursos (8 GB RAM)

Las métricas recolectadas en tiempo real por `Prometheus` + `Grafana` y `Dozzle` confirman que toda la plataforma opera con amplio margen dentro de una notebook de **8.00 GB de RAM**:

* **Hardware de prueba:** Notebook con 8.00 GB de RAM bajo Linux Mint XFCE (x86_64).
* **Tiempo total de ejecución E2E (2 meses completos + dbt run/test + consultas):** `~60–80 segundos`.
* **Consumo en reposo (plataforma viva con los 5 contenedores):** `~420 MB` totales (`dagster` ~230 MB, `grafana` ~95 MB, `prometheus` ~55 MB, `dozzle` ~22 MB, `docker_exporter` ~18 MB).
* **Pico máximo del contenedor `nyc_taxi_dagster`:** `~1,930 MB` (**23.55%** de los 8.00 GB físicos disponibles).
* **Consumo total del sistema en pico máximo (SO + 5 contenedores):** `~4.15 GB / 8.00 GB` (< 52% de utilización global).

---

## 7. Limitaciones Conocidas

1. **Censura en propinas en efectivo (`payment_type = 2`):** Los datos publicados por la TLC no registran propinas entregadas en efectivo al conductor, reportando `$0.00` sistemáticamente para ese medio de pago.
2. **Concurrencia de escritura única en DuckDB:** DuckDB permite múltiples lectores simultáneos o un único escritor exclusivo. Si se inspecciona `data/warehouse/nyc_taxi.duckdb` desde un cliente externo (como **DBeaver**), debe configurarse la propiedad de driver `duckdb.read_only = true` para evitar conflictos de bloqueo mientras Dagster ejecuta el pipeline.
3. **Procesamiento batch de nodo único:** El pipeline está diseñado para ejecución local contenedorizada en un solo nodo; para escalar a múltiples años completos simultáneos se requeriría migrar el almacenamiento a un Object Storage (S3/MinIO) y ejecutar dbt sobre un motor distribuido.

---

## 8. Declaración de Uso de Inteligencia Artificial

En cumplimiento con las normas de la cátedra, se declara que se utilizó **Gemini** como herramienta de asistencia durante el desarrollo del trabajo para:
1. El diseño estructural inicial de los módulos Python, el contrato YAML y la integración declarativa de `dlt`, Dagster (`Assets`, `Schedules` y `Sensors`) y `dbt Core`.
2. La configuración de contenedores con Docker Compose, el exportador de métricas compatible con *cgroups v2* y el aprovisionamiento automático de Prometheus y Grafana.
3. La redacción de casos de prueba unitarios con datos sintéticos en `pytest` y la optimización de consultas SQL analíticas.

Toda la arquitectura, umbrales de calidad, reglas de negocio, pruebas de ejecución y consultas dimensionales fueron auditados, probados y comprendidos integralmente por los autores para su defensa oral.