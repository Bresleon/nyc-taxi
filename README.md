# Pipeline Batch Reproducible: NYC Yellow Taxi (TLC)

### **Integrantes del grupo:**
- Brahim, Camila Ayelén - 56066
- Breslauer, León - 56340
### **Cátedra:** Fundamentos de Ingeniería de Datos — UTN FRT

---

Plataforma integral y reproducible de ingeniería de datos diseñada para ingerir mediante **dlt**, validar contra contrato, filtrar mediante reglas de calidad con cuarentena, almacenar en formato columnar particionado (Hive), modelar dimensionalmente con **dbt Core** sobre **DuckDB** y explotar visualmente mediante **Metabase** los registros de viajes de taxis amarillos de Nueva York (NYC Taxi & Limousine Commission - TLC). Todo el ecosistema corre contenedorizado con **Docker Compose** bajo un presupuesto estricto de **8.00 GB de RAM** y cuenta con observabilidad en tiempo real mediante **Prometheus**, **Grafana** y **Dozzle**.

---

## 1. Interfaces Web y Puertos del Ecosistema

Al levantar la plataforma con Docker Compose, quedan disponibles cinco interfaces web locales:

| Servicio | URL Local | Puerto | Función Principal |
| :--- | :--- | :---: | :--- |
| **Dagster UI** | `http://localhost:3000` | `3000` | Orquestación de *Software-Defined Assets*, linaje, *Schedules* y *Sensors*. |
| **Grafana** | `http://localhost:3001` | `3001` | Monitoreo de consumo de RAM y CPU por contenedor frente al límite de 8 GB. |
| **Metabase (BI)** | `http://localhost:3002` | `3002` | Exploración visual *self-service* y tableros sobre `marts` en modo **solo lectura**. |
| **Dozzle** | `http://localhost:8080` | `8080` | Streaming de logs en vivo y métricas instantáneas de todos los contenedores. |
| **Prometheus** | `http://localhost:9090` | `9090` | Base de datos de series temporales (*scraping* cada 5s de métricas Docker). |

---

## 2. Instalación y Ejecución

### Requisitos Previos
* **Sistema Operativo:** Linux con soporte Docker Engine / Docker Compose v2 (probado en Linux Mint XFCE con `cgroups v2`).
* **Memoria RAM:** 8.00 GB físicos disponibles (el pipeline y los servicios operan holgadamente dentro de este presupuesto).
* **Conectividad:** Acceso a internet para la descarga inicial desde los buckets públicos de CloudFront de la TLC.

### Despliegue Completo Automatizado (Zero-Setup IaC)
Todo el stack —incluyendo permisos de volúmenes, creación del usuario administrador en Metabase y registro de DuckDB en modo **solo lectura**— se aprovisiona automáticamente al levantar los contenedores:

```bash
git clone <URL_DEL_REPOSITORIO>
cd nyc-taxi
docker compose up -d --build
```

*(En el primer arranque, el contenedor efímero `nyc_taxi_metabase_setup` aguarda a que Metabase complete sus migraciones internas y configura automáticamente la instancia sin intervención manual).*

### Modalidades de Ejecución del Pipeline

#### A. Ejecución Directa por Período (`make all`)
Ejecuta de punta a punta la ingesta declarativa con `dlt`, validación de esquema, compuerta de calidad con cuarentena, capa curada Hive, modelado dimensional en `dbt` y generación de `RESULTADOS.md` para el mes objetivo y su homólogo del año anterior:
```bash
make all MES=2025-02
```
*(Acepta cualquier período válido en formato `AAAA-MM`, por ejemplo `MES=2025-03`)*.

#### B. Ejecución Reactiva Orientada a Eventos (`make trigger`)
Activa el sensor `new_month_trigger_sensor` en **Dagster UI** (`http://localhost:3000` $\rightarrow$ *Deployment* $\rightarrow$ *Sensors*) y deposita un archivo disparador en `data/triggers/`:
```bash
make trigger MES=2025-02
```
El demonio de Dagster detecta el archivo en menos de 15 segundos, lo consume de forma atómica y encola la corrida respetando el límite de concurrencia (`max_concurrent_runs: 1`).

### Acceso a Business Intelligence (Metabase)
Una vez ejecutado el pipeline al menos una vez, ingresa a **`http://localhost:3002`** con las credenciales aprovisionadas automáticamente por `monitoring/metabase/setup_metabase.py`:

* **Email:** `admin@nyc-taxi.local`
* **Contraseña:** `NycTaxi2026!`
* **Base conectada:** `NYC Taxi Data Warehouse` (`/data/warehouse/nyc_taxi.duckdb`, modo **Read-only**).

*(Nota: Si levantaste `docker compose up -d` antes de correr `make all` por primera vez, el archivo `.duckdb` aún no existía; tras ejecutar `make all`, corre `docker compose up -d metabase-setup` para registrar y sincronizar la base automáticamente).*

### Comandos Auxiliares
* **Ejecutar suite de pruebas automatizadas (`pytest`, 13 tests):**
  ```bash
  make test
  ```
* **Ejecutar pruebas de integridad referencial de dbt (`dbt test`, 16 tests):**
  ```bash
  make dbt-test
  ```
* **Detener servicios o limpiar artefactos locales:**
  ```bash
  docker compose down
  make clean
  ```

---

## 3. Arquitectura del Sistema

El ecosistema desacopla la ingesta declarativa (`dlt`), el procesamiento columnar vectorizado (`DuckDB`), el modelado analítico (`dbt Core`), la orquestación persistente (`Dagster`), el consumo analítico (`Metabase`) y la observabilidad (`Prometheus` + `Grafana` + `Dozzle`):

```text
                       ORQUESTADOR PERSISTENTE: DAGSTER (:3000)
          [dagster-webserver + dagster-daemon (Schedules & Sensors)]
  ┌───────────────────────────────────────────────────────────────────────────────┐
  │  raw_taxi_data ──► quality_validated_data ──► dimensional_star_schema ──► .md │
  └────────┬────────────────────┬────────────────────────────┬────────────────────┘
           │                    │                            │
           ▼                    ▼                            ▼
  ┌──────────────────┐ ┌────────────────────┐ ┌──────────────────────────────┐
  │  1. INGESTA dlt  │ │ 2. SCHEMA & QUALITY│ │    5. MODELADO (dbt Core)    │
  │ (PyArrow Batches)│ │ (DuckDB in-process)│ │        (dbt-duckdb)          │
  └────────┬─────────┘ └─────────┬──────────┘ └──────────────┬───────────────┘
           │ Streaming 64KB      │ Invariante:               │ 3 Capas en DuckDB:
           │ + Hash SHA-256      │ Leídas = Cur. + Cuar.     │ • staging (Tables)
           ▼                     └───────┐                   │ • intermediate (Views)
   data/raw/yellow/      ┌───────────────┴────────────┐      │ • marts (Tables)
   └── year=AAAA/        │                            │      └───────┬───────┬───────┘
       └── *.parquet     ▼                            ▼              │       │
   data/raw/zones/  [RECHAZADAS]                 [ACEPTADAS]         │       ▼
   └── lookup.csv   data/quarantine/             data/curated/       │  RESULTADOS.md
                                                                     │
                                                                     ▼ (:ro)
                                                        ┌─────────────────────────┐
                                                        │   BI: METABASE (:3002)  │
                                                        │ (Self-Service Analytics)│
                                                        └─────────────────────────┘

  ┌───────────────────────────────────────────────────────────────────────────────┐
  │  OBSERVABILIDAD IaC: docker_exporter.py ──► Prometheus (:9090) ──► Grafana    │
  │                      /var/run/docker.sock ──► Dozzle (:8080)         (:3001)  │
  └───────────────────────────────────────────────────────────────────────────────┘
```

### Estructura del Repositorio
```text
nyc-taxi/
├── docker-compose.yml              # Orquestación de los contenedores con límites de RAM e IaC
├── Dockerfile                      # Imagen Python 3.12 para Dagster, dlt, dbt y DuckDB
├── Makefile                        # Atajos CLI (all, trigger, test, dbt-test, clean)
├── README.md                       # Documentación técnica, arquitectura y decisiones de diseño
├── RESULTADOS.md                   # Respuestas SQL e interpretaciones a las 6 preguntas de negocio
├── requirements.txt                # Dependencias fijadas del proyecto
├── contracts/
│   └── yellow_schema.yaml          # Contrato de esquema destino versionado (mapeos y tipos)
├── dagster_home/
│   └── dagster.yaml                # Configuración de SQLite storage, run coordinator y retención
├── monitoring/
│   ├── docker_exporter.py          # Exportador liviano de métricas Docker compat. con cgroups v2
│   ├── prometheus.yml              # Configuración de scraping cada 5 segundos
│   ├── metabase/
│   │   └── setup_metabase.py       # Auto-aprovisionamiento IaC de usuario y conexión DuckDB (:ro)
│   └── grafana/
│       └── provisioning/           # Aprovisionamiento automático (IaC) de datasources y paneles
├── src/
│   ├── __init__.py                 # API pública y metadatos del paquete
│   ├── config.py                   # Rutas, parámetros de memoria DuckDB y parseo de períodos
│   ├── step1_ingest.py             # Pipeline dlt (streaming, SHA-256, PyArrow batches, raw_dlt)
│   ├── step2_schema.py             # Validación contra contrato, snake_case y auditoría de drift
│   ├── step3_quality.py            # 9 reglas de calidad, cuarentena Parquet/SQL y reconciliación
│   ├── step4_transform.py          # Columnas derivadas, enriquecimiento y particionado Hive
│   ├── step5_model_and_queries.py  # Ejecución de dbt Core, consultas analíticas y RESULTADOS.md
│   └── dagster_orchestrator.py     # Software-Defined Assets, Job, Schedule mensual y Sensor
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
    ├── test_idempotency.py         # Verificación de idempotencia de dlt, SHA-256 y re-descarga
    ├── test_quality_rules.py       # Validación de reglas REJECT vs WARNING
    ├── test_reconciliation.py      # Prueba de invariante matemática y aborto ante fugas
    └── test_schema_contract.py     # Validación de evolución de esquema y mapeo snake_case
```

---

## 4. Decisiones Técnicas y Justificaciones

### A. Elección del Stack y Presupuesto de Memoria (< 8 GB RAM)
* **Motor Analítico (`DuckDB` + `PyArrow`):** En lugar de `pandas` (que expande strings y timestamps en RAM entre 5x y 10x provocando errores *Out-Of-Memory* al cruzar más de 6 millones de filas) o `PySpark` (cuya JVM consume ~1.5 GB de overhead inicial), se eligió **DuckDB**. DuckDB opera mediante ejecución vectorizada por bloques y soporta *out-of-core spilling* a disco. Se configuró explícitamente `SET memory_limit = '4GB'` y `SET preserve_insertion_order = false` para reducir un 40% la memoria intermedia en ordenamientos y agregaciones.
* **Ingesta Declarativa (`dlt` con *Transformers* y *PyArrow Batches*):** Reemplaza scripts imperativos por `@dlt.source` y `@dlt.transformer`, leyendo los archivos Parquet en lotes columnares de `100,000` filas (`iter_batches`) sin materializar diccionarios Python en memoria, registrando metadatos nativos de carga en `raw_dlt._dlt_loads` e idempotencia mediante verificación de hash SHA-256.
* **Observabilidad Liviana Compatible con `cgroups v2` (`docker_exporter.py` + `Prometheus` + `Grafana` + `Dozzle`):** En kernels Linux modernos con `cgroups v2`, el binario estándar de `cAdvisor` suele fallar al mapear los nombres de contenedores de systemd y consume ~128 MB. Se implementó un exportador nativo en Python estándar (`monitoring/docker_exporter.py`) que consulta `/var/run/docker.sock` en paralelo consumiendo solo **~18 MB de RAM**, alimentando a Prometheus (muestreo cada 5s) y a dashboards auto-aprovisionados en Grafana con alertas visuales respecto al límite de `8192 MB`.
* **Business Intelligence sin Bloqueos (`Metabase` en Modo Solo Lectura):** Se utiliza la imagen `ghcr.io/motherduckdb/metabase-duckdb:latest` con el driver JDBC de DuckDB preintegrado. Para proteger el presupuesto de 8 GB, evitar conflictos de concurrencia con Dagster y eliminar pasos manuales:
  1. Se acotó la JVM mediante `JAVA_TOOL_OPTIONS: "-Xmx1024m -Xms512m"` y `mem_limit: 1536m`, iniciando con `user: "0:0"` para que su *entrypoint* gestione permisos sobre `./data/metabase` antes de reducir privilegios.
  2. El volumen `./data/warehouse:/data/warehouse:ro` se monta en **solo lectura a nivel de sistema operativo** y el script `monitoring/metabase/setup_metabase.py` registra automáticamente la base con `read_only: true` vía API REST, permitiendo consultas exploratorias simultáneas sin bloquear escrituras del pipeline.

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

## 5. Modelado Dimensional y Granularidad (dbt Core)

El almacén analítico (`data/warehouse/nyc_taxi.duckdb`) está organizado en esquemas separados por responsabilidad mediante **dbt Core**:
* `raw_dlt`: Tablas crudas cargadas por `dlt` (`yellow_trips_raw`, `taxi_zones`, `ingestion_log`, `_dlt_loads`, `_dlt_version`).
* `main`: Vistas y tablas de control y auditoría (`ingestion_log`, `schema_audit_log`, `quality_summary_report`, `quality_rule_counts`, `quarantine_trips`).
* `staging` *(Tablas)*: `stg_tlc__yellow_trips` y `stg_tlc__taxi_zones`. Castea tipos de datos, genera claves determinísticas `MD5` y elimina atributos redundantes de zona para respetar la 3FN antes del modelado dimensional.
* `intermediate` *(Vistas)*: `int_trips_enriched` e `int_zone_hourly_metrics`. Encapsula reglas reutilizables (*Role-Playing Joins* de zonas, decodificación de `payment_type` y clasificación de aeropuertos).
* `marts` *(Tablas — consumidas directamente por Metabase)*:
  * **`marts.fact_trips`:** **Granularidad:** 1 fila por cada viaje individual de taxi amarillo validado por la compuerta de calidad.
  * **`marts.dim_zone`:** **Granularidad:** 1 fila por cada zona oficial de la TLC (265 zonas). Actúa como *Role-Playing Dimension* para origen (`pu_location_id`) y destino (`do_location_id`).
  * **`marts.dim_date`:** **Granularidad:** 1 fila por cada día calendario único presente en los períodos analizados.
  * **`marts.agg_zone_hour`:** **Granularidad:** 1 fila por cada combinación única de `(pu_location_id, pickup_date, pickup_hour)`.
  * **`marts.mart_yoy_zone_performance`:** **Granularidad:** 1 fila por zona de origen comparando métricas agregadas interanuales.

---

## 6. Idempotencia y Reprocesamiento de Particiones

La capa curada se almacena en formato Parquet particionado estilo Hive:
```text
data/curated/yellow/
├── year=2024/month=02/curated_2024-02.parquet
└── year=2025/month=02/curated_2025-02.parquet
```
Al reprocesar un período, `step4_transform.py` construye el archivo en un directorio oculto temporal (`.tmp_year=AAAA_month=MM`) y reemplaza atómicamente (`shutil.move`) únicamente la carpeta de esa partición, dejando intactos los demás meses.

**Evidencia de ejecución repetida (`MES=2025-02`):**
* **Conteo de filas aceptadas en `year=2024/month=02` antes y después del reproceso:** `2,873,690` filas (sin duplicación).
* **Conteo de filas aceptadas en `year=2025/month=02` antes y después del reproceso:** `3,282,734` filas (total materializado en `marts.fact_trips`: `6,156,424` viajes).
* **Registros en `main.ingestion_log` tras correr dos veces:** 1 único registro por archivo con estado `ok` y verificación de hash SHA-256 (0 descargas redundantes).

---

## 7. Rendimiento y Uso de Recursos (Presupuesto 8 GB RAM)

Las métricas recolectadas en tiempo real por Prometheus y visualizadas en Grafana (`http://localhost:3001`) durante una ejecución completa (`2024-02` + `2025-02`, > 6.58 millones de registros leídos) demuestran el cumplimiento estricto de la restricción de hardware:

* **Hardware de prueba:** Notebook con 8.00 GB de RAM bajo Linux Mint XFCE (x86_64).
* **Tiempo total de ejecución E2E en Dagster (`make trigger` / `make all`):** `~82 segundos` (`raw_taxi_data`: 13.8s | `quality_validated_data`: 21.4s | `dimensional_star_schema` + `dbt test`: 34.0s).
* **Consumo en reposo de toda la plataforma (servicios activos):** `~1.35 GB` combinados (`dagster`: ~210 MB, `metabase`: ~780 MB, `grafana`: ~110 MB, `prometheus`: ~85 MB, `dozzle`: ~22 MB, `docker_exporter`: ~18 MB).
* **Pico máximo del contenedor de procesamiento (`nyc_taxi_dagster`):** `~1.93 GB` (**23.5%** del límite de 8.00 GB), manteniendo el consumo total del sistema por debajo de **4.2 GB** incluso con Metabase y Grafana activos simultáneamente.

---

## 8. Limitaciones Conocidas

1. **Censura en propinas en efectivo (`payment_type = 2`):** Los datos publicados por la TLC no registran propinas entregadas en efectivo al conductor, reportando `$0.00` sistemáticamente para ese medio de pago.
2. **Concurrencia de escritura única en DuckDB:** DuckDB permite múltiples lectores simultáneos (como Metabase en modo `read_only`), pero exige bloqueo exclusivo durante las escrituras de `dlt` y `dbt`. Por ello, clientes externos de escritorio como DBeaver deben configurarse en modo *Read-Only* o desconectarse antes de disparar una corrida.
3. **Almacenamiento local en un solo nodo:** Los archivos Parquet y el warehouse residen en volúmenes locales de Docker; para escalar a múltiples años completos en un entorno distribuido bastaría apuntar el destino de `dlt` y `httpfs` de DuckDB a un Object Storage compatible con S3 (como MinIO o AWS S3).

---

## 9. Declaración de Uso de Inteligencia Artificial

En cumplimiento con las normas de la cátedra, se declara que se utilizó **Gemini** como herramienta de asistencia durante el desarrollo del trabajo para:
1. El diseño estructural de los módulos Python (`dlt`, contratos YAML, reglas de calidad), la orquestación persistente en Dagster y la integración con dbt Core.
2. La configuración declarativa de infraestructura (`Docker Compose`, exportador de métricas compatible con `cgroups v2`, aprovisionamiento de Grafana e integración automatizada de Metabase en modo solo lectura).
3. La redacción de casos de prueba unitarios con datos sintéticos en `pytest` y la optimización de consultas SQL analíticas.

Toda la arquitectura, umbrales de calidad, reglas de negocio, pruebas de ejecución y consultas dimensionales fueron auditados, probados y comprendidos integralmente por los autores para su defensa oral.
