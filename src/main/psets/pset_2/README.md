# Pset 2 — SSD failure prediction infrastructure

This is a local, reproducible infrastructure layer for the future batch ELT
pipeline. Kestra is the sole orchestrator; PostgreSQL is used only by Kestra.
Snowflake remains external to Compose.

For the file-by-file technical design, mounts, persistence model, and Snowflake
bootstrap procedure, see [IMPLEMENTATION.md](IMPLEMENTATION.md).

```text
Alibaba Tianchi archives
        │ (manual authenticated download)
        ▼
src/res/data/raw  ──read-only──► Kestra ──► Snowflake BRONZE
                                             │
                    dbt-ui / dbt Core ◄─────┘
                           │
                 Snowflake SILVER → GOLD
                           │
                    Spark local[*] → Snowflake OBT
```

## Layout and persistence

| Host path / volume | Purpose |
| --- | --- |
| `src/res/data/raw` | Immutable Tianchi archives; bind-mounted read-only at `/usr/data/landing`. It is ignored by Git. |
| `src/main/kestra/flows` | Source-controlled Kestra DAG/flow code. This read-write bind mount is editable from both host and container. |
| `kestra_postgres_data` | Kestra flow definitions after import, queues, executions, metadata, and logs. |
| `kestra_internal_storage` | Kestra task output files and internal local storage. |
| `src/main/dbt` | Source-controlled dbt SQL/YAML project code, visible to dbt-ui at `/workspace/dbt-projects`. |
| `dbt_ui_data` | dbt-ui SQLite state, run history, API logs, and generated UI data. |
| `src/main/python/spark` | Source-controlled PySpark job code. |
| `src/res/config` and `src/res/docker` | Runtime configuration and externally supplied/custom image definitions. |
| `src/res/logs` | Host-visible local runtime logs and working output. |

The flow source directory is intentionally separate from the named Kestra volumes:
you edit and commit it normally, then import a changed YAML into Kestra. A bind
mount does **not** automatically register a flow. PostgreSQL and internal task
storage intentionally remain Docker-managed persistent state.

## Prerequisites

- Docker Desktop with Compose v2 (Apple Silicon is supported by the selected
  multi-platform PostgreSQL and Spark 4.0.4 / Scala 2.13 / Java 21 image).
- At least 4 GB of Docker memory available for Kestra plus local Spark.
- A Snowflake account only when running dbt connectivity checks or future
  warehouse work.
- The three archives downloaded manually from Tianchi: `smartlog2018ssd.zip`,
  `smartlog2019ssd.zip`, and `ssd_failure_label.csv.zip`.

## First start

From this directory:

```bash
cp .env.example .env
# Edit .env: at minimum replace KESTRA_POSTGRES_PASSWORD.
mkdir -p src/res/data/raw
# Copy the three Tianchi archives into src/res/data/raw yourself.

docker compose config
docker compose build
docker compose up -d
docker compose ps
```

Open Kestra at <http://localhost:8080> and dbt-ui at
<http://localhost:5173>. The configuration binds both to `127.0.0.1`, so they
are not published to the local network. To use different ports, change
`KESTRA_PORT`, `KESTRA_MANAGEMENT_PORT`, or `DBT_UI_PORT` in `src/res/env/.env`.

Import `src/main/kestra/flows/bronze_ingestion.yml` in the Kestra UI
(Flows → Create → Import). It validates and consolidates the selected CSV
members, uploads CSV to the internal Snowflake stage, and idempotently merges
source-faithful `VARIANT` objects into the three Bronze destination tables.

## Validation

```bash
# Kestra UI and management health endpoint
curl --fail http://localhost:${KESTRA_PORT:-8080}/
curl --fail http://localhost:${KESTRA_MANAGEMENT_PORT:-8081}/health

# Confirm the read-only landing mount and required archive names
docker compose exec kestra ls -l /usr/data/landing

# Confirm dbt and its Snowflake adapter are installed in dbt-ui's own venv
docker compose exec dbt-ui-backend /opt/dbt-ui/backend/.venv/bin/dbt --version

# With valid SNOWFLAKE_* values in .env, confirm the dbt profile can connect
docker compose exec --workdir /workspace/dbt-projects/ssd_failure_prediction \
  dbt-ui-backend /opt/dbt-ui/backend/.venv/bin/dbt debug

# Validate Spark runtime, local mode, and the installed connector
docker compose exec spark java -version
docker compose exec spark python3 --version
docker compose exec spark spark-submit --version
docker compose exec spark spark-submit /opt/spark/jobs/build_obt.py

# Verify Kestra PostgreSQL persistence after a restart
docker compose restart kestra-postgres kestra
docker compose ps
```

The Spark check does not contact Snowflake. `build_obt.py` only starts a local
Spark session and confirms the connector class can be loaded.

## Snowflake bootstrap and dbt

Set the plain `SNOWFLAKE_*` values used by dbt and Spark and the base64-encoded
`SECRET_SNOWFLAKE_*` values used by Kestra in `src/res/env/.env`; never commit
that file. The Bronze flow creates its file format, internal stage, and tables
with `IF NOT EXISTS`. The following scripts remain as an optional manual
bootstrap/reference and can be run in one Snowflake worksheet session:

1. `src/res/config/snowflake/bootstrap/001_database_and_schemas.sql`
2. `src/res/config/snowflake/bootstrap/002_file_formats.sql`
3. `src/res/config/snowflake/bootstrap/003_stages.sql`
4. `src/res/config/snowflake/bootstrap/004_bronze_tables.sql`

The stage file format is CSV. Snowflake constructs one source-faithful
`RAW_RECORD` object from each staged CSV row during the merge. dbt’s project lives at
`src/main/dbt/ssd_failure_prediction`; its profile reads account-specific
values with `env_var()`.

## Stop and reset

```bash
docker compose down       # stops/removes containers and network; keeps volumes
docker compose down -v    # destructive: also deletes Kestra and dbt-ui state
```

Do not use `down -v` unless you intentionally want to erase Kestra history,
imported flows, task output files, and dbt-ui state. The immutable archives are
host files and are not put in a Docker volume.

## Troubleshooting

- A missing archive makes the skeleton flow fail by design. Verify the exact
  filename and that the file is readable under `src/res/data/raw`.
- On Apple Silicon, ensure Docker Desktop is current and has enough memory;
  the selected Spark 4.0.4 tag publishes an ARM64 variant. If a custom dependency
  later proves AMD64-only, diagnose it explicitly rather than silently forcing
  an emulated platform.
- dbt-ui’s backend is the process that invokes dbt. Do not replace it with an
  unrelated dbt container; that would remove dbt-ui’s subprocess integration.

## Bronze ingestion flow

The flow at `src/main/kestra/flows/bronze_ingestion.yml` reads the three ZIPs
from the read-only landing mount. It validates each source CSV, stages it, and
stores its original names and text values in `RAW_RECORD`. The three Bronze
destinations are `SMART_2018_RAW`, `SMART_2019_RAW`, and
`SSD_FAILURE_LABEL_RAW`. Metadata records the archive, CSV
member, row number, date (when present), and row hash. Replays merge on
archive/member/row number.

The `dataset` input selects `smart_2018`, `smart_2019`, or `failure_labels`
and safely defaults to the small labels dataset. Date inputs are optional,
inclusive `YYYY-MM-DD` filters for SMART files. The planner groups the selected
calendar months in pairs, runs up to six workers in parallel, and makes each
worker extract and upload its source days sequentially. Each daily temporary
file is removed after its Snowflake `PUT`, so even a full-year load does not
materialize a roughly 38 GiB annual CSV. January through March, for example,
uses two workers: January-February and March. A practical first smoke test is
`dataset=failure_labels`; a run with no selected source files fails.

The inspected source contract is 105 columns for both SMART years and three
columns (`model`, `failure_time`, `disk_id`) for the labels. The helper rejects
schema drift, malformed row widths, invalid UTF-8, and hidden ZIP metadata such
as `__MACOSX/._ssd_failure_label.csv`.

### Trigger, errores y backfill

El flow usa un **trigger manual bajo demanda**. Esta decisión responde a la
fuente: Alibaba/Tianchi no ofrece para estos archivos un endpoint estable que
Kestra pueda consultar o descargar periódicamente. La frecuencia operacional es
por evento: se ejecuta cuando se recibe una nueva versión de uno de los ZIP o
cuando se solicita un backfill. La adquisición del ZIP es manual, pero la carga
no lo es: una vez disponible en la zona `raw`, Kestra valida, particiona, sube,
fusiona y verifica los datos sin ejecutar SQL ni copiar archivos manualmente a
Snowflake. Un trigger cron no aportaría datos nuevos y solo reprocesaría los ZIP
locales sin cambios.

Los errores determinísticos —ZIP ausente, rango inválido, cambio de esquema,
UTF-8 inválido o fila mal formada— detienen la ejecución inmediatamente. Los
errores transitorios de Snowflake sí tienen retry exponencial. El DDL y cada
`PUT` permiten hasta cuatro intentos; el `MERGE`, que puede ser largo, permite
tres intentos durante un máximo de dos horas. Kestra conserva el estado, intento
y logs de cada task; la verificación final falla el flow si alguna fila staged
no aparece en Bronze.

El backfill usa los inputs inclusivos `requested_start_date` y
`requested_end_date`. Si posteriormente aparece un día que faltaba en 2018, se
agrega su CSV al ZIP de 2018 y se reejecuta ese día, un rango que lo contenga o
el año completo. La clave `(SOURCE_ARCHIVE, SOURCE_FILE, SOURCE_ROW)` hace el
proceso idempotente: una fila idéntica no cambia, una fila nueva se inserta y
una fila existente cuyo contenido cambió se actualiza únicamente cuando cambia
`SOURCE_SHA256`. Por tanto, no es necesario convertir el `PUT` en un insert
condicional; el control de duplicados corresponde al `MERGE` de Bronze. Si una
versión posterior elimina una fila que antes existía, Bronze no la borra: esa
reconciliación destructiva debe resolverse explícitamente en Silver, no durante
la conservación del dato fuente.

Before running, make sure the existing `S_CDATOS_PSET2` database and
`S_CDATOS_PSET2_BRONZE` schema are available to the configured role. Add the
five base64-encoded `SECRET_SNOWFLAKE_*` variables shown in `.env.example`,
restart Kestra so it receives them, and import the flow. The flow itself creates
`SMART_CSV_FORMAT`, `SMART_ARCHIVE_STAGE`, and the three
raw tables if they do not exist. Re-import after edits to the YAML; the helper
script is read from its bind mount on each run. The first live run should still
be checked against Snowflake row counts. The helper uses UTF-8 with an optional
BOM and refuses malformed CSV.
