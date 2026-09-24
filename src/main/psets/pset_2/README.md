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
(Flows → Create → Import). It is deliberately a non-destructive skeleton: it
validates the three archives and records the required future loading stages.
Its explicit TODO is to inspect source files before deciding extraction rules,
Bronze columns, `COPY INTO`, and the date-based idempotency key/MERGE strategy.

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

Set the `SNOWFLAKE_*` values in `src/res/env/.env`; do not put them in Git, the Compose file,
dbt `profiles.yml`, flow YAML, or Python. Run these scripts in order from a
Snowflake worksheet using an appropriately privileged role:

1. `src/res/config/snowflake/bootstrap/001_database_and_schemas.sql`
2. `src/res/config/snowflake/bootstrap/002_file_formats.sql`
3. `src/res/config/snowflake/bootstrap/003_stages.sql`

The file-format script is provisional: inspect an actual CSV before enabling
production loading, especially its null and quoting conventions. dbt’s only
project lives at `src/main/dbt/ssd_failure_prediction`; its
profile reads every account-specific value with `env_var()`.

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
