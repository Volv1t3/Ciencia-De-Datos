# Pset 2 implementation guide

This document describes the implementation as a local development deployment.
It is intentionally not a production platform: it has no Airflow, Kubernetes,
Kafka, remote object store, or automatic Tianchi download. Kestra is the only
orchestrator and Snowflake is an existing external warehouse.

## Boundary: `src/main` versus `src/res`

The repository separates executable project source from resources that configure
or support that source.

| Area | Meaning | Contents in this pset |
| --- | --- | --- |
| `src/main` | Code that the student owns, edits, reviews, and versions as application logic. | Kestra flow/DAG YAML, dbt project and model YAML/SQL, and PySpark jobs. |
| `src/res/config` | Runtime configuration consumed by containers or external services. | Kestra server settings, dbt connection profile, Spark settings, and Snowflake initialization scripts. |
| `src/res/data` | Local input data supplied outside Git. | Tianchi archives are placed in `data/raw`; it is mounted read-only. |
| `src/res/docker` | Image build recipes and associated artifacts, not application source. | Supplied dbt-ui Dockerfiles, the pset Spark image recipe, and the dbt-ui Nginx proxy configuration. |
| `src/res/logs` | Host-visible runtime output. | dbt log files, Kestra task temporary work, and Spark event logs. |

This distinction is important operationally. Editing `src/main/dbt` changes a
dbt project; editing `src/res/config/dbt/profiles/profiles.yml` changes how that
project connects. Likewise, a flow under `src/main/kestra/flows` is DAG source,
whereas `src/res/config/kestra/application.yml` configures Kestra itself.

## System topology

```text
Host files                                      Docker network
──────────                                      ──────────────
src/res/data/raw ──read-only──► Kestra ──JDBC──► kestra-postgres
       │                             │                 │
       │                             ├── imports flow   └── named metadata volume
       │                             │    source from src/main/kestra/flows
       │                             └── future Snowflake ingestion
       │
src/main/dbt ──bind mount──► dbt-ui backend ◄── Nginx dbt-ui browser service
                                    │
                       dbt profile from src/res/config/dbt
                                    │
                               Snowflake (external)
                                    │
src/main/python/spark/jobs ─► Spark local[*] ─┘
        │                         │
src/res/config/spark ─────────────┘
```

All Compose services use the private bridge network
`ciencia-de-datos-data-platform-pset-2`. Services resolve each other through
their service names; no container-to-container IP address is hard-coded.
Only the Kestra UI/management interface, dbt-ui browser interface, and optional
PostgreSQL port are bound to `127.0.0.1` on the host.

## Root deployment files

| File | Technical role | Relationships |
| --- | --- | --- |
| `docker-compose.yml` | Defines the `kestra-postgres`, `kestra`, `dbt-ui-backend`, `dbt-ui`, and `spark` services; health checks; network; bind mounts; and named volumes. | It is the sole place that connects `src/main` code and `src/res` resources to container paths. It reads values from `src/res/env/.env` and builds Dockerfiles in `src/res/docker/custom-images`. |
| `src/res/env/.env.example` | Safe template for ports, local PostgreSQL credentials, Spark resource limits, dbt threads, and Snowflake variables. | Copy to `src/res/env/.env`; Compose interpolates it. The actual `src/res/env/.env` is ignored. Snowflake values are intentionally blank because the project must use an existing account/database selected by the user. |
| `../../../../.gitignore` | Prevents secrets, local data archives, dbt generated artifacts, bytecode, and private keys from being committed. | Preserves `.gitkeep` so empty required mount directories exist after clone. |
| `README.md` | Operator-oriented quick start, validation commands, ports, and stop/reset instructions. | Links here for the deeper implementation rationale. |
| `IMPLEMENTATION.md` | This file. | Documents the connection between code, configuration, mounts, Docker state, and Snowflake setup. |
| `src/res/prompts/infrastructureSetup_santiagoArellano_00328370.xml` | The pset specification supplied by the course. It is not loaded by Compose or any container. | It is the requirements source used to decide which files and services exist. |

## Docker Compose service design

### `kestra-postgres`

`kestra-postgres` is PostgreSQL 17 Alpine. It accepts exactly the three
`KESTRA_POSTGRES_*` values from `src/res/env/.env`, writes its database directory to the
named `kestra_postgres_data` volume, and is checked with `pg_isready`.

It exists only for Kestra: no dbt model, Spark job, or warehouse table is hosted
there. The host mapping defaults to `127.0.0.1:5433` to avoid conflicting with a
developer’s PostgreSQL on port 5432; containers always use
`kestra-postgres:5432` internally.

### `kestra`

`kestra` runs the pinned `kestra/kestra:v1.3.38` image in standalone server
mode. Its dependency condition prevents it from starting until
`kestra-postgres` passes the health check. It gets its JDBC hostname and
credentials through `ENV_KESTRA_*`, never from the checked-in YAML.

Mounts:

| Host or volume | Container path | Why |
| --- | --- | --- |
| `src/res/config/kestra/application.yml` | `/etc/kestra/application.yml` (read-only) | Server configuration: repository, queue, storage, database datasource. |
| `src/main/kestra/flows` | `/workspace/kestra/flows` (read-write) | Versioned flow source exposed for import and editable from either the host or container. It is not server configuration. |
| `src/res/data/raw` | `/usr/data/landing` (read-only) | Immutable manual Tianchi landing area. |
| `kestra_internal_storage` | `/app/storage` | Kestra internal local-storage backend for task outputs. |
| `src/res/logs/kestra` | `/tmp/kestra-wd` | Host-visible task temporary working area. It is not the authoritative execution-history store. |

The management health check is `http://localhost:8081/health` inside the
container. The browser UI is port 8080. `kestra_postgres_data` remains the
authoritative location for imported flow definitions, queues, runs, execution
metadata, and repository-backed logs; deleting that volume destroys this state.

Kestra does not auto-import a YAML merely because the file is mounted. Import
`src/main/kestra/flows/bronze_ingestion.yml` using the UI. Re-import after a
source change. Editing `/workspace/kestra/flows` in the container updates the
host bind mount, so it is suitable for container-side authoring. Saving a flow
through the Kestra UI instead writes the imported definition to PostgreSQL and
does not automatically synchronize the source YAML back to the mount. Git tracks
the editable DAG source; the database tracks the imported revision and its runs.

### `dbt-ui-backend` and `dbt-ui`

The provided `Dockerfile.semana05.backend` builds the backend from a pinned
commit of EricLamphere/dbt-ui. It creates the same virtual environment dbt-ui
uses for its subprocess calls, then installs pinned `dbt-core`, `dbt-duckdb`,
and `dbt-snowflake` versions there. Therefore `dbt debug`, `compile`, `run`,
`build`, and `test` from dbt-ui use the installed dbt executable rather than a
second unrelated container.

The provided `Dockerfile.semana05.frontend` compiles dbt-ui’s Vite frontend in a
Node 20 build stage and serves it from Nginx. `dbt-ui.nginx.conf` routes `/api/`
to `dbt-ui-backend:8001` over the private Docker network and routes all other
requests to the SPA’s `index.html`, allowing client-side routes to work.

| Backend mount/value | Technical effect |
| --- | --- |
| `src/main/dbt:/workspace/dbt-projects` | Exposes dbt project source to the backend and editor. This is the source-controlled implementation location. |
| `src/res/config/dbt/profiles:/home/dbtui/.dbt` | Provides the dbt profile while allowing the container to modify credentials configuration. |
| `dbt_ui_data:/var/lib/dbt-ui` | Persists dbt-ui’s SQLite/configuration state across container recreation. |
| `src/res/logs/dbt:/var/log/dbt` and `DBT_LOG_PATH=/var/log/dbt` | Sends dbt CLI log output to a visible host resource directory. |
| `DBT_UI_PROJECTS_PATH=/workspace/dbt-projects` | Tells dbt-ui where to discover its mounted project directories. |

The public `dbt-ui` service only starts after the backend’s API health check
passes. The browser communicates with the Nginx frontend on host port 5173;
Nginx communicates with the unexposed backend by service name. dbt-ui has no
authentication and must remain local-only.

### `spark`

The `spark` service is an idle development container, not a fake distributed
cluster. Its `while true; sleep 3600` command consumes negligible CPU while
allowing `docker compose exec spark spark-submit ...`. `spark.master=local[*]`
causes each submitted job to use the host-visible CPU allocation assigned to the
container.

It builds from `src/res/docker/custom-images/Dockerfile.pset2.spark`, which uses
exactly `apache/spark:4.0.4-scala2.13-java21-python3-r-ubuntu`. The tag provides
Spark 4.0.4, Scala 2.13, Java 21, Python 3, and R. The custom layer adds the
Scala 2.13 Snowflake connector `3.2.2-spark_4.0` and JDBC driver `4.0.2` to
`/opt/spark/jars` plus any future requirements listed in the resource file.

| Host resource/code | Container path | Technical effect |
| --- | --- | --- |
| `src/main/python/spark/jobs` | `/opt/spark/jobs` (read-only) | Versioned PySpark program source. |
| `src/res/config/spark/spark-defaults.conf` | `/opt/spark/conf/spark-defaults.conf` (read-only) | Local master, adaptive SQL, low-noise console settings, and event logging. |
| `src/res/config/spark/spark-env.sh` | `/opt/spark/conf/spark-env.sh` (read-only) | Converts `src/res/env/.env` memory variables into Spark launcher variables without baking local memory decisions into the image. |
| `src/res/config/spark/log4j2.properties` | `/opt/spark/conf/log4j2.properties` (read-only) | Reduces Spark/Hadoop logs to warnings while retaining failures. |
| `src/res/logs/spark` | `/opt/spark/logs` | Receives Spark event-log files declared by `spark.eventLog.dir`. |

The service receives Snowflake values for the eventual OBT job, but the current
job does not issue a remote query or write any table.

## Code under `src/main`

### Kestra flow source

| File | What it implements | Integration point |
| --- | --- | --- |
| `src/main/kestra/flows/bronze_ingestion.yml` | A deliberately non-destructive Bronze ingestion DAG skeleton. It accepts optional start/end dates, confirms the three exact expected archives can be read, and records the intended extract → discover daily CSV → range selection → stage → COPY → validation sequence. | Imported into the Kestra server. The archive validation reads `/usr/data/landing`, which Compose maps to `src/res/data/raw`. No source archive is modified. |
| `src/main/kestra/flows/README.md` | Explains that flow YAML is source code and must be imported manually. | Prevents the mistaken assumption that a bind mount automatically registers flows. |

The flow intentionally has no production `COPY INTO`: the file headers, quoting,
null convention, and column layout of the actual Alibaba archives must be
inspected first. Its idempotency note requires a final date/disk key or `MERGE`
policy before retry/backfill is enabled.

### dbt project source

| File | What it implements | Integration point |
| --- | --- | --- |
| `src/main/dbt/ssd_failure_prediction/dbt_project.yml` | Defines a valid dbt project named `ssd_failure_prediction`, selects its profile, discovers models/macros/tests, and maps future `silver` and `gold` models to the corresponding Snowflake schemas. | dbt-ui discovers this project beneath `/workspace/dbt-projects`; dbt reads the profile from `src/res/config/dbt/profiles`. |
| `packages.yml` | Declares no dbt packages. | Keeps the scaffold minimal and avoids unnecessary third-party code. |
| `models/sources/sources.yml` | Declares the conceptual `bronze` source in the configured Snowflake database and `BRONZE` schema. It deliberately names no table or columns. | Future Silver models can refer to `source('bronze', ...)` after source data has been inspected and a table is defined. |
| `models/silver/.gitkeep` | Reserves the source-controlled location for future Silver dbt models. | The `+schema: SILVER` project configuration applies here. |
| `models/gold/.gitkeep` | Reserves the source-controlled location for future Gold dbt models. | The `+schema: GOLD` project configuration applies here. |
| `macros/.gitkeep` | Reserves the project macro directory. | dbt will discover macros added here. |
| `tests/.gitkeep` | Reserves the project test directory. | dbt will discover singular tests added here. |

### Spark job source

| File | What it implements | Integration point |
| --- | --- | --- |
| `src/main/python/spark/jobs/build_obt.py` | A safe Spark OBT connectivity skeleton. It creates a `SparkSession`, prints Spark/Python/master information, loads the Snowflake connector class from the JVM classpath, reports whether credentials are present, and stops the session. | Run with `docker compose exec spark spark-submit /opt/spark/jobs/build_obt.py`. It proves local Spark and connector discovery without contacting Snowflake. |
| `src/main/python/spark/jobs/README.md` | States the future OBT contract: Gold tables → Spark DataFrames → joins/validation → OBT tables. | Documents the explicit boundary between this infrastructure pset and future transformation logic. |

## Runtime configuration under `src/res/config`

| File | Technical purpose | Consumer |
| --- | --- | --- |
| `kestra/application.yml` | Selects PostgreSQL for the repository and queue; configures local internal storage at `/app/storage`; maps `${ENV_KESTRA_*}` to the JDBC datasource; disables basic authentication for this local-only stack. | Kestra server. The Compose environment supplies the actual database values. |
| `dbt/profiles/profiles.yml` | Defines the `ssd_failure_prediction` Snowflake profile. Every account-dependent setting uses dbt `env_var()`, including password and database. | dbt running inside dbt-ui’s backend virtual environment. It must never contain literal credentials. |
| `spark/spark-defaults.conf` | Sets `local[*]`, adaptive SQL, conservative shuffle partitions, quiet console progress, and Spark event logging to `/opt/spark/logs`. | Spark launcher and submitted Spark jobs. |
| `spark/spark-env.sh` | Exports `SPARK_DRIVER_MEMORY` and `SPARK_EXECUTOR_MEMORY`, using `src/res/env/.env` values or `2g` local fallbacks. | Sourced by Spark launch scripts; it makes memory adjustable without changing source or rebuilding an image. |
| `spark/log4j2.properties` | Configures Spark/Hadoop loggers to WARN and defines a console appender. | Spark’s Log4j2 runtime. |
| `spark/requirements.txt` | Reserved resource manifest for Python libraries required by future Spark jobs. | Copied by the Spark Dockerfile and installed during image build. It is currently intentionally empty except comments. |

## Snowflake bootstrap resources

These SQL scripts exist to prepare an already provisioned Snowflake database for
the pipeline’s logical layers. They do **not** run automatically; Compose never
connects to Snowflake during startup. Run them manually in a Snowflake worksheet
or another client authenticated with a role allowed to create schemas, file
formats, and stages.

They do not create a database because account/database ownership and naming are
external infrastructure decisions. The user chooses an existing database, which
must also be the value of `SNOWFLAKE_DATABASE` in `src/res/env/.env` for dbt and future jobs.

### Correct execution procedure

1. Open one Snowflake worksheet and select a role authorized for the existing
   project database.
2. In `001_database_and_schemas.sql`, replace
   `'<existing_project_database>'` with that database’s exact identifier.
3. Run the entire first script. It sets the worksheet-session variable
   `PROJECT_DATABASE` and creates only the `BRONZE`, `SILVER`, `GOLD`, and `OBT`
   schemas under that database using `IDENTIFIER()`.
4. Without opening a new worksheet/session, run `002_file_formats.sql`, then
   `003_stages.sql`. Both use the session’s `PROJECT_DATABASE` variable.
5. Put the same database value in `src/res/env/.env` as `SNOWFLAKE_DATABASE`; choose
   `BRONZE` as the initial `SNOWFLAKE_SCHEMA` when validating raw ingestion.

| File | What it does | Why it is separate |
| --- | --- | --- |
| `snowflake/bootstrap/001_database_and_schemas.sql` | Requires the caller to set an existing database name, derives qualified schema identifiers, and idempotently creates BRONZE, SILVER, GOLD, and OBT schemas. | Schema topology is warehouse setup, not dbt/Spark application logic. No database is named or created in Git. |
| `snowflake/bootstrap/002_file_formats.sql` | Selects the caller’s existing database and BRONZE schema, then idempotently defines `SMART_CSV_FORMAT`. | Kestra’s eventual Bronze load will reference a named Snowflake file format. CSV details remain provisional until real files are inspected. |
| `snowflake/bootstrap/003_stages.sql` | Selects the same database/schema and idempotently creates `SMART_ARCHIVE_STAGE` using `SMART_CSV_FORMAT`. | Future Kestra ingestion can upload archive-derived files to this internal stage before a dataset-specific `COPY INTO`. |

`002` currently sets comma delimiter, one header row, optional double-quote
enclosure, and empty fields as null. Those are a starting configuration, not a
claim about uninspected input. Verify the real data before any load is enabled.

## Data and log resources

| File/directory | Function | Git behavior |
| --- | --- | --- |
| `src/res/data/raw/.gitkeep` | Ensures the empty manual landing directory exists in a clone. | Kept. Every other item in `raw` is ignored. |
| `src/res/logs/kestra/.gitkeep` | Ensures the Kestra temporary-work bind mount exists. | Kept; runtime output is not committed. |
| `src/res/logs/dbt/.gitkeep` | Ensures the dbt CLI log bind mount exists. | Kept; `DBT_LOG_PATH` writes runtime logs beside it. |
| `src/res/logs/spark/.gitkeep` | Ensures the Spark event-log bind mount exists. | Kept; Spark writes event files there. |

The `.gitkeep` files carry no configuration or executable logic. They exist only
because Git otherwise omits empty directories that Docker must mount.

## Image resources

| File | Technical purpose |
| --- | --- |
| `src/res/docker/custom-images/Dockerfile.semana05.backend` | Supplied dbt-ui backend build recipe: pins upstream dbt-ui, creates its virtual environment, and installs dbt plus adapters. Compose builds it as `pset2-dbt-ui-backend:1fe89c5`. |
| `src/res/docker/custom-images/Dockerfile.semana05.frontend` | Supplied dbt-ui frontend build recipe: builds the Vite UI from the same pinned upstream revision and packages the static site in Nginx. Compose builds it as `pset2-dbt-ui-frontend:1fe89c5`. |
| `src/res/docker/custom-images/dbt-ui.nginx.conf` | Nginx virtual host used by the frontend image. It forwards API calls to the backend service and falls back to `index.html` for SPA routes. |
| `src/res/docker/custom-images/Dockerfile.pset2.spark` | Custom resource build recipe for the requested Apache Spark base image and the required Snowflake connector/JDBC artifacts. Compose builds it as `pset2-spark:4.0.4-scala2.13-java21`. |

The Dockerfiles are resources because they define external runtime images; none
contains student transformation logic. The dbt-ui application itself is fetched
from its pinned upstream commit during image build. No Dockerfile carries a
Snowflake credential.

## Persistence, restart, and destruction

`docker compose down` removes containers and the network but retains the named
volumes. A later `up` restores Kestra’s repository state and dbt-ui’s internal
state. `docker compose down -v` is destructive: it deletes
`kestra_postgres_data`, `kestra_internal_storage`, and `dbt_ui_data`. The host
landing directory remains outside those volumes, but it is still the user’s
manually acquired input data and must be backed up independently.

## Validation map

| Goal | Command | What success proves |
| --- | --- | --- |
| Render configuration | `docker compose --env-file .env.example config` | YAML, interpolation, mounts, and required variables are coherent. |
| Build images | `docker compose build` | The supplied dbt-ui recipes and custom Spark recipe can retrieve and assemble their pinned dependencies. |
| Service health | `docker compose up -d && docker compose ps` | PostgreSQL gates Kestra; dbt frontend gates on backend health; Spark stays ready for `exec`. |
| Landing visibility | `docker compose exec kestra ls -l /usr/data/landing` | The resource landing directory is visible at the flow contract path. |
| dbt integration | `docker compose exec dbt-ui-backend /opt/dbt-ui/backend/.venv/bin/dbt --version` | dbt-ui’s own subprocess virtual environment contains dbt and adapters. |
| Spark integration | `docker compose exec spark spark-submit /opt/spark/jobs/build_obt.py` | Spark starts local mode and discovers the installed Snowflake connector. |

Do not treat successful container startup as a Snowflake connectivity test. That
test is intentionally opt-in and occurs only when valid user-provided
`SNOWFLAKE_*` values are present and `dbt debug` (or a future controlled job) is
run.
