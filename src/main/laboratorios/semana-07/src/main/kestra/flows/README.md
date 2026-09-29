# Bronze ingestion

`bronze_ingestion.yml` is a single Kestra flow for official NYC TLC Yellow Taxi
monthly Parquet files. It covers January–December 2025 and January–August 2026.
The [TLC download page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
currently publishes 2026 through July. The flow probes every month and skips
August only while it returns HTTP 404; a missing earlier month or another HTTP
status fails the execution. Rerun after August is published.

Each available file is downloaded with `curl` into the persistent host-mounted
`src/res/data/raw` directory, staged through Snowflake JDBC `PUT` without
additional compression, and merged into
`S_CDATOS_LABORATORIOINT.S_CDATOS_BRONZE.YELLOW_TAXI_TRIPS_RAW`. The row is
preserved as a `VARIANT`; the other columns record source month, staged file,
Parquet row number, file content key and first/updated load timestamp. The
`SOURCE_MONTH` + `SOURCE_ROW_NUMBER` merge key makes reruns idempotent for
unchanged monthly files. If TLC replaces a file with fewer rows, old trailing
rows require a separate reconciliation; this flow does not delete Bronze rows.

`bronze_ingestion_http.yml` is an isolated compatibility copy that uses
Kestra's HTTP Download task followed by Snowflake Upload. Use it only to test
the HTTP internal-storage path after a Kestra upgrade; its flow ID is
`nyc_yellow_taxi.bronze_ingestion_http`. It validates that the downloaded
artifact begins and ends with the Parquet `PAR1` signature before staging it.

Compose supplies raw `SNOWFLAKE_*` values for dbt and base64-encoded
`SECRET_SNOWFLAKE_*` values for Kestra OSS `secret('SNOWFLAKE_*')`.
Generate the latter from the local `.env` with
`python3 src/main/kestra/encode_snowflake_secrets.py src/res/env/.env`; copy its
output into that `.env`. No credential is stored in this YAML. The Snowflake
user/role needs warehouse usage and create
file format, stage, table, and DML privileges in the existing Bronze schema.
The flow creates its file format, stage and table if absent, so the bootstrap
SQL is optional once the database and schema already exist.

Import the YAML from `/workspace/kestra/flows/bronze_ingestion.yml` in the
Kestra UI, then execute `nyc_yellow_taxi.bronze_ingestion`. Mounted YAML is not
automatically imported; re-import after edits. This is only the Bronze layer:
Silver/Gold dbt models are separate deliverables.

To validate the flow locally without starting the Kestra server or querying
Snowflake, run from `laboratorios/semana-07`:

```bash
docker compose --env-file src/res/env/.env run --rm --no-deps kestra flow validate --local /workspace/kestra/flows/bronze_ingestion.yml
```

After a successful run, inspect Snowflake with:

```sql
SELECT SOURCE_MONTH, COUNT(*) AS ROWS_LOADED, MIN(LOADED_AT), MAX(LOADED_AT)
FROM S_CDATOS_LABORATORIOINT.S_CDATOS_BRONZE.YELLOW_TAXI_TRIPS_RAW
GROUP BY SOURCE_MONTH ORDER BY SOURCE_MONTH;

SELECT SOURCE_MONTH, SOURCE_ROW_NUMBER, COUNT(*) AS COPIES
FROM S_CDATOS_LABORATORIOINT.S_CDATOS_BRONZE.YELLOW_TAXI_TRIPS_RAW
GROUP BY SOURCE_MONTH, SOURCE_ROW_NUMBER HAVING COUNT(*) > 1;
```
# Bronze-flow local files

The bronze flow downloads each Parquet source to `/workspace/data/raw`. In the
Compose deployment this path is the `kestra_raw_data` named Docker volume, not
a macOS host bind mount. The volume keeps the download, checksum verification,
and Snowflake JDBC `PUT` in Docker's Linux filesystem and avoids Docker Desktop
bind-mount corruption of large Parquet files.

The data volume persists a normal `docker compose up` or service recreation.
To intentionally discard only the downloaded bronze inputs, stop the stack and
remove the project `kestra_raw_data` volume explicitly; do not remove the
Kestra internal-storage or PostgreSQL volumes.
