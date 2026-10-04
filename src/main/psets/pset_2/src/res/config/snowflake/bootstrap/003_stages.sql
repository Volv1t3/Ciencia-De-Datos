-- In the same worksheet/session as 001, use the caller-supplied existing DB.
--? Stage interno = almacenamiento de archivos dentro de Snowflake. Kestra sube aqui los CSV
--? diarios (PUT) y luego los lee con SELECT $1, $2 ... FROM @SMART_ARCHIVE_STAGE.
USE DATABASE IDENTIFIER($PROJECT_DATABASE);
USE SCHEMA IDENTIFIER($BRONZE_SCHEMA);
CREATE STAGE IF NOT EXISTS SMART_ARCHIVE_STAGE
  FILE_FORMAT = SMART_CSV_FORMAT;
--? Si el stage ya existia con otro formato, el ALTER lo corrige.
ALTER STAGE SMART_ARCHIVE_STAGE SET FILE_FORMAT = SMART_CSV_FORMAT;
