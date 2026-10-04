-- Run in the same worksheet/session as 001_database_and_schemas.sql.
-- Bronze keeps source values unchanged inside RAW_RECORD; dbt performs typing.
USE DATABASE IDENTIFIER($PROJECT_DATABASE);
USE SCHEMA IDENTIFIER($BRONZE_SCHEMA);

--? Esquema generico de Bronze (el mismo que crea el flow de Kestra):
CREATE TABLE IF NOT EXISTS SMART_2018_RAW (
  --? Linaje: ZIP + CSV + numero de linea = clave natural de la fila (usada en el MERGE).
  SOURCE_ARCHIVE VARCHAR,
  SOURCE_FILE VARCHAR,
  SOURCE_ROW NUMBER,
  --? Fecha del archivo diario (NULL para las etiquetas de falla).
  SOURCE_DATE DATE,
  --? Hash del contenido: detecta si una fila cambio entre recargas.
  SOURCE_SHA256 VARCHAR,
  --? Todas las columnas originales como texto dentro de un JSON (VARIANT).
  RAW_RECORD VARIANT,
  INGESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

--? Mismas columnas para 2019 y para las etiquetas.
CREATE TABLE IF NOT EXISTS SMART_2019_RAW LIKE SMART_2018_RAW;
CREATE TABLE IF NOT EXISTS SSD_FAILURE_LABEL_RAW LIKE SMART_2018_RAW;
