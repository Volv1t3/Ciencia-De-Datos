-- In the same worksheet/session as 001, use the caller-supplied existing DB.
--? Mismo FILE FORMAT que crea el flow de Kestra (create_csv_file_format). Esta duplicado a
--? proposito: el flow es autosuficiente y este script sirve para preparar Snowflake a mano.
USE DATABASE IDENTIFIER($PROJECT_DATABASE);
USE SCHEMA IDENTIFIER($BRONZE_SCHEMA);
CREATE FILE FORMAT IF NOT EXISTS SMART_CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  --? La primera linea del CSV es la cabecera.
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  --? Campo vacio -> NULL (no cadena vacia).
  EMPTY_FIELD_AS_NULL = TRUE
  ENCODING = 'UTF8'
  --? Detecta solo el .gz que genera PUT ... AUTO_COMPRESS = TRUE.
  COMPRESSION = AUTO;
