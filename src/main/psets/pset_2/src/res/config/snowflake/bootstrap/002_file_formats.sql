-- In the same worksheet/session as 001, use the caller-supplied existing DB.
-- Confirm delimiter, quoting, null representation, and compression after
-- inspecting an actual archive before using this format for COPY INTO.
USE DATABASE IDENTIFIER($PROJECT_DATABASE);
USE SCHEMA BRONZE;
CREATE FILE FORMAT IF NOT EXISTS SMART_CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE;
