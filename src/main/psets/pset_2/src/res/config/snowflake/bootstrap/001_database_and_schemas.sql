-- Snowflake remains external. Set this to an existing database before running.
-- This script intentionally never creates or hard-codes a database.
--? Bootstrap manual (opcional): se corre UNA vez en una worksheet de Snowflake, en orden
--? 001 -> 004 y en la MISMA sesion (usan variables de sesion definidas aqui).
--? La base S_CDATOS_PSET2 la provee la cuenta del curso; aqui solo se crean los esquemas.
--? Variables de sesion: SET X = ...; despues se usan como $X.
SET PROJECT_DATABASE = 'S_CDATOS_PSET2';
SET BRONZE_SCHEMA = $PROJECT_DATABASE || '.S_CDATOS_PSET2_BRONZE';
SET SILVER_SCHEMA = $PROJECT_DATABASE || '.S_CDATOS_PSET2_SILVER';
SET GOLD_SCHEMA = $PROJECT_DATABASE || '.S_CDATOS_PSET2_GOLD';
SET OBT_SCHEMA = $PROJECT_DATABASE || '.S_CDATOS_PSET2_OBT';

--? Un esquema por capa: Bronze (Kestra), Silver y Gold (dbt), OBT (Spark).
--? IDENTIFIER() permite usar el texto de una variable como nombre de objeto.
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($BRONZE_SCHEMA);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($SILVER_SCHEMA);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($GOLD_SCHEMA);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($OBT_SCHEMA);
