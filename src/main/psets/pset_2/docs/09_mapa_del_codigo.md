# 9. Mapa del código

[← Ejecución](08_ejecucion.md) · [Índice](README.md) · **Mapa del código**

Todos los archivos de código tienen comentarios en español con el prefijo `#?`, `--?` o `{#? #}`
(estilo *Better Comments*), que explican qué hace cada bloque y por qué.

## Qué hace cada archivo

| Archivo | Capa | Qué hace |
| --- | --- | --- |
| `docker-compose.yml` | Infra | Define los 6 servicios, puertos, volúmenes y variables de entorno |
| `src/res/env/.env.example` | Infra | Plantilla de variables (sin valores reales) |
| `src/res/config/kestra/application.yml` | Infra | Kestra con Postgres, *basic auth* y storage local |
| `src/res/config/dbt/profiles/profiles.yml` | Infra | Conexión de dbt a Snowflake con `env_var()` |
| `src/res/config/snowflake/bootstrap/00*.sql` | Bronze | Creación manual opcional de esquemas, formato, stage y tablas |
| `src/main/kestra/flows/bronze_ingestion.yml` | Bronze | Flow de ingesta: DDL, plan, extracción, `PUT`, `MERGE` y reconciliación |
| `src/main/kestra/flows/prepare_bronze.py` | Bronze | Valida el ZIP y el esquema; arma paquetes de días; extrae un CSV con linaje |
| `dbt/.../dbt_project.yml` | dbt | Materialización y esquema por carpeta |
| `dbt/.../models/sources/sources.yml` | dbt | Declara las tablas Bronze como `source` |
| `dbt/.../models/silver/smart_20XX.sql`, `ssd_failure_labels.sql` | Silver | Tipado incremental desde Bronze |
| `dbt/.../models/silver/smart/intermediate/*null_processed.sql` | Silver | Quita columnas 100 % nulas y filas sin mediciones |
| `dbt/.../models/silver/smart/audit/*.sql` | Silver | Cuarentena de filas sin mediciones |
| `dbt/.../models/silver/smart/intermediate/*mc1.sql` | Silver | Subconjunto MC1 (44 columnas) |
| `dbt/.../models/silver/smart/final/*.sql` | Silver | `UNION ALL` de 2018 y 2019 |
| `dbt/.../models/gold/dimensions/*.sql` | Gold | `DIM_SSD` y `DIM_DATE` |
| `dbt/.../models/gold/facts/*.sql` | Gold | Hechos SMART diarios y de eventos de falla |
| `dbt/.../models/**/schema.yml` | Tests | Documentación y tests por modelo |
| `dbt/.../macros/secondary_silver_smart_columns.sql` | dbt | **Política de columnas**: listas de atributos vacíos y conservados |
| `dbt/.../macros/smart_attribute_columns.sql` | dbt | Genera las 102 columnas tipadas |
| `dbt/.../macros/*_tests.sql`, `tests/*.sql` | Tests | Tests genéricos propios y tests singulares |
| `dbt/.../macros/generate_schema_name.sql` | dbt | Usa el nombre de esquema tal cual (sin prefijo) |
| `src/main/python/spark/jobs/build_mc1_obt.py` | OBT | Job de la OBT (Snowpark Connect) |
| `src/main/python/spark/lib/snowflake_io.py` | EDA | Conexión Spark ↔ Snowflake para notebooks |
| `src/main/python/spark/notebooks/build_eda_*.py` | EDA | Generan los notebooks de EDA |
| `src/main/python/spark/notebooks/ssd_null_analysis_book/` | EDA | Evidencia y conclusiones del análisis de nulos |

## Dónde tocar para cambios típicos

| Si te piden… | Archivo(s) | Después |
| --- | --- | --- |
| Agregar un trigger programado a la ingesta | `bronze_ingestion.yml`: bloque `triggers:` con `io.kestra.plugin.core.trigger.Schedule` y `inputs` fijos | Reimportar el flow |
| Cambiar los reintentos | bloque `retry:` de la tarea en `bronze_ingestion.yml` | Reimportar el flow |
| Recargar un rango de fechas (backfill) | Nada: inputs `requested_start_date` y `requested_end_date` al ejecutar | — |
| Agregar o quitar una columna de la fuente | `SMART_HEADERS` en `prepare_bronze.py` **y** la lista `$5..$109` del `MERGE` | Reimportar el flow y recargar |
| Considerar "vacío" otro atributo SMART | `globally_unavailable_smart_attribute_ids()` o `mc1_unavailable_smart_attribute_ids()` | `dbt build`; si cambia MC1, actualizar `SMART_IDS` y los anchos esperados en `build_mc1_obt.py` |
| Imputar nulos en vez de dejarlos | `*_null_processed.sql`: `coalesce(n_X, valor)`; justificar en [calidad de datos](03_calidad_de_datos.md) | `dbt build --select tag:null_processed+` |
| Agregar una columna a una dimensión (p. ej. `is_holiday`) | `dim_date.sql` (+ test en `gold/schema.yml`) | `dbt build --select dim_date+` |
| Agregar un test | `schema.yml` (genérico) o un `.sql` en `tests/` (singular: debe devolver 0 filas) | `dbt test --select modelo` |
| Cambiar el horizonte de la etiqueta (p. ej. 14 días) | `_expected_label_status()` en `build_mc1_obt.py` (`between(1, 30)` y `date_add(..., 30)`) y el contexto `+30` en `apply_context_date_scope` | Corrida acotada y después producción |
| Agregar una ventana (p. ej. 60 días) | `WINDOW_DAYS` y `WINDOW_REGISTRY` en `build_mc1_obt.py`, el contexto `-59` y `EXPECTED_*_WIDTH` | `--validate-only` primero |
| Agregar un modelo de SSD además de MC1 | Nuevo subconjunto en `silver/smart/intermediate`, un hecho en Gold y parametrizar el filtro `MC1` en el job | `dbt build` + job |
