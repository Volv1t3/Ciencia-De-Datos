# 8. Ejecución paso a paso

[← Limitaciones](07_limitaciones.md) · [Índice](README.md) · **Ejecución** · [Siguiente: Mapa del código →](09_mapa_del_codigo.md)

Todos los comandos se ejecutan desde `src/main/psets/pset_2`. Si clonas en Windows, activa antes
las rutas largas: `git config --global core.longpaths true`.

## 0. Requisitos

- Docker Desktop con Compose v2 y al menos 4 GB de memoria.
- Una cuenta de Snowflake con la base `S_CDATOS_PSET2`, un warehouse normal y uno más grande
  para la OBT.
- Los 3 ZIP de Tianchi: `smartlog2018ssd.zip`, `smartlog2019ssd.zip` y
  `ssd_failure_label.csv.zip`.

## 1. Configurar el entorno

```bash
cp src/res/env/.env.example src/res/env/.env
# Completar src/res/env/.env. Las variables SECRET_SNOWFLAKE_* (Kestra) van en base64:
printf '%s' 'mi-valor' | base64
# Token de Jupyter:
openssl rand -hex 32
```

`src/res/env/.env` **nunca** se sube al repositorio.

## 2. Levantar la infraestructura

```bash
mkdir -p src/res/data/raw          # copiar aquí los 3 ZIP
docker compose --env-file src/res/env/.env config -q   # valida el compose y el .env
docker compose --env-file src/res/env/.env build
docker compose --env-file src/res/env/.env up -d
docker compose --env-file src/res/env/.env ps          # todo debe estar "healthy"
```

| Interfaz | URL |
| --- | --- |
| Kestra | <http://localhost:8080> (usuario y clave `KESTRA_BASIC_AUTH_*`) |
| dbt-ui | <http://localhost:5173> |
| JupyterLab (EDA) | <http://127.0.0.1:4041> (con `JUPYTER_TOKEN`) |

## 3. Ingesta Bronze (Kestra)

1. En Kestra: **Flows → Create → Import** →
   `src/main/kestra/flows/bronze_ingestion.yml`. Si editas el YAML, hay que volver a importarlo;
   el `.py` se lee del volumen en cada ejecución.
2. **Execute**, una vez por dataset: primero `failure_labels` (es pequeño y sirve como prueba),
   después `smart_2018` y `smart_2019`.
3. Para hacer backfill de un rango, completa `requested_start_date` y `requested_end_date`.
4. Comprueba el resultado en Snowflake:

```sql
SELECT COUNT(*) FROM S_CDATOS_PSET2.S_CDATOS_PSET2_BRONZE.SSD_FAILURE_LABEL_RAW;
SELECT SOURCE_DATE, COUNT(*) FROM S_CDATOS_PSET2.S_CDATOS_PSET2_BRONZE.SMART_2018_RAW
GROUP BY 1 ORDER BY 1 LIMIT 10;
```

## 4. Silver y Gold (dbt)

```bash
# Probar la conexión
docker compose --env-file src/res/env/.env exec \
  --workdir /workspace/dbt-projects/ssd_failure_prediction dbt-ui-backend \
  /opt/dbt-ui/backend/.venv/bin/dbt debug --profiles-dir /home/dbtui/.dbt

# Construir y testear todo (modelos + tests en orden del DAG)
docker compose --env-file src/res/env/.env exec \
  --workdir /workspace/dbt-projects/ssd_failure_prediction dbt-ui-backend \
  /opt/dbt-ui/backend/.venv/bin/dbt build --profiles-dir /home/dbtui/.dbt
```

Variantes útiles (se agregan al final del comando anterior):

| Objetivo | Argumentos |
| --- | --- |
| Solo Silver | `--select tag:silver` |
| Solo Gold | `--select tag:gold` |
| Un modelo y todo lo que depende de él | `--select smart_2018+` |
| Reprocesar Silver desde cero (tras cambiar la lógica) | `--full-refresh --select tag:silver` |
| Solo tests | cambiar `build` por `test` |
| Documentación navegable con el DAG | `docs generate` |

## 5. Spark y OBT

### 5.1 Verificación de conectividad y pruebas unitarias de contrato

Antes de correr el job pesado en Snowflake, se pueden verificar los componentes de Spark:

```bash
# 1. Comprobar que el conector Spark-Snowflake está en el classpath (humo local)
docker compose --env-file src/res/env/.env exec spark \
  /opt/spark/bin/spark-submit /opt/spark/jobs/build_obt.py

# 2. Probar conectividad real y credenciales contra Snowflake (SELECT liviano)
docker compose --env-file src/res/env/.env exec spark \
  /opt/spark/bin/spark-submit /opt/spark/jobs/check_snowflake_connection.py

# 3. Correr pruebas unitarias de contrato de la OBT (esquema, ventanas y paridad sin Snowflake)
docker compose --env-file src/res/env/.env run --rm --no-deps \
  -v .:/workspace:ro snowpark-connect \
  python /workspace/src/test/python/spark/test_build_mc1_obt_contract.py
```

### 5.2 Construcción y publicación de la OBT (Snowpark Connect)

```bash
docker compose --env-file src/res/env/.env build snowpark-connect

# a) Solo validar contratos en Snowflake (no escribe tablas ni auditoría)
docker compose --env-file src/res/env/.env run --rm --no-deps snowpark-connect \
  python /opt/spark/jobs/build_mc1_obt.py \
  --start-date 2018-01-01 --end-date 2018-01-31 --ssd-limit 100 --validate-only

# b) Corrida de desarrollo acotada: escribe y valida stages, NO publica a las tablas finales
docker compose --env-file src/res/env/.env run --rm --no-deps snowpark-connect \
  python /opt/spark/jobs/build_mc1_obt.py \
  --start-date 2018-01-01 --end-date 2018-01-31 --ssd-limit 100

# c) Producción: rango completo, valida etapas y publica OBT_MC1_RN / _R / _N mediante CLONE
docker compose --env-file src/res/env/.env run --rm --no-deps snowpark-connect \
  python /opt/spark/jobs/build_mc1_obt.py
```

Comprueba que la OBT tiene el mismo número de filas que el hecho Gold y revisa la auditoría:

```sql
SELECT (SELECT COUNT(*) FROM S_CDATOS_PSET2.S_CDATOS_PSET2_OBT.OBT_MC1_RN)      AS obt,
       (SELECT COUNT(*) FROM S_CDATOS_PSET2.S_CDATOS_PSET2_GOLD.FCT_SMART_DAILY_MC1) AS gold;

SELECT RUN_STATUS, MC1_OBT_ROW_COUNT, POSITIVE_COUNT, NEGATIVE_COUNT, CENSORED_COUNT, ERROR_MESSAGE
FROM S_CDATOS_PSET2.S_CDATOS_PSET2_OBT.OBT_MC1_RUN_AUDIT ORDER BY COMPLETED_AT_UTC DESC;
```

## 6. Apagar

```bash
docker compose down       # conserva volúmenes (historial de Kestra)
docker compose down -v    # BORRA también el estado de Kestra y de dbt-ui
```
