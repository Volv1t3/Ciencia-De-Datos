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
| Kestra | <http://localhost:8080> (`KESTRA_BASIC_AUTH_USERNAME` **debe ser un email**; clave de 8 caracteres o más, con mayúscula y número) |
| dbt-ui | <http://localhost:5173> |
| JupyterLab (EDA) | <http://127.0.0.1:4041> (con `JUPYTER_TOKEN`) |

## 3. Ingesta Bronze (Kestra)

Los flows se importan solos: el servicio `kestra-flow-sync` los sube a Kestra al levantar
Docker. Si editas un `.yml`, vuelve a sincronizar con:

```bash
docker compose --env-file src/res/env/.env up kestra-flow-sync
```

Los `.py` (`prepare_bronze.py`, `docker_exec.py`) se leen del volumen en cada ejecución, así que
no hace falta sincronizar nada después de editarlos.

**Opción A: pipeline completo de una vez.** Ejecuta `ssd_pipeline` con
`datasets = [failure_labels, smart_2018, smart_2019]`. Hace la ingesta, después `dbt build` y
por último la OBT; si falla una etapa, no se ejecuta la siguiente.

**Opción B: por partes.**

1. Ejecuta `bronze_ingestion` una vez por dataset: primero `failure_labels` (es pequeño y sirve
   como prueba), después `smart_2018` y `smart_2019`.
2. Para un rango, completa `requested_start_date` y `requested_end_date`. Para un backfill día
   por día, usa *Triggers → Backfill executions* en `bronze_daily_schedule` (ver
   [Ingesta](02_ingesta_kestra.md#backfill-de-datos-históricos)).
3. Comprueba el resultado en Snowflake:

```sql
SELECT COUNT(*) FROM S_CDATOS_PSET2.S_CDATOS_PSET2_BRONZE.SSD_FAILURE_LABEL_RAW;
SELECT SOURCE_DATE, COUNT(*) FROM S_CDATOS_PSET2.S_CDATOS_PSET2_BRONZE.SMART_2018_RAW
GROUP BY 1 ORDER BY 1 LIMIT 10;
```

## 4. Silver y Gold (dbt)

Desde Kestra: ejecuta `ssd_pipeline` sin datasets. También corre solo cada lunes a las 06:00.
Desde la terminal:

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

## 5. OBT (Spark con Snowpark Connect)

```bash
docker compose --env-file src/res/env/.env build snowpark-connect

# a) Solo validar contratos (no escribe nada)
docker compose --env-file src/res/env/.env run --rm --no-deps snowpark-connect \
  python /opt/spark/jobs/build_mc1_obt.py \
  --start-date 2018-01-01 --end-date 2018-01-31 --ssd-limit 100 --validate-only

# b) Corrida de desarrollo: escribe y valida stages, NO publica
docker compose --env-file src/res/env/.env run --rm --no-deps snowpark-connect \
  python /opt/spark/jobs/build_mc1_obt.py \
  --start-date 2018-01-01 --end-date 2018-01-31 --ssd-limit 100

# c) Producción: todo el rango, publica OBT_MC1_RN / _R / _N
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
