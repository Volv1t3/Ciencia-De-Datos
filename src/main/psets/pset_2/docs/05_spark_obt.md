# 5. Spark y OBT

[← Modelado](04_modelado_dbt.md) · [Índice](README.md) · **Spark y OBT** · [Siguiente: Batch vs. streaming →](06_batch_vs_streaming.md)

Código: [`src/main/python/spark/jobs/build_mc1_obt.py`](../src/main/python/spark/jobs/build_mc1_obt.py)
(contenedor `snowpark-connect`).

## Cómo se construyó

El job usa la **API de DataFrames de PySpark** a través de **Snowpark Connect**. El código es
Spark, pero el plan se ejecuta dentro del warehouse de Snowflake
(`SNOWFLAKE_WAREHOUSE_HIGH_COMPUTE`). Así se evita descargar unos 115 millones de filas MC1 a un
contenedor local.

```mermaid
flowchart TD
    G["Gold: FCT_SMART_DAILY_MC1 · FCT_FAILURE_EVENT_MC1 · DIM_SSD · DIM_DATE"] --> V1["Validar contrato<br/>44 columnas SMART numéricas"]
    V1 --> V2["Validar grain de entrada<br/>filas = claves distintas = pares (SSD, fecha)"]
    V2 --> V3["Integridad referencial<br/>dimensiones sin duplicados, sin huérfanos"]
    V3 --> J["Join con DIM_SSD y DIM_DATE<br/>conteo después == conteo antes"]
    J --> L["Etiquetas: primera falla, último día visto, TARGET_30D"]
    J --> W["Ventanas 7/14/30 días: media, conteo, tendencia"]
    L --> RN["OBT_MC1_RN (847 cols)"]
    W --> RN
    RN --> ST[("Stage OBT_MC1_RN__RUN_sufijo")]
    ST --> VS["Validar lo PERSISTIDO"]
    VS --> R[("Stage R, 429 cols")] & N[("Stage N, 429 cols")]
    R & N --> P["Paridad R/N vs RN"]
    P --> C["CREATE OR REPLACE ... CLONE<br/>OBT_MC1_RN / _R / _N"]
    C --> AU[("OBT_MC1_RUN_AUDIT")]
```

## Grain

**Una fila = un SSD MC1 en un día de observación.** Es el mismo grain que
`FCT_SMART_DAILY_MC1`, así que la OBT debe tener **exactamente** el mismo número de filas que el
hecho Gold (dentro del rango pedido).

## Columnas

| Bloque | Columnas | Detalle |
| --- | ---: | --- |
| Identidad | 6 | `SMART_DAILY_KEY`, `SSD_KEY`, `DISK_ID`, `MODEL_CODE`, `OBSERVATION_DATE_KEY`, `OBSERVATION_DATE` |
| Features | 836 (RN) / 418 (R o N) | 22 atributos × 19 columnas × representación (R raw, N normalizada) |
| Etiqueta | 5 | `FIRST_FAILURE_DATE`, `LAST_SEEN_DATE`, `DAYS_TO_FAILURE`, `LABEL_STATUS`, `TARGET_30D` |

Las 19 columnas por atributo, por ejemplo para `R_5` (sectores reasignados):

| Columna | Significado |
| --- | --- |
| `R_5` | valor del día |
| `R_5_MEAN_{7,14,30}D` | media móvil de la ventana actual (hoy y los N−1 días previos) |
| `R_5_COUNT_{7,14,30}D` | días con valor no nulo en la ventana actual |
| `R_5_PREV_COUNT_{7,14,30}D` | lo mismo en la ventana anterior (los N días previos a la actual) |
| `R_5_MEAN_VALID_{7,14,30}D` | 1 si la media actual existe |
| `R_5_MEAN_SHIFT_{7,14,30}D` | media actual − media anterior: **tendencia o degradación** |
| `R_5_MEAN_SHIFT_VALID_{7,14,30}D` | 1 si las dos ventanas tienen datos |

- Las ventanas son por **rango de días** (`rangeBetween` sobre el número de día), no por número
  de filas. Si un disco no reporta algunos días, la ventana sigue cubriendo el período calendario
  correcto.
- `avg` y `count` ignoran los `NULL`. Se mantiene la decisión de Silver de **no imputar**.

## Etiqueta (`TARGET_30D`)

| `LABEL_STATUS` | Condición | `TARGET_30D` |
| --- | --- | --- |
| `POST_FAILURE` | observación posterior a la primera falla | `NULL` (no se usa) |
| `SAME_DAY_FAILURE` | observación el mismo día de la falla | `NULL` (no da anticipación) |
| `POSITIVE` | la falla ocurre entre 1 y 30 días después | **1** |
| `NEGATIVE` | el SSD sigue observado 30 días o más después y no falló en ese lapso | **0** |
| `CENSORED` | no hay suficiente futuro observado para saberlo | `NULL` (no se usa) |

`CENSORED` evita etiquetar como "sano" a un disco del que simplemente se dejó de tener datos. Si
un SSD tiene varias fallas, se usa la **primera**, y el job lo deja registrado en el log y en la
auditoría (`MULTIPLE_FAILURE_SSD_COUNT`).

## Validación de los joins y del número de observaciones

| Validación | Qué evita |
| --- | --- |
| `validate_source_grain`: filas = `SMART_DAILY_KEY` distintas = pares `(SSD_KEY, fecha)` distintos | Construir sobre un Gold con duplicados |
| `validate_dimension_relationships`: `DIM_SSD` y `DIM_DATE` sin claves repetidas, sin hechos huérfanos (`left_anti`) | Que un join **multiplique** filas por claves duplicadas o **pierda** filas sin pareja |
| `build_base_observations`: **conteo después del join == conteo antes** | Es la prueba directa de que el enriquecimiento no cambió el número de observaciones |
| `validate_rn_stage` sobre la tabla **ya escrita**: filas = filas de Gold, sin claves ni pares duplicados, conteos de ventana entre 0 y N, medias y tendencias coherentes, etiquetas recalculadas iguales, solo MC1, sin `DISK_ID` ni `LAST_SEEN_DATE` nulos | Errores que solo aparecen al materializar |
| `validate_variant_stage` + `validate_variant_parity` (`exceptAll` en ambos sentidos) | Que R o N tengan filas distintas de RN |

Si alguna validación falla, el job **no publica**: las tablas finales anteriores quedan intactas,
se escribe una fila `FAILED_VALIDATION` en `OBT_MC1_RUN_AUDIT` y el proceso termina con código 2.

**Publicación con `CLONE`:** las tablas *stage* validadas se copian a las finales con
`CREATE OR REPLACE TABLE ... CLONE`. Es una copia *zero-copy* e instantánea, así que nadie lee
nunca una OBT a medio escribir.

### Pruebas unitarias y de contrato locales (sin Snowflake)

Para validar la lógica de transformación, el cálculo de ventanas y la integridad de esquemas sin
incurrir en costos ni depender de la red, se implementó una suite de pruebas unitarias en
[`src/test/python/spark/test_build_mc1_obt_contract.py`](../src/test/python/spark/test_build_mc1_obt_contract.py):

- **Motor local:** corre en PySpark con `local[1]`, completamente desconectado de Snowflake.
- **Fixture sintético:** genera 2 observaciones en días consecutivos con falla programada en el día
  20 y seguimiento hasta el día 35.
- **Contrato de esquemas:** verifica que `OBT_MC1_RN` tenga exactamente **847 columnas** y que las
  variantes `OBT_MC1_R` y `OBT_MC1_N` tengan **429 columnas** cada una, sin colisiones de nombres.
- **Ventanas por calendario:** prueba que `rangeBetween` respete el avance por días calendario.
- **Compuertas de auditoría:** ejecuta `validate_rn_stage` sobre el DataFrame de prueba y verifica
  que los contadores de inconsistencias en `AuditMetrics` sean estrictamente cero.
- **Paridad relacional:** proyecta las variantes R y N y ejecuta `validate_variant_parity`
  (usando `exceptAll` en ambos sentidos) para garantizar que ninguna variante diverja de RN.

Ejecución de las pruebas locales:
```bash
docker compose --env-file src/res/env/.env run --rm --no-deps \
  -v .:/workspace:ro snowpark-connect \
  python /workspace/src/test/python/spark/test_build_mc1_obt_contract.py
```

## Arquitectura dual de Spark en el proyecto

El proyecto utiliza dos entornos de Apache Spark especializados y desacoplados:

| Componente | Servicio Docker | Versión de Spark | Conector | Propósito |
| --- | --- | --- | --- | --- |
| **EDA Interactivo** | `spark` | 4.0.4 (Scala 2.13, Java 21) | `spark-snowflake` 3.2.2 (JDBC) | Notebooks en JupyterLab (puerto 4041) con `autopushdown=on` para consultas analíticas |
| **Construcción OBT** | `snowpark-connect` | 3.5.6 (Scala 2.12, Java 17) | `snowpark-connect` 1.44.0 | Job batch de producción (`build_mc1_obt.py`) ejecutado remotamente en Snowflake |

### Scripts auxiliares y librerías compartidas

- [`src/main/python/spark/lib/snowflake_io.py`](../src/main/python/spark/lib/snowflake_io.py): helper
  compartido para el conector clásico `spark-snowflake` que abstrae `create_spark_session`,
  `read_snowflake_table` y `read_snowflake_query` con configuración automática de UTC y pushdown.
- [`src/main/python/spark/jobs/check_snowflake_connection.py`](../src/main/python/spark/jobs/check_snowflake_connection.py):
  script de diagnóstico que ejecuta un `SELECT CURRENT_ACCOUNT(), ...` en Snowflake para verificar
  credenciales y conectividad de red.
- [`src/main/python/spark/jobs/build_obt.py`](../src/main/python/spark/jobs/build_obt.py): smoke test
  ligero para comprobar que la JVM de Spark 4 registra correctamente el data source de Snowflake.

## Por qué tres variantes (RN, R, N)

`R` (raw) y `N` (normalizado por el fabricante) son dos representaciones del mismo atributo. Su
relación numérica no se pudo reconstruir con los exportes agregados del EDA (ver
[limitaciones](07_limitaciones.md)). Tener las tres variantes permite comparar en la etapa de
modelado si conviene usar una, la otra o ambas, sin recalcular ventanas: R y N son proyecciones
de la misma tabla RN.

## Star schema frente a OBT en este proyecto

| Uso | Estructura | Por qué |
| --- | --- | --- |
| Análisis exploratorio y reportes (fallas por mes, comparación entre modelos, tasa de falla por año) | **Star schema (Gold)** | Las dimensiones conformadas permiten agregar por cualquier eje sin duplicar datos; los facts siguen siendo reutilizables |
| Entrenamiento y scoring del modelo predictivo MC1 | **OBT** | El modelo necesita una fila por observación con todas las features y la etiqueta ya calculadas, sin joins ni ventanas en el momento de entrenar |
| Agregar un modelo de SSD nuevo o cambiar la definición de la etiqueta | Se cambia en Gold/Spark y se regenera la OBT | La OBT es un derivado desechable: se reconstruye desde Gold |

