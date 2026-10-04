# 3. Calidad de datos

[← Ingesta](02_ingesta_kestra.md) · [Índice](README.md) · **Calidad de datos** · [Siguiente: Modelado →](04_modelado_dbt.md)

El análisis se hizo **antes** de transformar, con los notebooks de EDA sobre Silver base
(`spark/notebooks/datagrip/*.ipynb`). Los resultados agregados están versionados como CSV en
[`ssd_null_analysis_book/exports`](../src/main/python/spark/notebooks/ssd_null_analysis_book/exports)
y en `spark/notebooks/exports`. Todas las cifras de este documento salen de esos archivos.

## Panorama

| Dataset | Filas | Observación principal |
| --- | ---: | --- |
| SMART 2018 | 135.843.663 | Nulos muy estructurados: dependen del modelo de SSD |
| SMART 2019 | 137.268.621 | Misma estructura; un atributo (211) aparece recién en 2019 |
| Etiquetas de falla | 16.305 (todos los modelos; 1.964 en 2018 y 8.546 en 2019 son de MC1) | 0 nulos en todas las columnas |
| MC1 2018 / 2019 | 53.789.530 / 61.484.596 | Población objetivo del modelo predictivo |

## Problemas, evidencia, acción y justificación

| # | Dimensión | Problema | Evidencia | Acción (dónde) | Justificación |
| --- | --- | --- | --- | --- | --- |
| 1 | Completitud | Atributos SMART que nunca tienen valor | **17 atributos (34 columnas)** 100 % nulos en 2018 **y** 2019: `2, 3, 4, 6, 7, 8, 10, 11, 13, 189, 191, 193, 200, 204, 205, 207, 240` | Eliminarlos de las tablas procesadas, pero siguen en Bronze y en Silver base (macro `globally_unavailable_smart_attribute_ids`) | Sin una sola observación no hay base empírica para imputar; no aportan variación al modelo |
| 2 | Consistencia entre años | Atributo presente en un año y ausente en el otro | SMART **211**: 100 % nulo en 2018; en MC1 2019 tiene 2.738 valores (99,995547 % nulo) | **Conservarlo** | Borrarlo solo por la ausencia en 2018 sería decidir sin evidencia; lo decidirá la selección de variables del modelo |
| 3 | Completitud | Filas sin ninguna medición SMART | **490.285 filas en 2018 (0,360919 %)** y **635 en 2019 (0,000463 %)** con las 102 columnas nulas | **Cuarentena** en `INT_AUDIT_SMART_<año>_NO_ATTRIBUTES`, con `audit_reason`; no se borran | No aportan señal, pero se conservan para trazarlas y explicarlas; los tests prueban que limpio + auditoría = original |
| 4 | Completitud | Nulos parciales | 4.705 (2018) y 3.379 (2019) combinaciones exactas de nulos; las filas con 60/62/64/66 columnas nulas son el **97,24 %** y el **98,14 %**; el patrón dominante cubre ≥ 99,4 % de cada modelo (salvo MA1, 72,83 %) | **No imputar** (ni 0, ni media, ni mediana); el nulo queda como `NULL` | El nulo significa "este modelo no reporta el atributo", no "valor 0"; imputar fabricaría señal falsa |
| 5 | Completitud (población MC1) | Atributos que MC1 nunca reporta | **12 atributos** 100 % nulos en MC1 en ambos años: `175, 177, 181, 182, 190, 192, 232, 233, 241, 242, 244, 245` | Quitarlos **solo** del esquema MC1, que queda con **22 atributos (44 columnas)** (macro `mc1_unavailable_smart_attribute_ids`) | No tienen información para la población objetivo; otros modelos sí los reportan, así que siguen en la tabla general |
| 6 | Consistencia / unicidad | `disk_id` repetido entre modelos distintos | **115.017** `disk_id` en 2018 y **113.569** en 2019 aparecen con más de un modelo (hasta 6); 119.213 se cruzan entre años | La clave de negocio es **(`disk_id`, `model_code`)**; en Gold se usa `ssd_key = SHA-256(disk_id \| model_code)` | Usar `disk_id` solo mezclaría la telemetría de discos físicos distintos |
| 7 | Unicidad | Duplicados por SSD y día | **0** grupos duplicados por (`disk_id`, `model`, fecha) y **0** duplicados exactos de telemetría en 2018 y 2019 | No se deduplica; la unicidad se **impone con tests** (`unique_column_combination` en Gold y `validate_source_grain` en Spark) | Deduplicar sin duplicados ocultaría futuros errores de carga; un duplicado nuevo rompe el build |
| 8 | Consistencia | Pares raw/normalizado | **0** patrones donde `r_X` sea nulo y `n_X` no (o al revés) | Test `smart_pair_missingness_matches` en los hechos Gold | Es una regla real del dato: los dos valores del atributo vienen juntos |
| 9 | Precisión / validez de tipo | Todo llega como texto | La fuente es CSV y Bronze guarda texto | `TRY_TO_DECIMAL(..., 38, 6)`, `TRY_TO_NUMBER`, `TRY_TO_DATE('YYYYMMDD')`; `not_null` en las claves | Un valor inválido pasa a `NULL` en vez de romper la carga; el original queda en Bronze; `NUMBER(38,6)` alcanza para contadores raw muy grandes |
| 10 | Validez | Fechas fuera de rango o inconsistentes | Reglas: la fecha de la fila (`ds`) debe caer en el año del archivo e igualar la fecha del nombre del archivo; las fallas deben caer entre 2018-01-01 y 2019-12-31 | Tests singulares `silver_smart_<año>_date_consistency` y `silver_failure_label_date_validity` | Detectan errores de parseo o archivos mal nombrados |
| 11 | Validez (contrato de fuente) | Esquema distinto, filas corruptas, encoding | Contrato de 105 columnas en un orden fijo | `prepare_bronze.py` rechaza el archivo antes de cargar | Es mejor fallar en la ingesta que cargar datos corridos de columna |
| 12 | Completitud (etiquetas) | Nulos en etiquetas | **0** nulos en las 16.305 filas | Sin imputación; `not_null` en `disk_id`, `model_code`, `failure_at` y `failure_date` | Las etiquetas definen el *target*, y un nulo ahí sería grave |
| 13 | Consistencia temporal | La prevalencia de fallas cambia entre años | Tasa de falla de MC1: **0,105071 %** (2018) frente a **0,404121 %** (2019); el patrón dominante de nulos sigue la misma tasa (+0,000172 / −0,000001 puntos porcentuales) | No es limpieza: obliga a validar el modelo con **split temporal** y no aleatorio | Un split aleatorio mezclaría dos prevalencias distintas y sobreestimaría el desempeño |

## Cómo se garantiza que la limpieza no pierde datos

- **Partición sin pérdida:** el test `smart_source_partition_reconciles` compara, con `MINUS`
  en ambos sentidos, Silver base contra limpio `UNION ALL` auditoría. Cero filas de diferencia
  significa que ninguna fila se perdió ni se inventó.
- **Unión sin pérdida:** `smart_year_union_reconciles` verifica que la tabla de todos los años
  sea exactamente 2018 + 2019.
- **Columnas eliminadas de verdad:** `smart_columns_absent` consulta `information_schema` y
  falla si alguna columna eliminada sigue existiendo.
- **Gold sin pérdida:** `gold_row_count_matches_source` exige que cada hecho Gold tenga el mismo
  número de filas que su fuente Silver.

## Qué **no** se concluye

- Que los nulos *causen* o *predigan* fallas: en MC1 el patrón dominante tiene prácticamente la
  misma tasa de falla que la línea base del año.
- La relación numérica exacta entre `r_X` y `n_X`: los exportes agregados no la permiten
  reconstruir.
