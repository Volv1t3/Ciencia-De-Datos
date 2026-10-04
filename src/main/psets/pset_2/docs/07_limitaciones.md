# 7. Limitaciones

[← Batch vs. streaming](06_batch_vs_streaming.md) · [Índice](README.md) · **Limitaciones** · [Siguiente: Ejecución →](08_ejecucion.md)

## Datos

| Limitación | Impacto | Posible solución |
| --- | --- | --- |
| No hay exporte de 2019 equivalente a `smart_2018_mc1_feature_missingness_risk.csv` | No se puede comparar entre años la relación nulo-por-atributo vs. falla | Generar ese exporte con el notebook de 2019 |
| La relación numérica entre `r_X` y `n_X` no se puede reconstruir con los exportes agregados | No se sabe si conviene usar R, N o ambas; por eso se publican las 3 variantes de la OBT | Analizar la relación con datos fila a fila en la etapa de modelado |
| SMART 211 es casi totalmente nulo en MC1 (99,995547 %) | Probablemente no aporta al modelo | Decidir en la selección de variables, no en la limpieza |
| La prevalencia de fallas cambia entre años (0,105 % → 0,404 %) | Un split aleatorio sobreestimaría el desempeño | Validación temporal por meses |
| `DAYS_TO_FAILURE` usa la **primera** falla del SSD | Las fallas posteriores del mismo disco no generan etiquetas propias | Aceptable para "¿cuándo falla por primera vez?"; documentado en la auditoría |
| Los datos son observacionales | No se puede afirmar causalidad (firmware, carga de trabajo y edad son confusores) | Fuera del alcance del pipeline |

## Arquitectura

| Limitación | Impacto | Posible solución |
| --- | --- | --- |
| **Trigger manual** de la ingesta | Alguien debe dejar el ZIP y pulsar *Execute* | Trigger `Schedule` diario o trigger por archivo nuevo en la *landing zone* |
| **Kestra solo orquesta la ingesta** | dbt y Spark se ejecutan con comandos aparte; no hay un DAG único Bronze → OBT | Flow de orquestación que encadene ingesta → `dbt build` → job OBT |
| **Los archivos del stage no se borran** después del `MERGE` | El stage `SMART_ARCHIVE_STAGE` acumula una carpeta por ejecución (costo de almacenamiento) | Tarea `REMOVE @stage/kestra/<execution.id>/` al final del flow (el README de Kestra ya la menciona en el diagrama) |
| Si `upload_day` falla, `cleanup_day` no se ejecuta | Pueden quedar CSV temporales en `src/res/logs/kestra` | Mover la limpieza a un bloque `finally`/`errors` del flow |
| El incremental de Silver usa `INGESTED_AT > max(...)` | No es CDC completo: si cambia la lógica de parseo hay que hacer `--full-refresh` | Documentado; correr `dbt build --full-refresh` tras cambios de lógica |
| La lista de atributos SMART está repetida (ingesta, `smart_attribute_columns`, `smart_attribute_ids`, `SMART_IDS` en Spark) | Agregar un atributo exige tocar varios lugares | Centralizar en una variable de dbt (`vars`) o en un archivo de configuración |
| Gold y Silver procesada se reconstruyen completos en cada `dbt build` | Más cómputo que un incremental | Aceptable con este volumen; se podría pasar a incremental por fecha |
| El healthcheck de `snowpark-connect` solo prueba que los imports funcionan | Un contenedor "sano" puede no tener credenciales válidas | Correr `--validate-only` como prueba de humo |
| `dbt-ui` depende de imágenes de la semana 05 (`Dockerfile.semana05.*`) | Acoplamiento con otro laboratorio | Renombrar o mover las imágenes a pset_2 |
