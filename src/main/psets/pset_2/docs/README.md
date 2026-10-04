# Documentación del PSet 2 (español)

Pipeline ELT de punta a punta para predecir fallas de SSD a partir de la telemetría SMART de
Alibaba (Tianchi, 2018–2019):

```text
ZIPs Tianchi → Kestra → Bronze → dbt → Silver → dbt → Gold (star schema) → Spark → OBT
```

Esta carpeta explica **qué se implementó y por qué**, en el mismo orden que el documento
técnico que pide el PSet. La documentación técnica detallada en inglés que escribió el equipo
sigue en los README de cada carpeta; aquí se enlaza donde corresponde.

| # | Documento | Contenido |
| --- | --- | --- |
| 1 | [Arquitectura](01_arquitectura.md) | Diagrama de infraestructura, servicios de Docker y flujo de datos |
| 2 | [Ingesta con Kestra](02_ingesta_kestra.md) | Fuente, granularidad, trigger, carga, retries, backfill e idempotencia |
| 3 | [Calidad de datos](03_calidad_de_datos.md) | Problemas encontrados, evidencia con métricas, acción y justificación |
| 4 | [Transformaciones y modelado](04_modelado_dbt.md) | Bronze, Silver y Gold; grain; star schema; tests de dbt |
| 5 | [Spark y OBT](05_spark_obt.md) | Construcción de la OBT, grain, etiqueta, features y validación de joins |
| 6 | [Batch vs. streaming](06_batch_vs_streaming.md) | Por qué batch y qué cambiaría para justificar streaming |
| 7 | [Limitaciones](07_limitaciones.md) | Problemas de datos y de arquitectura no resueltos |
| 8 | [Ejecución paso a paso](08_ejecucion.md) | Cómo levantar la infraestructura y correr Kestra, dbt y Spark |
| 9 | [Mapa del código](09_mapa_del_codigo.md) | Qué hace cada archivo y dónde tocar para hacer cambios típicos |
| 10 | [Notebooks](10_notebooks.md) | EDA en Spark (consultas con pushdown, sección por sección) y libro de análisis de nulos (notebook por notebook) |

## Resumen en una página

- **Problema:** predecir si un SSD del modelo **MC1** fallará en los próximos **30 días** a partir
  de su telemetría SMART diaria.
- **Fuente:** tres ZIP de Tianchi: `smartlog2018ssd.zip` (135.843.663 filas),
  `smartlog2019ssd.zip` (137.268.621 filas) y `ssd_failure_label.csv.zip` (16.305 fallas).
  Un CSV por día con 105 columnas: `disk_id`, `ds` (fecha), `model` y 51 atributos SMART, cada
  uno en versión *raw* (`r_X`) y *normalizada* (`n_X`).
- **Bronze (Kestra):** cada fila del CSV se guarda tal cual (texto dentro de un `VARIANT`) con
  linaje (`archivo`, `fila`, `hash`). La carga es idempotente (`MERGE`) y tiene reintentos,
  backfill por rango de fechas y una reconciliación final.
- **Silver (dbt):** tipado con `TRY_TO_*`. Se eliminan los 17 atributos SMART que están 100 %
  vacíos, las filas sin ninguna medición pasan a cuarentena (no se borran) y los nulos parciales
  **no se imputan**. Hay además un subconjunto MC1 con 44 columnas.
- **Gold (dbt):** star schema con `DIM_SSD`, `DIM_DATE`, dos hechos SMART diarios (todos los
  modelos / MC1) y dos hechos de eventos de falla. Grain del hecho principal: **un SSD en un día**.
- **OBT (Spark con Snowpark Connect):** una fila por SSD MC1 y día. Incluye ventanas móviles de
  7/14/30 días, la tendencia y la etiqueta `TARGET_30D`. Se publica en 3 variantes (RN, R y N)
  solo si pasan todas las validaciones, y deja una fila de auditoría por corrida.
- **Batch:** las fallas se gestionan con días de anticipación y la fuente se publica en archivos
  históricos. No hay ningún evento que exija latencia de segundos.
