# Plataforma Spark

[Inicio del proyecto](../../../../README.md) · **Plataforma Spark** · [Jobs](jobs/README.md) · [Notebooks](notebooks/README.md) · [Librería compartida](lib/README.md) · [Imágenes Docker](../../../res/docker/custom-images/README.md)

Este directorio contiene el código fuente en PySpark y Snowpark Connect para la plataforma de predicción de fallas de discos SSD. Apache Spark se utiliza en dos capacidades técnicas especializadas dentro de este proyecto:

1. **Análisis exploratorio interactivo (EDA):** Se ejecuta dentro del contenedor `spark` utilizando Apache Spark 4.0.4 junto con el conector clásico `spark-snowflake` para consultar las tablas de la capa Silver en Snowflake con traducción automática a SQL (*pushdown*), calculando distribuciones estadísticas y generando evidencia empírica de datos nulos.
2. **Ingeniería de variables y construcción de la OBT (One Big Table) en producción:** Se ejecuta dentro del contenedor `snowpark-connect` utilizando Apache Spark 3.5.6 con Snowpark Connect para leer el esquema en estrella de la capa Gold, calcular ventanas temporales móviles de 7, 14 y 30 días, etiquetar los horizontes de predicción y publicar los conjuntos analíticos finales validados.

> **Documentación en español:** Consulte [`docs/05_spark_obt.md`](../../../../docs/05_spark_obt.md) para la arquitectura de la OBT y [`docs/10_notebooks.md`](../../../../docs/10_notebooks.md) para el recorrido sección por sección del análisis exploratorio.

---

## Dualidad arquitectónica: EDA frente a OBT de producción

La plataforma separa deliberadamente la exploración interactiva en notebooks de la generación de datos para entrenamiento de modelos productivos en dos entornos de contenedores independientes:

```mermaid
flowchart TD
    subgraph Almacenamiento["Warehouse en Snowflake"]
        S[(Capa Silver:<br/>SMART_2018 / 2019)]
        G[(Capa Gold:<br/>FCT_SMART_DAILY_MC1<br/>DIM_SSD, DIM_DATE)]
        O[(Capa OBT:<br/>OBT_MC1_RN / R / N)]
    end

    subgraph EDA["Entorno Interactivo EDA (Servicio: spark)"]
        J[JupyterLab / DataGrip<br/>Puerto 4041]
        K[Kernel PySpark 4 + Snowflake]
        IO[Helper snowflake_io.py]
        SC[Conector clásico spark-snowflake 3.2.2]
        J --> K --> IO --> SC
    end

    subgraph PROD["Entorno OBT Producción (Servicio: snowpark-connect)"]
        JOB[build_mc1_obt.py]
        SPC[Snowpark Connect 1.44.0]
        SPK[API de DataFrames PySpark 3.5.6]
        JOB --> SPK --> SPC
    end

    subgraph PRUEBAS["Pruebas Unitarias de Contrato"]
        TST[test_build_mc1_obt_contract.py]
        LSPK[Spark local en local[1]]
        TST --> LSPK
    end

    SC <-->|autopushdown=on<br/>resultados agregados| S
    SPC -->|lee hechos y dimensiones Gold| G
    SPC -->|escribe stages y clones validados| O
```

### Matriz comparativa de entornos

| Dimensión | EDA Interactivo (`spark`) | OBT de Producción (`snowpark-connect`) |
| --- | --- | --- |
| **Imagen base Docker** | `Dockerfile.pset2.spark` (Ubuntu base) | `Dockerfile.pset2.snowpark` (Debian Bookworm) |
| **Versión de Apache Spark** | 4.0.4 (Scala 2.13, Java 21) | 3.5.6 (Scala 2.12, Java 17) |
| **Versión de PySpark** | 4.0.4 | 3.5.6 |
| **Interfaz con Snowflake** | `net.snowflake.spark.snowflake` 3.2.2 + JDBC 4.0.2 | `snowpark-connect` 1.44.0 |
| **Estilo de interfaz** | JupyterLab interactivo (`:4041`), kernel remoto para DataGrip | Línea de comandos headless por lotes (`docker compose exec/run`) |
| **Patrón de cómputo** | Consultas SQL con `autopushdown="on"` ejecutadas en Snowflake | Transformaciones en PySpark evaluadas directamente en el warehouse |
| **Consumo de memoria** | Heap de JVM limitado a 2 GB (`SPARK_DRIVER_MEMORY=2g`) | Memoria local mínima; el cómputo pesado ocurre en Snowflake |
| **Artefacto principal** | `datagrip/*.ipynb`, reportes CSV en `exports/` | `jobs/build_mc1_obt.py`, tablas en esquema `S_CDATOS_PSET2_OBT` |

---

## Estructura del directorio

```text
src/main/python/spark/
├── README.md                           # Este documento (visión general de la plataforma Spark)
├── jobs/                               # Trabajos batch ejecutables y scripts de diagnóstico
│   ├── README.md                       # Especificación técnica del job y guía de ejecución
│   ├── build_mc1_obt.py                # Constructor canonical de la OBT MC1 (847 columnas)
│   ├── build_obt.py                    # Smoke test local para verificar la JVM y el conector clásico
│   └── check_snowflake_connection.py   # Script de prueba de consulta en vivo en Snowflake
├── lib/                                # Librerías auxiliares compartidas
│   ├── README.md                       # Documentación técnica del helper snowflake_io.py
│   └── snowflake_io.py                 # Lectura de DataFrames perezosos con autopushdown
└── notebooks/                          # Análisis exploratorio y evidencia empírica
    ├── README.md                       # Instrucciones de configuración para Jupyter y DataGrip
    ├── build_eda_smart2018_notebook.py # Generador de notebooks para SMART 2018 y 2019
    ├── build_eda_failure_labels_notebook.py # Generador de notebook para etiquetas de fallas
    ├── datagrip/                       # Notebooks interactivos generados (.ipynb)
    ├── exports/                        # Archivos CSV de evidencia producidos por las consultas EDA
    └── ssd_null_analysis_book/         # Paquete autocontenido y reproducible del análisis de nulos
```

Ubicaciones relacionadas de configuración y pruebas:

- **Pruebas unitarias locales:** [`src/test/python/spark/`](../../../../src/test/python/spark/) contiene `test_build_mc1_obt_contract.py`.
- **Configuración de Spark:** [`src/res/config/spark/`](../../../../src/res/config/spark/) contiene `spark-defaults.conf`, `spark-env.sh`, `log4j2.properties` y `requirements.txt`.
- **Definiciones Docker:** [`src/res/docker/custom-images/`](../../../../src/res/docker/custom-images/) contiene `Dockerfile.pset2.spark` y `Dockerfile.pset2.snowpark`.

---

## Flujos de trabajo y comandos principales

### 1. Verificar conectividad con Snowflake y la JVM local

Verificar que la JVM de Spark reconozca el conector de Snowflake en el classpath:

```bash
docker compose --env-file src/res/env/.env exec spark \
  /opt/spark/bin/spark-submit /opt/spark/jobs/build_obt.py
```

Ejecutar una consulta en vivo en Snowflake para comprobar credenciales, rol y warehouse:

```bash
docker compose --env-file src/res/env/.env exec spark \
  /opt/spark/bin/spark-submit /opt/spark/jobs/check_snowflake_connection.py
```

### 2. Ejecutar las pruebas unitarias de contrato

Correr la suite de pruebas local sin conexión a Snowflake ni consumo de créditos:

```bash
docker compose --env-file src/res/env/.env run --rm --no-deps \
  -v .:/workspace:ro snowpark-connect \
  python /workspace/src/test/python/spark/test_build_mc1_obt_contract.py
```

### 3. Iniciar el entorno interactivo de notebooks (EDA)

Levantar el contenedor interactivo de Spark:

```bash
docker compose --env-file src/res/env/.env up -d spark
```

Abrir JupyterLab en `http://127.0.0.1:4041` utilizando el token definido en `JUPYTER_TOKEN` del archivo `.env`. Seleccionar siempre el kernel **`PySpark 4 + Snowflake`**.

Para regenerar los notebooks interactivos a partir de sus generadores en Python:

```bash
python3 src/main/python/spark/notebooks/build_eda_smart2018_notebook.py --year 2018
python3 src/main/python/spark/notebooks/build_eda_smart2018_notebook.py --year 2019
python3 src/main/python/spark/notebooks/build_eda_failure_labels_notebook.py
```

### 4. Construir y publicar la OBT de MC1

Validar el contrato relacional en Snowflake sobre una muestra acotada sin mutar tablas:

```bash
docker compose --env-file src/res/env/.env run --rm --no-deps snowpark-connect \
  python /opt/spark/jobs/build_mc1_obt.py \
  --start-date 2018-01-01 --end-date 2018-01-31 --ssd-limit 100 --validate-only
```

Ejecutar la corrida completa de producción y publicar las tablas definitivas mediante zero-copy clone:

```bash
docker compose --env-file src/res/env/.env run --rm --no-deps snowpark-connect \
  python /opt/spark/jobs/build_mc1_obt.py
```

---

## Políticas de seguridad y aislamiento de credenciales

1. **Cero credenciales en código:** Ningún archivo Python ni imagen Docker almacena contraseñas o tokens.
2. **Inyección por entorno:** Todos los parámetros (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_WAREHOUSE_HIGH_COMPUTE`) se leen exclusivamente del archivo `.env` del host.
3. **Sin fugas en logs:** Los scripts auxiliares y loggers sanitizan las cadenas de conexión para evitar registrar secretos en consola o disco.
