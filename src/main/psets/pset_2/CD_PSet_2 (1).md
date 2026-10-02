# PSet #2: Pipeline de Datos End-to-End

## Ciencia de Datos: end-to-end

En el PSet #1 definiste un proyecto de ciencia de datos, justificaste su viabilidad y determinaste qué datos necesitas. En este PSet debes construir la tubería de datos que alimentará ese proyecto. El objetivo es implementar un pipeline ELT reproducible desde la fuente hasta Snowflake utilizando Kestra, dbt y Spark. Al finalizar debes tener: Fuente → Kestra → Bronze → dbt → Silver → dbt → Gold → Spark → OBT Este PSet termina con datos limpios, modelados y preparados para las siguientes etapas del proyecto.

1. Arquitectura

Diseña e implementa una arquitectura en la que Snowflake sea el destino de los datos. Kestra debe encargarse de la ingesta y de la orquestación. dbt debe realizar las transformaciones y construir las capas Silver y Gold. Spark debe utilizarse para construir una One Big Table (OBT) a partir de los datos procesados. La infraestructura local debe definirse en el archivo docker-compose.yml e incluir, como mínimo, Kestra y Spark. Puedes utilizar dbt Core en Docker o en dbt Cloud. Incluye un diagrama de la arquitectura implementada.

2. Ingesta con Kestra

Construye un flow en Kestra que extraiga los datos de la fuente de tu proyecto y los cargue en Snowflake. La ingesta debe poder realizarse sin realizar cargas manuales. Debes implementar y poder explicar:

- Trigger y frecuencia de ejecución
- Manejo de errores y retries
- Backfill de datos históricos. Los datos originales deben almacenarse en una capa Bronze, conservando el dato original.

3. Calidad y limpieza de datos

Analiza la calidad de los datos antes de transformarlos. Como mínimo, debes revisar la completitud, la precisión, la consistencia y la validez. No basta con limpiar los datos. Debes justificar las decisiones tomadas. Para los problemas principales documenta: 


| Problema | Evidencia | Acción | Justificación | IDs |
|----------|-----------|--------|---------------|-----|
| 120 | Conservar registro más | Representa la última duplicados registros | reciente actualización | Las transformaciones de limpieza deben realizarse con dbt para generar la capa Silver. |

4. Modelado dimensional con dbt

A partir de Silver construye una capa Gold siguiendo un star schema. Debes definir claramente el grain de la tabla de hechos y construir las dimensiones necesarias para tu proyecto. Incluye un diagrama del modelo dimensional. Tu proyecto dbt debe utilizar source() y ref() para gestionar las dependencias entre modelos e incluir pruebas de calidad relevantes, como not_null, unique y relationships. ¡Los tests deben validar reglas reales de tus datos y del problema de negocio!

5. OBT con Spark

Utiliza Spark para construir una One Big Table a partir de las tablas Gold. La OBT debe integrar la información necesaria para alimentar el futuro modelo de ciencia de datos. Debes definir su grain y validar que los joins no hayan introducido duplicados ni modificado incorrectamente el número de observaciones.

El resultado debe almacenarse nuevamente en Snowflake. En el documento se explica brevemente cuándo utilizarías el star schema y cuándo utilizarías la OBT en tu proyecto.

6. Batch vs. streaming

Tu pipeline se implementará mediante el procesamiento por lotes. Justifica esta decisión considerando la frecuencia con la que se generan los datos y la latencia que realmente requiere el problema de negocio. Explica también qué tendría que cambiar en el caso de uso para justificar una arquitectura de streaming. No debes implementar streaming.

# Documento técnico

Entrega un documento técnico de hasta 6 páginas, sin contar anexos. El documento debe explicar tu implementación y las decisiones tomadas, no definir teóricamente qué son Kestra, dbt, Spark o Snowflake. Debe contener:

1. Arquitectura

Diagrama de la infraestructura y explicación del flujo de datos.

2. Ingesta

Fuente, frecuencia, granularidad, proceso de carga, retries y estrategia de backfill.

3. Data Quality

Problemas encontrados, evidencia y decisiones de limpieza. Utiliza métricas concretas. Por ejemplo: El 4.3% de los registros no tenía customer_id.

4. Transformaciones y modelado

Explica la construcción de Bronze, Silver y Gold. Define el grain de la tabla de hechos y presenta el star schema.

5. Spark y OBT

Explica cómo construiste la OBT, cuál es su grain y cómo validaste los joins.

6. Batch vs. streaming

Justifica por qué batch es suficiente actualmente y bajo qué condiciones utilizarías streaming.

7. Limitaciones

Documenta problemas de datos o de arquitectura que aún no hayan sido resueltos.

# Repositorio

El repositorio debe permitir reproducir el pipeline. Una estructura posible es: pset_2/ ├── docker-compose.yml ├── .env.example ├── README.md ├── kestra/ ├── dbt/ ├── spark/ └── docs/ El README.md debe explicar cómo levantar la infraestructura y ejecutar Kestra, dbt y Spark. ¡No subas contraseñas, tokens ni credenciales al repositorio!

# Entregables

Entregable Requisito Repositorio Código del pipeline y README Documento técnico Máximo 6 páginas Arquitectura Diagrama de infraestructura Snowflake Bronze, Silver, Gold y OBT Kestra Ingesta, retry y backfill funcionando dbt Limpieza, star schema y tests Spark Construcción de la OBT Nombre del documento: PSet2_memo_<apellido_1>_<apellido_2>.pdf

# Rúbrica

Criterio Peso Kestra, ingesta y orquestación 15% Calidad y limpieza de datos 20% dbt y arquitectura Bronze/Silver/Gold 15% Star schema 15% Spark y OBT 15% Infraestructura y reproducibilidad 10% Documento técnico y decisiones de 10% arquitectura Total 100%
