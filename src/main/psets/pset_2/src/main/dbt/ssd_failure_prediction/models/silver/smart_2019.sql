--? SILVER BASE 2019: Bronze (VARIANT con texto) -> tabla tipada, 1 fila por fila de Bronze.
--? Aqui NO se filtra ni se imputa nada: solo se tipan columnas y se conserva el linaje.
--? La limpieza (columnas vacias, filas sin mediciones, MC1) ocurre en los modelos de silver/smart/.
--? Materializacion INCREMENTAL con estrategia MERGE (heredada de dbt_project.yml para silver/):
--?   - 1a corrida (o dbt run --full-refresh): crea la tabla con TODO Bronze.
--?   - corridas siguientes: solo procesa filas de Bronze con INGESTED_AT mas nuevo que lo ya cargado
--?     y hace MERGE por unique_key (silver_record_id): actualiza si existe, inserta si no.
--? on_schema_change='fail': si cambian las columnas del modelo, dbt falla en vez de alterar la tabla
--? en silencio -> hay que correr --full-refresh a proposito.
--? alias = nombre fisico de la tabla en Snowflake; tags permiten correr subconjuntos (dbt run -s tag:2018).
{{
    config(
        alias='SMART_2019',
        incremental_strategy='merge',
        unique_key='silver_record_id',
        on_schema_change='fail',
        tags=['silver', 'smart', '2019']
    )
}}

--? source('bronze', ...) = tabla declarada en models/sources/sources.yml (cargada por Kestra).
--? Usar source() en vez del nombre fijo da linaje en el DAG de dbt y permite cambiar el esquema en un solo lugar.
with bronze as (
    select *
    from {{ source('bronze', 'smart_2019_raw') }}
    --? Solo en modo incremental: filtrar filas de Bronze nuevas o actualizadas desde la ultima corrida.
    --? this = la propia tabla Silver ya existente. coalesce cubre el caso de tabla vacia.
    --? Limitacion: no es CDC completo; si cambia la logica de parseo hay que hacer --full-refresh.
    {% if is_incremental() %}
    where ingested_at > (
        select coalesce(max(bronze_ingested_at), '1900-01-01'::timestamp_ltz)
        from {{ this }}
    )
    {% endif %}
)

select
    --? silver_record_id: clave estable de la fila = hash(ZIP|CSV|numero de fila). Es la unique_key del MERGE
    --? y es unica porque la clave natural de Bronze es justamente (archive, file, row).
    sha2(concat_ws('|', source_archive, source_file, source_row::varchar), 256) as silver_record_id,
    --? Hash de (archivo + contenido): sirve para analizar duplicados de contenido; NO se asume unico.
    sha2(concat_ws('|', source_file, source_sha256), 256) as source_file_row_hash_key,
    --? raw_record:campo = extraer una clave del VARIANT (JSON). ::varchar lo castea a texto.
    --? TRY_TO_* devuelve NULL si el valor no convierte (en vez de romper el modelo); los tests not_null
    --? de schema.yml detectan si eso pasa en columnas obligatorias. El valor original sigue en Bronze.
    try_to_number(raw_record:disk_id::varchar, 38, 0) as disk_id,
    --? ds viene como texto YYYYMMDD (ej. 20180105) -> DATE.
    try_to_date(raw_record:ds::varchar, 'YYYYMMDD') as observation_date,
    --? model_code: modelo censurado del SSD (MC1, MC2, ...). trim + '' -> NULL.
    nullif(trim(raw_record:model::varchar), '') as model_code
    --? Macro (macros/smart_attribute_columns.sql): genera las 102 columnas n_X / r_X como NUMBER(38,6).
    --? Empieza con coma, por eso la linea anterior no la lleva.
    {{ smart_attribute_columns('raw_record') }},
    --? Columnas de linaje: permiten rastrear cualquier fila de Silver hasta el CSV y la linea original.
    source_archive,
    source_file,
    source_row,
    --? Guarda como texto el nombre completo de la tabla Bronze de origen.
    '{{ source('bronze', 'smart_2019_raw') }}' as bronze_source_relation,
    source_date,
    source_sha256,
    --? bronze_ingested_at alimenta el filtro incremental de la proxima corrida.
    ingested_at as bronze_ingested_at,
    current_timestamp() as silver_loaded_at
from bronze
