--? SILVER ETIQUETAS DE FALLA: Bronze -> tabla tipada. Una fila = un evento de falla confirmado.
--? Materializacion INCREMENTAL con estrategia MERGE (heredada de dbt_project.yml para silver/):
--?   - 1a corrida (o dbt run --full-refresh): crea la tabla con TODO Bronze.
--?   - corridas siguientes: solo procesa filas de Bronze con INGESTED_AT mas nuevo que lo ya cargado
--?     y hace MERGE por unique_key (silver_record_id): actualiza si existe, inserta si no.
--? on_schema_change='fail': si cambian las columnas del modelo, dbt falla en vez de alterar la tabla
--? en silencio -> hay que correr --full-refresh a proposito.
--? alias = nombre fisico de la tabla en Snowflake; tags permiten correr subconjuntos (dbt run -s tag:2018).
{{
    config(
        alias='SSD_FAILURE_LABELS',
        incremental_strategy='merge',
        unique_key='silver_record_id',
        on_schema_change='fail',
        tags=['silver', 'failure_labels']
    )
}}

with bronze as (
    select *
    from {{ source('bronze', 'ssd_failure_label_raw') }}
    --? Solo filas de Bronze nuevas/actualizadas desde la ultima corrida (ver smart_2018.sql).
    {% if is_incremental() %}
    where ingested_at > (
        select coalesce(max(bronze_ingested_at), '1900-01-01'::timestamp_ltz)
        from {{ this }}
    )
    {% endif %}
)

select
    --? Clave estable de la fila (ZIP|CSV|fila) = unique_key del MERGE.
    sha2(concat_ws('|', source_archive, source_file, source_row::varchar), 256) as silver_record_id,
    sha2(concat_ws('|', source_file, source_sha256), 256) as source_file_row_hash_key,
    --? disk_id + model_code = SSD fisico. El disk_id SOLO se repite entre modelos distintos,
    --? por eso nunca se usa disk_id solo como identificador (ver CONTEXT.md).
    try_to_number(raw_record:disk_id::varchar, 38, 0) as disk_id,
    nullif(trim(raw_record:model::varchar), '') as model_code,
    --? failure_time -> TIMESTAMP_NTZ (sin zona horaria: se respeta la hora tal como viene).
    --? failure_date = solo la fecha; es la que se une con dim_date y con las observaciones diarias.
    try_to_timestamp_ntz(raw_record:failure_time::varchar) as failure_at,
    to_date(try_to_timestamp_ntz(raw_record:failure_time::varchar)) as failure_date,
    --? Linaje hacia Bronze.
    source_archive,
    source_file,
    source_row,
    '{{ source('bronze', 'ssd_failure_label_raw') }}' as bronze_source_relation,
    source_date,
    source_sha256,
    ingested_at as bronze_ingested_at,
    current_timestamp() as silver_loaded_at
from bronze
