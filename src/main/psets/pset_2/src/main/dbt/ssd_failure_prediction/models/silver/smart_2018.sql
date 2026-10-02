{{
    config(
        alias='SMART_2018',
        incremental_strategy='merge',
        unique_key='silver_record_id',
        on_schema_change='fail',
        tags=['silver', 'smart', '2018']
    )
}}

with bronze as (
    select *
    from {{ source('bronze', 'smart_2018_raw') }}
    {% if is_incremental() %}
    where ingested_at > (
        select coalesce(max(bronze_ingested_at), '1900-01-01'::timestamp_ltz)
        from {{ this }}
    )
    {% endif %}
)

select
    sha2(concat_ws('|', source_archive, source_file, source_row::varchar), 256) as silver_record_id,
    sha2(concat_ws('|', source_file, source_sha256), 256) as source_file_row_hash_key,
    try_to_number(raw_record:disk_id::varchar, 38, 0) as disk_id,
    try_to_date(raw_record:ds::varchar, 'YYYYMMDD') as observation_date,
    nullif(trim(raw_record:model::varchar), '') as model_code
    {{ smart_attribute_columns('raw_record') }},
    source_archive,
    source_file,
    source_row,
    '{{ source('bronze', 'smart_2018_raw') }}' as bronze_source_relation,
    source_date,
    source_sha256,
    ingested_at as bronze_ingested_at,
    current_timestamp() as silver_loaded_at
from bronze
