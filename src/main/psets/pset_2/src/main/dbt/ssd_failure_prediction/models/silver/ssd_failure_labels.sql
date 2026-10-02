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
    nullif(trim(raw_record:model::varchar), '') as model_code,
    try_to_timestamp_ntz(raw_record:failure_time::varchar) as failure_at,
    to_date(try_to_timestamp_ntz(raw_record:failure_time::varchar)) as failure_date,
    source_archive,
    source_file,
    source_row,
    '{{ source('bronze', 'ssd_failure_label_raw') }}' as bronze_source_relation,
    source_date,
    source_sha256,
    ingested_at as bronze_ingested_at,
    current_timestamp() as silver_loaded_at
from bronze
