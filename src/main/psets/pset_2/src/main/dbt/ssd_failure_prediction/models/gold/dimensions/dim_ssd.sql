{{
    config(
        alias='DIM_SSD',
        tags=['gold', 'dimension', 'ssd']
    )
}}

with ssd_business_keys as (
    select disk_id, model_code
    from {{ ref('smart_null_processed_all_years') }}

    union

    select disk_id, model_code
    from {{ ref('ssd_failure_labels') }}
)

select
    sha2(concat_ws('|', disk_id::varchar, model_code), 256) as ssd_key,
    disk_id,
    model_code
from ssd_business_keys
