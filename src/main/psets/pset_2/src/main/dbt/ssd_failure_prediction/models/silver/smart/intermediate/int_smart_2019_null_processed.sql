{{
    config(
        alias='INT_SMART_2019_NULL_PROCESSED',
        materialized='table',
        tags=['silver', 'smart', 'intermediate', 'null_processed', '2019']
    )
}}

select
    {{ smart_metadata_columns('source') }},
    {{ retained_smart_feature_columns('source') }}
from {{ ref('smart_2019') }} as source
where not ({{ all_retained_smart_attributes_null('source') }})
