{{
    config(
        alias='SMART_NULL_PROCESSED_ALL_YEARS',
        materialized='table',
        tags=['silver', 'smart', 'final', 'merged', 'null_processed']
    )
}}

select
    {{ smart_metadata_columns('source') }},
    {{ retained_smart_feature_columns('source') }}
from {{ ref('int_smart_2018_null_processed') }} as source

union all

select
    {{ smart_metadata_columns('source') }},
    {{ retained_smart_feature_columns('source') }}
from {{ ref('int_smart_2019_null_processed') }} as source
