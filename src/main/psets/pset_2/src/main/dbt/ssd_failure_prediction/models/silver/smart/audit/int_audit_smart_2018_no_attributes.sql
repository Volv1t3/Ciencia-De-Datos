{{
    config(
        alias='INT_AUDIT_SMART_2018_NO_ATTRIBUTES',
        materialized='table',
        tags=['silver', 'smart', 'audit', 'all_attributes_null', '2018']
    )
}}

select
    {{ smart_metadata_columns('source') }},
    {{ retained_smart_feature_columns('source') }},
    2018::number(4, 0) as source_year,
    'ALL_SMART_ATTRIBUTES_NULL'::varchar as audit_reason
from {{ ref('smart_2018') }} as source
where {{ all_retained_smart_attributes_null('source') }}
