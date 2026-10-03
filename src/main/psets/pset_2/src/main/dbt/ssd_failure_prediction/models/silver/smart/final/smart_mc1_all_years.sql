{{
    config(
        alias='SMART_MC1_ALL_YEARS',
        materialized='table',
        tags=['silver', 'smart', 'final', 'merged', 'mc1']
    )
}}

select
    {{ smart_metadata_columns('source') }},
    {{ mc1_smart_feature_columns('source') }}
from {{ ref('int_smart_2018_mc1') }} as source

union all

select
    {{ smart_metadata_columns('source') }},
    {{ mc1_smart_feature_columns('source') }}
from {{ ref('int_smart_2019_mc1') }} as source
