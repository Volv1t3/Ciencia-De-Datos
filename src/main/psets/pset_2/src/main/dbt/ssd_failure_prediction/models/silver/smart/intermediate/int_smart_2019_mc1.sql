{{
    config(
        alias='INT_SMART_2019_MC1',
        materialized='table',
        tags=['silver', 'smart', 'intermediate', 'mc1', '2019']
    )
}}

select
    {{ smart_metadata_columns('source') }},
    {{ mc1_smart_feature_columns('source') }}
from {{ ref('int_smart_2019_null_processed') }} as source
where upper(source.model_code) = 'MC1'
