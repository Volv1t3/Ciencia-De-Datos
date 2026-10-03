{{
    config(
        alias='INT_SMART_2018_MC1',
        materialized='table',
        tags=['silver', 'smart', 'intermediate', 'mc1', '2018']
    )
}}

select
    {{ smart_metadata_columns('source') }},
    {{ mc1_smart_feature_columns('source') }}
from {{ ref('int_smart_2018_null_processed') }} as source
where upper(source.model_code) = 'MC1'
