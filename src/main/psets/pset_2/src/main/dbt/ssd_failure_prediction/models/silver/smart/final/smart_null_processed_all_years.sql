--? SILVER FINAL (todos los modelos, 2018 + 2019). Es la entrada de dim_ssd, dim_date y fct_smart_daily_all.
--? Los anos se procesan por separado hasta tener el MISMO esquema y luego se unen con UNION ALL
--? (no UNION): no se deduplica ni se agrega nada; el test smart_year_union_reconciles verifica
--? que el resultado sea exactamente 2018 + 2019.
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
