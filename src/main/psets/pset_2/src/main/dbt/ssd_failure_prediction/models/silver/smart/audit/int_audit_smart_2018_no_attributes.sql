--? AUDITORIA / CUARENTENA 2018: filas con metadatos validos pero SIN ninguna medicion SMART
--? conservada. Se guardan aparte (no se borran) para poder explicarlas y contarlas en el informe.
--? Junto con int_smart_2018_null_processed forma una particion exacta de smart_2018
--? (lo comprueba el test smart_source_partition_reconciles).
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
    --? Columnas de contexto: ano de origen y motivo de la cuarentena (constantes).
    2018::number(4, 0) as source_year,
    'ALL_SMART_ATTRIBUTES_NULL'::varchar as audit_reason
from {{ ref('smart_2018') }} as source
--? Exactamente el complemento del filtro de los modelos *_null_processed.
where {{ all_retained_smart_attributes_null('source') }}
