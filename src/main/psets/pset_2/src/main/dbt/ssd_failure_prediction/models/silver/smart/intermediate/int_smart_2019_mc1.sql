--? SUBCONJUNTO MC1 2019. El modelo predictivo se enfoca en MC1 (poblacion con esquema SMART muy estable
--? entre anos, ver EDA). Se quitan ademas 12 atributos que MC1 nunca reporta -> 44 columnas.
--? Parte de la tabla ya limpia (null_processed), asi que hereda las decisiones anteriores.
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
--? upper() por seguridad ante diferencias de mayusculas en el codigo de modelo.
where upper(source.model_code) = 'MC1'
