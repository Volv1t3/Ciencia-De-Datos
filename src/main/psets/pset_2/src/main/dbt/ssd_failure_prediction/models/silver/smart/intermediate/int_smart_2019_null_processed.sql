--? SILVER LIMPIO 2019 (todos los modelos de SSD). Dos decisiones de calidad:
--?   1) Columnas: solo las 68 conservadas (se quitan 17 atributos 100% nulos en ambos anos).
--?   2) Filas: se excluyen las que no tienen NINGUNA medicion conservada (todo NULL). Esas filas
--?      no se borran: van a int_audit_smart_2019_no_attributes (cuarentena).
--? Nulos PARCIALES se conservan tal cual: no se imputan (0/media/mediana) porque en SMART un nulo
--? suele significar 'el modelo no reporta ese atributo' y no 'valor 0'.
--? materialized='table' sobreescribe el incremental de la carpeta: se reconstruye completa cada vez.
{{
    config(
        alias='INT_SMART_2019_NULL_PROCESSED',
        materialized='table',
        tags=['silver', 'smart', 'intermediate', 'null_processed', '2019']
    )
}}

--? Macros: 13 columnas de linaje + 68 columnas SMART conservadas (alias source).
select
    {{ smart_metadata_columns('source') }},
    {{ retained_smart_feature_columns('source') }}
from {{ ref('smart_2019') }} as source
--? NOT(todas nulas) = al menos una medicion presente.
where not ({{ all_retained_smart_attributes_null('source') }})
