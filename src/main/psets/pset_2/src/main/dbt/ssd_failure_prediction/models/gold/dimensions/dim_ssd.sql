--? DIMENSION SSD (un registro por SSD fisico).
--? Clave de negocio = (disk_id, model_code): el disk_id se repite entre modelos distintos,
--? asi que disk_id solo NO identifica un disco (ver CONTEXT.md).
--? Se alimenta de observaciones Y etiquetas para que un SSD que fallo sin telemetria limpia
--? tambien exista (test ssd_dimension_covers_source).
{{
    config(
        alias='DIM_SSD',
        tags=['gold', 'dimension', 'ssd']
    )
}}

--? UNION (no UNION ALL) deja cada par (disk_id, model_code) una sola vez.
with ssd_business_keys as (
    select disk_id, model_code
    from {{ ref('smart_null_processed_all_years') }}

    union

    select disk_id, model_code
    from {{ ref('ssd_failure_labels') }}
)

select
    --? Clave sustituta ssd_key = SHA-256 de la clave de negocio: determinista (no cambia entre
    --? reconstrucciones) a diferencia de un autoincremental.
    sha2(concat_ws('|', disk_id::varchar, model_code), 256) as ssd_key,
    disk_id,
    model_code
from ssd_business_keys
