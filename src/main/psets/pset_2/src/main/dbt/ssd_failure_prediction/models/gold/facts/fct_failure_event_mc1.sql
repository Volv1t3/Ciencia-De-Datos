--? TABLA DE HECHOS DE EVENTOS DE FALLA (solo MC1).
--? GRAIN: una fila = una falla confirmada (una fila de la tabla de etiquetas).
--? Hecho transaccional: medida aditiva failure_count = 1 para poder SUMAR fallas por fecha/modelo.
--? Se separa del hecho SMART porque una falla puede ocurrir un dia sin telemetria.
{{
    config(
        alias='FCT_FAILURE_EVENT_MC1',
        tags=['gold', 'fact', 'failure_event', 'mc1']
    )
}}

select
    --? Clave del evento: hash(ssd_key | id de la fila Silver de la etiqueta).
    sha2(
        concat_ws('|', ssd.ssd_key, failure.silver_record_id),
        256
    ) as failure_event_key,
    ssd.ssd_key,
    date_dimension.date_key as failure_date_key,
    failure.failure_date,
    failure.failure_at,
    1::number(1, 0) as failure_count
--? Joins a las dimensiones por clave de negocio (disk_id + model_code) y por fecha de falla.
from {{ ref('ssd_failure_labels') }} as failure
inner join {{ ref('dim_ssd') }} as ssd
    on failure.disk_id = ssd.disk_id
   and failure.model_code = ssd.model_code
inner join {{ ref('dim_date') }} as date_dimension
    on failure.failure_date = date_dimension.full_date
--? Solo las fallas de SSD MC1.
where upper(failure.model_code) = 'MC1'
