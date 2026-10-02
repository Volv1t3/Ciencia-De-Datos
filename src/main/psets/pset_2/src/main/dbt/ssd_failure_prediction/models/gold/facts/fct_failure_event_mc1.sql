{{
    config(
        alias='FCT_FAILURE_EVENT_MC1',
        tags=['gold', 'fact', 'failure_event', 'mc1']
    )
}}

select
    sha2(
        concat_ws('|', ssd.ssd_key, failure.silver_record_id),
        256
    ) as failure_event_key,
    ssd.ssd_key,
    date_dimension.date_key as failure_date_key,
    failure.failure_date,
    failure.failure_at,
    1::number(1, 0) as failure_count
from {{ ref('ssd_failure_labels') }} as failure
inner join {{ ref('dim_ssd') }} as ssd
    on failure.disk_id = ssd.disk_id
   and failure.model_code = ssd.model_code
inner join {{ ref('dim_date') }} as date_dimension
    on failure.failure_date = date_dimension.full_date
where upper(failure.model_code) = 'MC1'
