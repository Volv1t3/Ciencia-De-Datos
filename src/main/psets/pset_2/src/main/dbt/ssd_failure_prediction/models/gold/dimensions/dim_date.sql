--? DIMENSION FECHA (calendario conformado, compartido por ambos hechos).
--? Va del primer al ultimo dia con datos (observaciones o fallas) SIN huecos: el test
--? continuous_date_spine verifica que no falte ningun dia. Asi un analisis por dia/semana/mes
--? muestra tambien los dias sin eventos.
{{
    config(
        alias='DIM_DATE',
        tags=['gold', 'dimension', 'date']
    )
}}

--? 1) Todas las fechas que aparecen en los datos (UNION elimina repetidas).
with relevant_dates as (
    select observation_date as full_date
    from {{ ref('smart_null_processed_all_years') }}

    union

    select failure_date as full_date
    from {{ ref('ssd_failure_labels') }}
),
--? 2) Primera y ultima fecha.
date_bounds as (
    select min(full_date) as minimum_date, max(full_date) as maximum_date
    from relevant_dates
),
--? 3) Generar todos los dias entre ambas: array_generate_range(0, N) da [0..N-1] y flatten lo
--?    convierte en filas; dateadd suma cada numero a la fecha minima.
date_spine as (
    select dateadd(day, generated_date.value::integer, minimum_date)::date as full_date
    from date_bounds,
    lateral flatten(
        input => array_generate_range(
            0,
            datediff(day, minimum_date, maximum_date) + 1
        )
    ) as generated_date
)

select
    --? date_key entero YYYYMMDD (ej. 20180105): legible y compacto como clave foranea.
    to_number(to_char(full_date, 'YYYYMMDD'), 8, 0) as date_key,
    full_date,
    year(full_date) as year,
    quarter(full_date) as quarter,
    month(full_date) as month,
    monthname(full_date) as month_name,
    weekiso(full_date) as week_of_year,
    day(full_date) as day_of_month,
    --? ISO: lunes = 1 ... domingo = 7.
    dayofweekiso(full_date) as day_of_week,
    dayname(full_date) as day_name,
    dayofweekiso(full_date) in (6, 7) as is_weekend
from date_spine
