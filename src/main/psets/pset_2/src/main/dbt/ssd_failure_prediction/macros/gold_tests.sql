{% test unique_column_combination(model, columns) %}
select
    {%- for column_name in columns %}
    {{ column_name }}{% if not loop.last %}, {% endif %}
    {%- endfor %},
    count(*) as duplicate_count
from {{ model }}
group by
    {%- for column_name in columns %}
    {{ column_name }}{% if not loop.last %}, {% endif %}
    {%- endfor %}
having count(*) > 1
{% endtest %}


{% test continuous_date_spine(model) %}
with ordered_dates as (
    select
        full_date,
        lag(full_date) over (order by full_date) as previous_date
    from {{ model }}
)
select full_date, previous_date
from ordered_dates
where previous_date is not null
  and datediff(day, previous_date, full_date) <> 1
{% endtest %}


{% test ssd_dimension_covers_source(model, source_model) %}
select disk_id, model_code
from {{ source_model }}
where disk_id is not null and model_code is not null
group by disk_id, model_code

minus

select disk_id, model_code
from {{ model }}
{% endtest %}


{% test gold_row_count_matches_source(model, source_model, source_scope='all') %}
with source_count as (
    select count(*) as row_count
    from {{ source_model }}
    {% if source_scope == 'mc1' %}
    where upper(model_code) = 'MC1'
    {% elif source_scope != 'all' %}
        {{ exceptions.raise_compiler_error(
            "gold_row_count_matches_source source_scope must be 'all' or 'mc1'"
        ) }}
    {% endif %}
),
gold_count as (
    select count(*) as row_count
    from {{ model }}
)
select
    source_count.row_count as source_row_count,
    gold_count.row_count as gold_row_count
from source_count
cross join gold_count
where source_count.row_count <> gold_count.row_count
{% endtest %}


{% test smart_fact_has_measurement(model, feature_scope) %}
    {%- if feature_scope == 'global' -%}
        {%- set attribute_ids = retained_smart_attribute_ids() -%}
    {%- elif feature_scope == 'mc1' -%}
        {%- set attribute_ids = mc1_retained_smart_attribute_ids() -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "smart_fact_has_measurement feature_scope must be 'global' or 'mc1'"
        ) }}
    {%- endif %}

select smart_daily_key
from {{ model }}
where
    {%- for attribute_id in attribute_ids %}
    n_{{ attribute_id }} is null
    and r_{{ attribute_id }} is null{% if not loop.last %}
    and {% endif %}
    {%- endfor %}
{% endtest %}


{% test smart_pair_missingness_matches(model, feature_scope) %}
    {%- if feature_scope == 'global' -%}
        {%- set attribute_ids = retained_smart_attribute_ids() -%}
    {%- elif feature_scope == 'mc1' -%}
        {%- set attribute_ids = mc1_retained_smart_attribute_ids() -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "smart_pair_missingness_matches feature_scope must be 'global' or 'mc1'"
        ) }}
    {%- endif %}

select smart_daily_key
from {{ model }}
where
    {%- for attribute_id in attribute_ids %}
    ((r_{{ attribute_id }} is null and n_{{ attribute_id }} is not null)
     or (r_{{ attribute_id }} is not null and n_{{ attribute_id }} is null))
    {% if not loop.last %}or {% endif %}
    {%- endfor %}
{% endtest %}


{% test fact_ssd_model_is(model, dimension_model, expected_model_code) %}
select fact.ssd_key
from {{ model }} as fact
inner join {{ dimension_model }} as ssd
    on fact.ssd_key = ssd.ssd_key
where upper(ssd.model_code) <> upper('{{ expected_model_code }}')
{% endtest %}
