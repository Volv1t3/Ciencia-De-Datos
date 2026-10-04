{#? TESTS GENERICOS PROPIOS de la capa Silver. Regla de dbt: un test PASA si su query devuelve 0 filas; #}
{#? cada fila devuelta es una violacion. Se aplican desde models/silver/smart/schema.yml. #}
{#? Para tablas de AUDITORIA: toda fila debe tener todas las mediciones nulas (si no, se enruto mal). #}
{% test all_retained_smart_attributes_are_null(model) %}
select silver_record_id
from {{ model }}
where not ({{ all_retained_smart_attributes_null() }})
{% endtest %}


{#? La tabla final (UNION ALL de 2018 y 2019) debe ser EXACTAMENTE la suma de sus dos entradas. #}
{#? Se compara en ambos sentidos con MINUS: filas que faltan y filas que sobran -> ambas deben ser 0. #}
{#? Garantiza que la union no perdio, duplico ni invento filas. #}
{% test smart_year_union_reconciles(model, model_2018, model_2019, feature_scope) %}
    {%- if feature_scope == 'global' -%}
        {%- set feature_columns_macro = 'retained_smart_feature_columns' -%}
    {%- elif feature_scope == 'mc1' -%}
        {%- set feature_columns_macro = 'mc1_smart_feature_columns' -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "smart_year_union_reconciles feature_scope must be 'global' or 'mc1'"
        ) }}
    {%- endif %}

with expected_rows as (
    select
        {{ smart_metadata_columns('source') }},
        {% if feature_columns_macro == 'retained_smart_feature_columns' %}
        {{ retained_smart_feature_columns('source') }}
        {% else %}
        {{ mc1_smart_feature_columns('source') }}
        {% endif %}
    from {{ model_2018 }} as source

    union all

    select
        {{ smart_metadata_columns('source') }},
        {% if feature_columns_macro == 'retained_smart_feature_columns' %}
        {{ retained_smart_feature_columns('source') }}
        {% else %}
        {{ mc1_smart_feature_columns('source') }}
        {% endif %}
    from {{ model_2019 }} as source
),
actual_rows as (
    select * from {{ model }}
),
missing_from_merge as (
    select * from expected_rows
    minus
    select * from actual_rows
),
unexpected_in_merge as (
    select * from actual_rows
    minus
    select * from expected_rows
)
select 'MISSING_FROM_MERGE' as reconciliation_error, *
from missing_from_merge

union all

select 'UNEXPECTED_IN_MERGE' as reconciliation_error, *
from unexpected_in_merge
{% endtest %}


{#? Para tablas LIMPIAS: ninguna fila puede tener todas las mediciones nulas. #}
{% test at_least_one_retained_smart_attribute_is_present(model) %}
select silver_record_id
from {{ model }}
where {{ all_retained_smart_attributes_null() }}
{% endtest %}


{#? Verifica en information_schema que las columnas eliminadas por la politica de nulos #}
{#? realmente NO existen fisicamente en la tabla. #}
{% test smart_columns_absent(model, column_scope) %}
    {%- if column_scope == 'global' -%}
        {%- set forbidden_columns = globally_removed_smart_column_names() -%}
    {%- elif column_scope == 'mc1' -%}
        {%- set forbidden_columns = mc1_removed_smart_column_names() -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "smart_columns_absent column_scope must be 'global' or 'mc1'"
        ) }}
    {%- endif %}

select column_name
from {{ model.database }}.information_schema.columns
where upper(table_schema) = upper('{{ model.schema }}')
  and upper(table_name) = upper('{{ model.identifier }}')
  and upper(column_name) in (
    {%- for column_name in forbidden_columns %}
    '{{ column_name }}'{% if not loop.last %}, {% endif %}
    {%- endfor %}
  )
{% endtest %}


{#? Particion sin perdida: base Silver = filas limpias UNION ALL filas de auditoria, exactamente. #}
{#? Cada fila de origen esta en una (y solo una) de las dos salidas; ninguna se pierde ni se inventa. #}
{% test smart_source_partition_reconciles(model, source_model, audit_model) %}
with source_rows as (
    select
        {{ smart_metadata_columns('source') }},
        {{ retained_smart_feature_columns('source') }}
    from {{ source_model }} as source
),
routed_rows as (
    select
        {{ smart_metadata_columns('clean') }},
        {{ retained_smart_feature_columns('clean') }}
    from {{ model }} as clean

    union all

    select
        {{ smart_metadata_columns('audit') }},
        {{ retained_smart_feature_columns('audit') }}
    from {{ audit_model }} as audit
),
missing_from_outputs as (
    select * from source_rows
    minus
    select * from routed_rows
),
unexpected_in_outputs as (
    select * from routed_rows
    minus
    select * from source_rows
)
select 'MISSING_FROM_OUTPUTS' as reconciliation_error, *
from missing_from_outputs

union all

select 'UNEXPECTED_IN_OUTPUTS' as reconciliation_error, *
from unexpected_in_outputs
{% endtest %}
