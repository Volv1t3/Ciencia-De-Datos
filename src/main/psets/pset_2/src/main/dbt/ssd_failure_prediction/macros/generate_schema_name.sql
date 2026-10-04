{#? Sobreescribe la macro interna de dbt que decide en que ESQUEMA se crea cada modelo. #}
{#? Por defecto dbt concatena esquema_del_perfil + '_' + esquema_custom #}
{#? (quedaria S_CDATOS_PSET2_BRONZE_S_CDATOS_PSET2_SILVER). Con esta version: #}
{#?   - si el modelo/carpeta define +schema -> se usa ese nombre exacto; #}
{#?   - si no -> se usa el esquema del perfil (target.schema). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
