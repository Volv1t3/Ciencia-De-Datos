{# Use custom schemas exactly as configured. This prevents dbt from prefixing
   S_CDATOS_SILVER with the target schema. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{#
READ-AFTER-CODE: schema-resolution flow

This macro participates before model SQL reaches Snowflake. dbt asks it where
each model should be built. A model without an explicit schema receives the
profile target schema. A model with a configured schema receives that name
unchanged after trimming whitespace.

    dbt model configuration
             |
             v
    custom schema supplied? ---- no ----> target.schema
             |
            yes
             v
    configured schema, trimmed exactly
             |
             v
       Snowflake object schema

The exact-name behaviour is intentional. dbt's usual default can concatenate a
target schema and a custom schema. This project needs the explicit physical
schema S_CDATOS_SILVER, so this macro prevents an unintended prefixed schema.
#}
