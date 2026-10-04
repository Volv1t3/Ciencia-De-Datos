{#? Macros que definen QUE columnas SMART sobreviven en cada capa. Son la politica de nulos del EDA #}
{#? (ver notebooks/ssd_null_analysis_book) convertida en codigo reutilizable por modelos y tests. #}
{#? Conteos: 51 atributos -> quitar 17 globalmente vacios = 34 (68 columnas n/r) #}
{#?          -> quitar 12 vacios en MC1 = 22 (44 columnas) para el subconjunto MC1. #}
{#? Los 51 atributos SMART que trae la fuente (en el orden del CSV). #}
{% macro smart_attribute_ids() -%}
    {{ return([
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
        170, 171, 172, 173, 174, 177, 180, 181, 182, 183, 184,
        187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197,
        198, 199, 200, 204, 205, 206, 207, 211, 233, 240, 241,
        242, 244, 245, 175, 232
    ]) }}
{%- endmacro %}


{#? 17 atributos 100% nulos en TODOS los modelos y en ambos anos (evidencia en el EDA). #}
{#? No aportan informacion -> se eliminan de las tablas procesadas. Siguen en Bronze/Silver base. #}
{% macro globally_unavailable_smart_attribute_ids() -%}
    {{ return([2, 3, 4, 6, 7, 8, 10, 11, 13, 189, 191, 193, 200, 204, 205, 207, 240]) }}
{%- endmacro %}


{#? 12 atributos adicionales que el modelo MC1 nunca reporta (estructuralmente ausentes en MC1), #}
{#? aunque otros modelos si. Solo se quitan del subconjunto MC1. #}
{% macro mc1_unavailable_smart_attribute_ids() -%}
    {{ return([175, 177, 181, 182, 190, 192, 232, 233, 241, 242, 244, 245]) }}
{%- endmacro %}


{#? Atributos conservados para todos los modelos = todos - globalmente vacios (34). #}
{#? do retained.append(...) es la forma de modificar una lista dentro de Jinja. #}
{% macro retained_smart_attribute_ids() -%}
    {%- set retained = [] -%}
    {%- set unavailable = globally_unavailable_smart_attribute_ids() -%}
    {%- for attribute_id in smart_attribute_ids() -%}
        {%- if attribute_id not in unavailable -%}
            {%- do retained.append(attribute_id) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(retained) }}
{%- endmacro %}


{#? Atributos conservados para MC1 = conservados - vacios en MC1 (22). #}
{% macro mc1_retained_smart_attribute_ids() -%}
    {%- set retained = [] -%}
    {%- set unavailable = mc1_unavailable_smart_attribute_ids() -%}
    {%- for attribute_id in retained_smart_attribute_ids() -%}
        {%- if attribute_id not in unavailable -%}
            {%- do retained.append(attribute_id) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(retained) }}
{%- endmacro %}


{#? Devuelve 'alias.' si se pasa un alias de tabla (para escribir source.disk_id), o nada. #}
{% macro relation_prefix(relation_alias='') -%}
    {%- if relation_alias -%}{{ relation_alias }}.{%- endif -%}
{%- endmacro %}


{#? Las 13 columnas de identidad y linaje que comparten TODAS las tablas SMART procesadas. #}
{% macro smart_metadata_columns(relation_alias='') -%}
    {{ relation_prefix(relation_alias) }}silver_record_id,
    {{ relation_prefix(relation_alias) }}source_file_row_hash_key,
    {{ relation_prefix(relation_alias) }}disk_id,
    {{ relation_prefix(relation_alias) }}observation_date,
    {{ relation_prefix(relation_alias) }}model_code,
    {{ relation_prefix(relation_alias) }}source_archive,
    {{ relation_prefix(relation_alias) }}source_file,
    {{ relation_prefix(relation_alias) }}source_row,
    {{ relation_prefix(relation_alias) }}bronze_source_relation,
    {{ relation_prefix(relation_alias) }}source_date,
    {{ relation_prefix(relation_alias) }}source_sha256,
    {{ relation_prefix(relation_alias) }}bronze_ingested_at,
    {{ relation_prefix(relation_alias) }}silver_loaded_at
{%- endmacro %}


{#? Lista n_X, r_X para una lista de IDs dada (con o sin alias). #}
{% macro smart_feature_columns(attribute_ids, relation_alias='') -%}
    {%- for attribute_id in attribute_ids %}
    {{ relation_prefix(relation_alias) }}n_{{ attribute_id }},
    {{ relation_prefix(relation_alias) }}r_{{ attribute_id }}{% if not loop.last %},{% endif %}
    {%- endfor %}
{%- endmacro %}


{#? Atajo: las 68 columnas conservadas para todos los modelos. #}
{% macro retained_smart_feature_columns(relation_alias='') -%}
    {{ smart_feature_columns(retained_smart_attribute_ids(), relation_alias) }}
{%- endmacro %}


{#? Atajo: las 44 columnas conservadas para MC1. #}
{% macro mc1_smart_feature_columns(relation_alias='') -%}
    {{ smart_feature_columns(mc1_retained_smart_attribute_ids(), relation_alias) }}
{%- endmacro %}


{#? Condicion SQL: TODAS las columnas conservadas son NULL (fila sin ninguna medicion util). #}
{#? Con 'not (...)' selecciona filas limpias; sin 'not' selecciona las que van a auditoria. #}
{% macro all_retained_smart_attributes_null(relation_alias='') -%}
    {%- for attribute_id in retained_smart_attribute_ids() %}
    {{ relation_prefix(relation_alias) }}n_{{ attribute_id }} is null
    and {{ relation_prefix(relation_alias) }}r_{{ attribute_id }} is null{% if not loop.last %}
    and {% endif %}
    {%- endfor %}
{%- endmacro %}


{#? Nombres de columnas que NO deben existir en las tablas procesadas (los usa el test smart_columns_absent). #}
{% macro globally_removed_smart_column_names() -%}
    {%- set columns = [] -%}
    {%- for attribute_id in globally_unavailable_smart_attribute_ids() -%}
        {%- do columns.append('N_' ~ attribute_id) -%}
        {%- do columns.append('R_' ~ attribute_id) -%}
    {%- endfor -%}
    {{ return(columns) }}
{%- endmacro %}


{#? Igual, para MC1: las globales + las 12 especificas de MC1. #}
{% macro mc1_removed_smart_column_names() -%}
    {%- set columns = globally_removed_smart_column_names() -%}
    {%- for attribute_id in mc1_unavailable_smart_attribute_ids() -%}
        {%- do columns.append('N_' ~ attribute_id) -%}
        {%- do columns.append('R_' ~ attribute_id) -%}
    {%- endfor -%}
    {{ return(columns) }}
{%- endmacro %}
