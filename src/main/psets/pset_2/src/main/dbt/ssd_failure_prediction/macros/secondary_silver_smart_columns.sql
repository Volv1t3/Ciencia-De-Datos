{% macro smart_attribute_ids() -%}
    {{ return([
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
        170, 171, 172, 173, 174, 177, 180, 181, 182, 183, 184,
        187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197,
        198, 199, 200, 204, 205, 206, 207, 211, 233, 240, 241,
        242, 244, 245, 175, 232
    ]) }}
{%- endmacro %}


{% macro globally_unavailable_smart_attribute_ids() -%}
    {{ return([2, 3, 4, 6, 7, 8, 10, 11, 13, 189, 191, 193, 200, 204, 205, 207, 240]) }}
{%- endmacro %}


{% macro mc1_unavailable_smart_attribute_ids() -%}
    {{ return([175, 177, 181, 182, 190, 192, 232, 233, 241, 242, 244, 245]) }}
{%- endmacro %}


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


{% macro relation_prefix(relation_alias='') -%}
    {%- if relation_alias -%}{{ relation_alias }}.{%- endif -%}
{%- endmacro %}


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


{% macro smart_feature_columns(attribute_ids, relation_alias='') -%}
    {%- for attribute_id in attribute_ids %}
    {{ relation_prefix(relation_alias) }}n_{{ attribute_id }},
    {{ relation_prefix(relation_alias) }}r_{{ attribute_id }}{% if not loop.last %},{% endif %}
    {%- endfor %}
{%- endmacro %}


{% macro retained_smart_feature_columns(relation_alias='') -%}
    {{ smart_feature_columns(retained_smart_attribute_ids(), relation_alias) }}
{%- endmacro %}


{% macro mc1_smart_feature_columns(relation_alias='') -%}
    {{ smart_feature_columns(mc1_retained_smart_attribute_ids(), relation_alias) }}
{%- endmacro %}


{% macro all_retained_smart_attributes_null(relation_alias='') -%}
    {%- for attribute_id in retained_smart_attribute_ids() %}
    {{ relation_prefix(relation_alias) }}n_{{ attribute_id }} is null
    and {{ relation_prefix(relation_alias) }}r_{{ attribute_id }} is null{% if not loop.last %}
    and {% endif %}
    {%- endfor %}
{%- endmacro %}


{% macro globally_removed_smart_column_names() -%}
    {%- set columns = [] -%}
    {%- for attribute_id in globally_unavailable_smart_attribute_ids() -%}
        {%- do columns.append('N_' ~ attribute_id) -%}
        {%- do columns.append('R_' ~ attribute_id) -%}
    {%- endfor -%}
    {{ return(columns) }}
{%- endmacro %}


{% macro mc1_removed_smart_column_names() -%}
    {%- set columns = globally_removed_smart_column_names() -%}
    {%- for attribute_id in mc1_unavailable_smart_attribute_ids() -%}
        {%- do columns.append('N_' ~ attribute_id) -%}
        {%- do columns.append('R_' ~ attribute_id) -%}
    {%- endfor -%}
    {{ return(columns) }}
{%- endmacro %}
