{% macro smart_attribute_columns(raw_record='raw_record') -%}
    {%- set attribute_ids = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
        170, 171, 172, 173, 174, 177, 180, 181, 182, 183, 184,
        187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197,
        198, 199, 200, 204, 205, 206, 207, 211, 233, 240, 241,
        242, 244, 245, 175, 232
    ] -%}
    {%- for attribute_id in attribute_ids %}
        , try_to_decimal({{ raw_record }}:n_{{ attribute_id }}::varchar, 38, 6) as n_{{ attribute_id }}
        , try_to_decimal({{ raw_record }}:r_{{ attribute_id }}::varchar, 38, 6) as r_{{ attribute_id }}
    {%- endfor %}
{%- endmacro %}
