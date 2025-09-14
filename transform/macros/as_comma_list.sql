{%- macro as_comma_list(input) -%}
    input | map('tojson') | join(', ')
{%- endmacro -%}