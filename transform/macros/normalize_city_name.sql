
{% macro normalize_city_name(col) %}
    TRIM(REGEXP_REPLACE({{ col }}, 'CITY OF|CITY', '', 'g'))
{% endmacro %}

