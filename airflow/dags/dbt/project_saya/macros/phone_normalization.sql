{% macro normalize_phone_number(column_name) %}
    ltrim({{ column_name }}, '+')
{% endmacro %}