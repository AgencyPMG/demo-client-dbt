{% macro count_substring(string,substring) %}
    {% if target.type == 'bigquery' %}
        ARRAY_LENGTH(REGEXP_EXTRACT_ALL({{string}},'{{substring}}'))

    {% elif target.type == 'redshift' %}
       REGEXP_COUNT({{string}},'{{substring}}')
    {% endif %}
{% endmacro %}
