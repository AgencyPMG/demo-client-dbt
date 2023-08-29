{% macro length(string_text) %}
    {% if target.type == 'bigquery' %}
        length({{string_text}}})
    {% elif target.type == 'redshift' %}
        len({{string_text}})
    {% endif %}
{% endmacro %}