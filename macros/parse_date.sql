{% macro parse_date(date) %}

    {% if target.type == 'bigquery' %}
        PARSE_DATE("%F",{{date}})

    {% elif target.type == 'redshift' %}
        TO_DATE({{date}}, 'yyyy-mm-dd')

    {% endif %}

{% endmacro %}
