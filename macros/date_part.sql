{% macro date_part(datepart,date) %}
    {% if target.type == 'bigquery' %}
            EXTRACT({{datepart}} FROM {{date}})

    {% elif target.type == 'redshift' %}
        datepart('{{datepart}}',{{date}})
    {% endif %}
{% endmacro %}
