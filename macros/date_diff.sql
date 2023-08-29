{% macro date_diff(start_date,end_date) %}
    {% if target.type == 'bigquery' %}
        DATE_DIFF(cast({{end_date}} as date), cast({{start_date}} as date), DAY) + 1
    {% elif target.type == 'redshift' %}
        DATEDIFF(d, {{start_date}}, {{end_date}}) + 1
    {% endif %}
{% endmacro %}
