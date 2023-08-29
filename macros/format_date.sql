{% macro format_date(timeperiod) %}

    {% if target.type == 'bigquery' %}
        PARSE_DATE('%m/%d/%Y',{{split_part('timeperiod',' ',1)}})

    {% elif target.type == 'redshift' %}
        TO_DATE({{split_part('timeperiod',' ',1)}}, 'mm/dd/yyyy')

    {% endif %}
{% endmacro %}