{% macro grant_permissions(schemas, target_name, target_schema) %}
    {% if target_name == 'redshift-dev' or target_name == 'redshift-prod' %}
        {% for schema in schemas %}
            {% set client_group = 'nothing_bundt_cakes' %}
            {% set analytics_group = 'analytics' %}
            grant usage on schema {{ schema }} to group {{ analytics_group }} ;
            grant select on all tables in schema {{ schema }} to group  {{ analytics_group }} ;

            grant usage on schema {{ schema }} to group  {{ client_group }}  ;
            grant select on all tables in schema {{ schema }} to group  {{ client_group }}
        {% endfor %}
    {% endif %}
{% endmacro %}
