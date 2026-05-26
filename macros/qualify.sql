{% macro qualify(client_name, func, params) %}
    {% if target.type == 'redshift' %}
      {{client_name}}{{ func }}{{params}}
    {% elif target.type == 'bigquery' %}
       `{{target.project}}`.{{client_name}}{{func}}{{params}}
    {% endif %}
{% endmacro %}
