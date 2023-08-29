{%- macro headline(field1) -%}
     {% if target.type == 'bigquery' %}
      REGEXP_EXTRACT({{field1}}, r'"text":"([\w+]*[^\w+]*[^","]*)', 1,2)

    {% elif target.type == 'redshift' %}
      regexp_substr({{field1}}, '"text":"(([\\w+]*[^\\w+]*[^","]*))',1,2, 'ie')

    {% endif %}
{%- endmacro -%}