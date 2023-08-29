{%- macro search_creative_description(field1) -%}
     {% if target.type == 'bigquery' %}
      REGEXP_EXTRACT({{field1}}, r'"text":"([\w+]*[^\w+]*[^","]*)', 1,1)

    {% elif target.type == 'redshift' %}
      regexp_substr({{field1}}, '"text":"(([\\w+]*[^\\w+]*[^","]*))',1,1, 'ie')

    {% endif %}
{%- endmacro -%}