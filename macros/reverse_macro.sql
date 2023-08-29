{% macro reverse_splitpart(string_text, delimiter_text, last_part_number) %}
 {% if target.type == 'redshift' %}

      REVERSE(SPLIT_PART(REVERSE({{string_text}}),'{{delimiter_text}}',{{last_part_number}}))

  {% elif target.type == 'bigquery' %}
        ARRAY_REVERSE(
            SPLIT({{ string_text }},'{{ delimiter_text }}')
          )
        [SAFE_OFFSET(cast( {{ last_part_number }} as integer) - 1)]
  {% endif %}
{% endmacro %}