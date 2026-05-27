{%- macro default_null() -%}
 
    CAST((NULL) AS {{type_string()}})

{%- endmacro -%}