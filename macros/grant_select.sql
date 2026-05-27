{% macro grant_select(this) -%}

    {%- if target.type == 'redshift' -%}
        grant select on {{ this }} to group demo;
    {%- endif -%}

{%- endmacro %}