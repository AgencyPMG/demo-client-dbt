{% macro grant_select(this) -%}

    {%- if target.type == 'redshift' -%}
        grant select on {{ this }} to group nothing_bundt_cakes;
    {%- endif -%}

{%- endmacro %}