{{
config(
materialized = 'table'
)
}}

WITH tv AS (
SELECT * FROM {{ source('playground','tv_demo') }}
),


transformed AS (


SELECT
    CAST((dma) AS {{dbt_utils.type_string()}}) AS dma
    ,CAST((broadcast_year) AS {{dbt_utils.type_string()}}) AS broadcast_year
    ,CAST((broadcast_month) AS {{dbt_utils.type_string()}}) AS broadcast_month
    ,CAST((broadcast_period) AS {{dbt_utils.type_string()}}) AS broadcast_period
    ,CAST((broadcast_week_full_name) AS {{dbt_utils.type_string()}}) AS broadcast_week_full_name
    ,CAST((business_unit) AS {{dbt_utils.type_string()}}) AS business_unit
    ,CAST((campaign_name) AS {{dbt_utils.type_string()}}) AS campaign_name
    ,CAST((fiscal_month) AS {{dbt_utils.type_string()}}) AS fiscal_month
    ,CAST((format) AS {{dbt_utils.type_string()}}) AS format
    ,CAST((media_tactic) AS {{dbt_utils.type_string()}}) AS media_tactic
    ,CAST((media_type) AS {{dbt_utils.type_string()}}) AS media_type
    ,CAST(date AS date) AS date
    ,CAST((week) AS {{dbt_utils.type_string()}}) AS week
    ,CAST(week_begins AS date) AS week_begins
    ,CAST(week_ends AS date) AS week_ends
    ,SUM(expense) AS expense
    ,SUM(gross_calls) AS gross_calls
    ,SUM(grp_circ_impr) AS grp_circ_impr
    ,SUM(offered_calls) AS offered_calls


FROM

    (
    SELECT
         dma AS dma
        ,broadcast_year AS broadcast_year
        ,broadcast_month AS broadcast_month
        ,broadcast_period AS broadcast_period
        ,broadcast_week_full_name AS broadcast_week_full_name
        ,business_unit AS business_unit
        ,campaign_name AS campaign_name
        ,fiscal_month AS fiscal_month
        ,format AS format
        ,media_tactic AS media_tactic
        ,media_type AS media_type
        ,period AS date
        ,REGEXP_EXTRACT(broadcast_week_full_name, r'Wk \d+') AS week
        ,REGEXP_EXTRACT(broadcast_week_full_name, r'(\d{4}-\d{2}-\d{2})') AS week_begins
        ,REGEXP_EXTRACT(broadcast_week_full_name, r'\d{4}-\d{2}-\d{2} to (\d{4}-\d{2}-\d{2})') as week_ends
        ,expense AS expense
        ,gross_calls AS gross_calls
        ,grp_circ_impr AS grp_circ_impr
        ,offered_calls AS offered_calls

    FROM tv

    ) AS RAW_DATA


    GROUP BY
    dma
    ,broadcast_year
    ,broadcast_month
    ,broadcast_period
    ,broadcast_week_full_name
    ,business_unit
    ,campaign_name
    ,fiscal_month
    ,format
    ,media_tactic
    ,media_type
    ,date
    ,week
    ,week_begins
    ,week_ends

    )
SELECT *
FROM transformed