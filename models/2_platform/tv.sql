{{
config(
materialized = 'table'
)
}}

WITH tv AS (
SELECT * FROM {{ source('playground','tv_demo') }}
)


transformed AS (


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
    ,CAST(date AS date) AS date
    ,SUM(expense) AS expense
    ,SUM(gross_calls) AS gross_calls
    ,SUM(grp_circ_impr) AS grp_circ_impr
    ,SUM(offered_calls) AS offered_calls


FROM

    (
    SELECT
         CONCAT(SUBSTR(dma, 1, INSTR(dma, ' ') - 1),', ',SUBSTR(dma, INSTR(dma, ' ') + 1)) AS dma
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
        ,date AS date
        ,expense AS expense
        ,gross_calls AS gross_calls
        ,grp_circ_impr AS grp_circ_impr
        ,offered_calls AS offered_calls

    FROM search

    ) AS RAW_DATA


    GROUP BY
    RAW_DATA.date
    ,fiscal_year
    ,fiscal_month
    ,fiscal_week
    ,fiscal_quarter
    ,source_table
    ,channel
    ,platform
    ,campaign_name
    ,adgroup_name

    )
SELECT *
FROM transformed