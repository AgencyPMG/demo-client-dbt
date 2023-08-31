{{
config(
materialized = 'table'
)
}}

WITH search AS (
SELECT * FROM {{ref('search')}}
),

tv AS (
SELECT * FROM {{ref('tv')}}
)


SELECT

    dma
    ,year
    ,month
    ,business_unit
    ,campaign_name
    ,fiscal_month
    ,format
    ,tactic
    ,media_type
    ,date
    ,week
    ,week_begins
    ,week_ends
    ,SUM(impressions) AS impressions
    ,SUM(clicks) AS clicks
    ,SUM(expense) AS expense
    ,SUM(gross_calls) AS gross_calls
    ,SUM(offered_calls) AS offered_calls

FROM

(
    SELECT
     dma AS dma
    ,broadcast_year AS year
    ,broadcast_month AS month
    ,business_unit AS business_unit
    ,campaign_name AS campaign_name
    ,fiscal_month AS fiscal_month
    ,format AS format
    ,media_tactic AS tactic
    ,media_type AS media_type
    ,'Video' AS channel
    ,CAST(date AS date) AS date
    ,week
    ,week_begins
    ,week_ends
    ,expense AS expense
    ,gross_calls
    ,grp_circ_impr AS impressions
    ,offered_calls
    ,0 AS clicks

    FROM tv

    UNION ALL

    SELECT

    dma
    ,year
    ,month
    ,'n/a' AS business_unit
    ,'Paid Search' AS campaign_name
    ,fiscal_month
    ,'Paid Search' AS format
    ,tactic AS tactic
    ,'n/a' AS media_type
    ,'Paid Search' AS channel
    ,date
    ,week
    ,week_begins
    ,week_ends
    ,media_spend AS expense
    ,0 AS gross_calls
    ,impressions AS impressions
    ,0 AS offered_calls
    ,clicks

    FROM search

    ) AS STAK

    GROUP BY
    dma
    ,year
    ,month
    ,business_unit
    ,campaign_name
    ,fiscal_month
    ,format
    ,tactic
    ,media_type
    ,date
    ,week
    ,week_begins
    ,week_ends