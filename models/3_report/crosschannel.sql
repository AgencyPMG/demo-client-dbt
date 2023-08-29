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
    ,tactic
    ,fiscal_month
    ,format
    ,media_tactic
    ,media_type
    ,date
    ,week
    ,week_begins
    ,week_ends
    ,SUM(impressions) AS impressions
    ,SUM(clicks) AS clicks
    ,SUM(media_spend) AS media_spend
    ,SUM(expense) AS expense
    ,SUM(gross_calls) AS gross_calls
    ,SUM(grp_circ_impr) AS grp_circ_impr
    ,SUM(offered_calls) AS offered_calls

FROM

(
    SELECT
     dma AS dma
    ,broadcast_year AS year
    ,broadcast_month AS month
    ,business_unit AS business_unit
    ,campaign_name AS campaign_name
    ,'n/a' AS tactic
    ,fiscal_month AS fiscal_month
    ,format AS format
    ,media_tactic AS media_tactic
    ,media_type AS media_type
    ,CAST(date AS date) AS date
    ,week
    ,week_begins
    ,week_ends
    ,0 AS impressions
    ,0 AS clicks
    ,0 AS media_spend
    ,expense
    ,gross_calls
    ,grp_circ_impr
    ,offered_calls

    FROM tv

    UNION ALL

    SELECT

    dma
    ,year
    ,month
    ,'n/a' AS business_unit
    ,'n/a' AS campaign_name
    ,tactic
    ,fiscal_month
    ,'n/a' AS format
    ,'n/a' AS media_tactic
    ,'n/a' AS media_type
    ,date
    ,week
    ,week_begins
    ,week_ends
    ,impressions
    ,clicks
    ,media_spend
    ,0 AS expense
    ,0 AS gross_calls
    ,0 AS grp_circ_impr
    ,0 AS offered_calls

    ) AS STAK

    GROUP BY
    dma
    ,year
    ,month
    ,business_unit
    ,campaign_name
    ,tactic
    ,fiscal_month
    ,format
    ,media_tactic
    ,media_type
    ,date
    ,week
    ,week_begins
    ,week_ends