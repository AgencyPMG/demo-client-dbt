{{
config(
materialized = 'table'
)
}}

WITH search AS (
SELECT * FROM {{ source('playground','paid_search_demo') }}
),

cal AS (
SELECT * FROM {{ source('playground','calendar_demo') }}
),

transformed AS (


SELECT
    RAW_DATA.date AS date
    ,month
    ,dma
    ,tactic
    ,fiscal_month
    ,week
    ,week_begins
    ,week_ends
    ,SUM(impressions) AS impressions
    ,SUM(clicks) AS clicks
    ,SUM(media_spend) AS media_spend



FROM

    (
    SELECT
         cast(day AS date) AS date
        ,SUBSTR(month, 1, INSTR(month, '-') - 1) AS month
        ,metro_area__matched_ AS metro_area_matched
        ,CONCAT(SUBSTR(dma_region__matched_, 1, INSTR(dma_region__matched_, ' ') - 1),', ',SUBSTR(dma_region__matched_, INSTR(dma_region__matched_, ' ') + 1)) AS dma
        ,brand_or_nb AS tactic
        ,impr_ AS impressions
        ,clicks AS clicks
        ,cost AS media_spend


    FROM search

    ) AS RAW_DATA

    LEFT JOIN (
        SELECT
        CAST(date AS date) AS date
        ,fiscal_month AS fiscal_month
        ,week AS week
        ,week_begins AS week_begins
        ,week_ends AS week_ends
        FROM cal
        ) AS CAL
        ON Cast(RAW_DATA.date as date) = Cast(CAL.date as date)

    GROUP BY
       RAW_DATA.date
    ,month
    ,dma
    ,tactic
    ,fiscal_month
    ,week
    ,week_begins
    ,week_ends

    )
SELECT *
FROM transformed