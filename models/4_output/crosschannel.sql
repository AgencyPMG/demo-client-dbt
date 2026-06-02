WITH SEARCH AS (
    SELECT *
    FROM {{ ref('demo_search') }}
),

TV AS (
    SELECT *
    FROM {{ ref('demo_tv') }}
),

STAK AS (
    SELECT dma AS dma
        , CAST(broadcast_year AS {{ type_float() }}) AS year
        , CAST(broadcast_month AS {{ type_float() }}) AS month
        , business_unit AS business_unit
        , campaign_name AS campaign_name
        , fiscal_month AS fiscal_month
        , format AS format
        , media_tactic AS tactic
        , media_type AS media_type
        , 'video' AS channel
        , CAST(date AS date) AS date
        , CAST(week AS {{ type_float() }}) AS week
        , CAST(week_begins AS date) AS week_begins
        , CAST(week_ends AS date) AS week_ends
        , expense AS expense
        , gross_calls
        , grp_circ_impr AS impressions
        , offered_calls
        , 0 AS clicks
    FROM TV

    UNION ALL

    SELECT dma
        , CAST(year AS {{ type_float() }}) AS year
        , CAST(month AS {{ type_float() }}) AS month
        , 'n/a' AS business_unit
        , 'paid search' AS campaign_name
        , fiscal_month
        , 'paid search' AS format
        , tactic AS tactic
        , 'n/a' AS media_type
        , 'paid search' AS channel
        , CAST(date AS date) AS date
        , CAST(week AS {{ type_float() }}) AS week
        , CAST(week_begins AS date) AS week_begins
        , CAST(week_ends AS date) AS week_ends
        , media_spend AS expense
        , 0 AS gross_calls
        , impressions AS impressions
        , 0 AS offered_calls
        , clicks
    FROM SEARCH
)

SELECT dma
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
    ,channel
    ,SUM(impressions) AS impressions
    ,SUM(clicks) AS clicks
    ,SUM(expense) AS expense
    ,SUM(gross_calls) AS gross_calls
    ,SUM(offered_calls) AS offered_calls
FROM STAK
GROUP BY dma
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
    ,channel