WITH RAW_DATA AS (
    SELECT *
    FROM {{ ref('base_paid_search_demo') }}
),

CAL AS (
    SELECT *
    FROM {{ ref('base_calendar_demo') }}
),

JOINED AS (
    SELECT RAW_DATA.date
        , RAW_DATA.month
        , RAW_DATA.year
        , RAW_DATA.dma
        , RAW_DATA.tactic
        , CAL.fiscal_month
        , CAL.week
        , CAL.week_begins
        , CAL.week_ends
        , RAW_DATA.impressions
        , RAW_DATA.clicks
        , RAW_DATA.media_spend
    FROM RAW_DATA
    LEFT JOIN CAL
    ON RAW_DATA.date = CAL.date
)

SELECT date
    , month
    , year
    , dma
    , tactic
    , fiscal_month
    , week
    , CAST(week_begins AS date) AS week_begins
    , CAST(week_ends AS date) AS week_ends
    , SUM(impressions) AS impressions
    , SUM(clicks) AS clicks
    , SUM(media_spend) AS media_spend
FROM JOINED
GROUP BY date
    , month
    , year
    , dma
    , tactic
    , fiscal_month
    , week
    , week_begins
    , week_ends