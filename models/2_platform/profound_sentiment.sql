WITH RAW_DATA AS (
    SELECT *
    FROM {{ ref('stg__profound_sentiment') }}
),

TOTALS AS (
    SELECT date
        , source_table
        , category_id
        , category
        , asset_name
        , region
        , topic
        , model
        , prompt
        , SUM(positive) / SUM(occurrences) AS positive_sentiment
        , SUM(negative) / SUM(occurrences) AS negative_sentiment
        , (SUM(positive) - SUM(negative)) /SUM(occurrences) AS net_sentiment
    FROM RAW_DATA
    GROUP BY date
        , source_table
        , category_id
        , category
        , asset_name
        , region
        , topic
        , model
        , prompt
)

SELECT RAW_DATA.date
    , RAW_DATA.source_table
    , RAW_DATA.category_id
    , RAW_DATA.category
    , RAW_DATA.asset_name
    , RAW_DATA.region
    , RAW_DATA.topic
    , RAW_DATA.model
    , RAW_DATA.personas
    , RAW_DATA.tags
    , RAW_DATA.prompt
    , RAW_DATA.theme
    , RAW_DATA.sentiment_type
    , RAW_DATA.positive
    , RAW_DATA.negative
    , RAW_DATA.occurrences
    , TOTALS.positive_sentiment
    , TOTALS.negative_sentiment
    , TOTALS.net_sentiment
FROM RAW_DATA

LEFT JOIN TOTALS
ON RAW_DATA.date = TOTALS.date
AND RAW_DATA.category_id = TOTALS.category_id
AND RAW_DATA.asset_name = TOTALS.asset_name
AND RAW_DATA.region = TOTALS.region
AND RAW_DATA.topic = TOTALS.topic
AND RAW_DATA.model = TOTALS.model
AND RAW_DATA.prompt = TOTALS.prompt