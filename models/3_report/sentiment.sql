WITH RAW_DATA AS (
    SELECT *
    FROM {{ ref('profound_sentiment') }}
),

MANIP_DATA AS (
    SELECT date
        , source_table
        , category_id
        , category
        , asset_name
        , region
        , topic
        , model
        , personas
        , tags
        , prompt
        , theme
        , sentiment_type
        , occurrences
        , negative
        , positive
        , positive_sentiment
        , negative_sentiment
        , net_sentiment
    FROM RAW_DATA
)

SELECT MANIP_DATA.date
    , CAST(DATE_TRUNC('week', MANIP_DATA.date + INTERVAL '1 day') - INTERVAL '1 day' AS date) AS week
    , MANIP_DATA.source_table
    , 'sentiment' AS profound_insight
    , MANIP_DATA.category_id
    , MANIP_DATA.category
    , MANIP_DATA.asset_name
    , MANIP_DATA.region
    , MANIP_DATA.topic
    , MANIP_DATA.model
    , MANIP_DATA.personas
    , MANIP_DATA.tags
    , MANIP_DATA.prompt
    , MANIP_DATA.theme
    , MANIP_DATA.sentiment_type
    , MANIP_DATA.occurrences
    , MANIP_DATA.negative
    , MANIP_DATA.positive
    , MANIP_DATA.positive_sentiment
    , MANIP_DATA.negative_sentiment
    , MANIP_DATA.net_sentiment
    , RESPONSES.nike_product_present
    , RESPONSES.response_brands_in_order
    , RESPONSES.nike_present
    , RESPONSES.nike_response_rank
FROM MANIP_DATA

LEFT JOIN RESPONSES

ON MANIP_DATA.date = RESPONSES.date
AND MANIP_DATA.category_id = RESPONSES.category_id
AND MANIP_DATA.model = RESPONSES.model
AND MANIP_DATA.region = RESPONSES.region
AND MANIP_DATA.prompt = RESPONSES.prompt
AND MANIP_DATA.prompt IS NOT NULL