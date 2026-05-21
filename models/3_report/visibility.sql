WITH RAW_DATA AS (
    SELECT *
    FROM {{ ref('profound_visibility') }}
),

MANIP_DATA AS (
    SELECT date
        , source_table
        , category_id
        , category
        , asset_name
        , topic
        , model
        , region
        , prompt
        , personas
        , tags
        , mentions_count
        , visibility_score
        , share_of_voice
    FROM RAW_DATA
)

SELECT MANIP_DATA.date
    , CAST(DATE_TRUNC('week', MANIP_DATA.date + INTERVAL '1 day') - INTERVAL '1 day' AS date) AS week
    , MANIP_DATA.source_table
    , 'visibility' AS profound_insight
    , MANIP_DATA.category_id
    , MANIP_DATA.category
    , MANIP_DATA.asset_name
    , MANIP_DATA.topic
    , MANIP_DATA.model
    , MANIP_DATA.region
    , MANIP_DATA.prompt
    , MANIP_DATA.personas
    , MANIP_DATA.tags
    , MANIP_DATA.mentions_count AS brand_mentions
    , MANIP_DATA.visibility_score
    , MANIP_DATA.share_of_voice
    , MANIP_DATA.mentions_count AS occurrences
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