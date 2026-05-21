WITH RAW_DATA AS (
    SELECT *
    FROM {{ ref('stg__profound_visibility') }}
),

TOTALS AS (
    SELECT
          date
        , category_id
        , category
        , model
        , region
        , COUNT(DISTINCT prompt) AS total_prompts
        , SUM(mentions_count) AS total_mentions
    FROM raw_data
    GROUP BY
          date
        , category_id
        , category
        , model
        , region
),

ASSET_TOTALS AS (
    SELECT date
        , category_id
        , category
        , model
        , region
        , asset_name
        , COUNT(DISTINCT prompt) AS asset_prompts
        , SUM(mentions_count) AS asset_mentions
    FROM raw_data
    GROUP BY
          date
        , category_id
        , category
        , model
        , region
        , asset_name
)

SELECT RAW_DATA.date
    , RAW_DATA.source_table
    , RAW_DATA.category_id
    , RAW_DATA.category
    , RAW_DATA.asset_name
    , RAW_DATA.model
    , RAW_DATA.prompt
    , RAW_DATA.personas
    , RAW_DATA.tags
    , RAW_DATA.region
    , RAW_DATA.topic
    , RAW_DATA.mentions_count
    , COALESCE(ASSET_TOTALS.asset_prompts, 0)  AS asset_prompts
    , COALESCE(ASSET_TOTALS.asset_mentions, 0) AS asset_mentions
    , COALESCE(TOTALS.total_prompts, 0)  AS total_prompts
    , COALESCE(TOTALS.total_mentions, 0) AS total_mentions
    , COALESCE(ASSET_TOTALS.asset_prompts, 0)::DECIMAL(18,6) / NULLIF(TOTALS.total_prompts, 0)  AS visibility_score
    , COALESCE(ASSET_TOTALS.asset_mentions, 0)::DECIMAL(18,6) / NULLIF(TOTALS.total_mentions, 0) AS share_of_voice
FROM RAW_DATA

LEFT JOIN ASSET_TOTALS
ON RAW_DATA.date = ASSET_TOTALS.date
AND RAW_DATA.category_id = ASSET_TOTALS.category_id
AND RAW_DATA.model = ASSET_TOTALS.model
AND RAW_DATA.region = ASSET_TOTALS.region
AND RAW_DATA.asset_name = ASSET_TOTALS.asset_name

LEFT JOIN TOTALS
ON RAW_DATA.date = TOTALS.date
AND RAW_DATA.category_id = TOTALS.category_id
AND RAW_DATA.model = TOTALS.model
AND RAW_DATA.region = TOTALS.region