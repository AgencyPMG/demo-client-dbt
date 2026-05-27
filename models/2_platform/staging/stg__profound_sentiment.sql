WITH RAW_DATA AS (
    SELECT date
        , source_table
        , category_id
        , category
        , asset_name
        , region
        , topic
        , model
        , prompt
        , theme
        , sentiment_type
        , SUM(positive) AS positive
        , SUM(negative) AS negative
        , SUM(occurrences) AS occurrences
    FROM {{ ref('base_profound_sentiment') }}
    GROUP BY date
        , source_table
        , category_id
        , category
        , asset_name
        , region
        , topic
        , model
        , prompt
        , theme
        , sentiment_type
),

PERSONAS AS (
    SELECT date
        , asset_name
        , region
        , topic
        , model
        , LISTAGG(DISTINCT persona, ', ') WITHIN GROUP (ORDER BY persona) AS personas
        , prompt
        , theme
        , sentiment_type
    FROM {{ ref('base_profound_sentiment') }}
    GROUP BY date
        , asset_name
        , region
        , topic
        , model
        , prompt
        , theme
        , sentiment_type
),

TAGS AS (
    SELECT date
        , asset_name
        , region
        , topic
        , model
        , LISTAGG(DISTINCT tags, ', ') WITHIN GROUP (ORDER BY tags) AS tags
        , prompt
        , theme
        , sentiment_type
    FROM {{ ref('base_profound_sentiment') }}
    GROUP BY date
        , asset_name
        , region
        , topic
        , model
        , prompt
        , theme
        , sentiment_type
)

SELECT RAW_DATA.date
    , RAW_DATA.source_table
    , RAW_DATA.category_id
    , RAW_DATA.category
    , RAW_DATA.asset_name
    , RAW_DATA.region
    , RAW_DATA.topic
    , RAW_DATA.model
    , PERSONAS.personas
    , TAGS.tags
    , RAW_DATA.prompt
    , RAW_DATA.theme
    , RAW_DATA.sentiment_type
    , RAW_DATA.positive
    , RAW_DATA.negative
    , RAW_DATA.occurrences
FROM RAW_DATA

LEFT JOIN PERSONAS
ON RAW_DATA.date = PERSONAS.date
AND RAW_DATA.asset_name = PERSONAS.asset_name
AND RAW_DATA.region = PERSONAS.region
AND RAW_DATA.topic = PERSONAS.topic
AND RAW_DATA.model = PERSONAS.model
AND RAW_DATA.prompt = PERSONAS.prompt
AND RAW_DATA.theme = PERSONAS.theme
AND RAW_DATA.sentiment_type = PERSONAS.sentiment_type

LEFT JOIN TAGS
ON RAW_DATA.date = TAGS.date
AND RAW_DATA.asset_name = TAGS.asset_name
AND RAW_DATA.region = TAGS.region
AND RAW_DATA.topic = TAGS.topic
AND RAW_DATA.model = TAGS.model
AND RAW_DATA.prompt = TAGS.prompt
AND RAW_DATA.theme = TAGS.theme
AND RAW_DATA.sentiment_type = TAGS.sentiment_type