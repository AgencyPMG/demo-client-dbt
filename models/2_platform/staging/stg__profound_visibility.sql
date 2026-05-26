WITH RAW_DATA AS (
    SELECT date
        , source_table
        , category_id
        , category
        , asset_name
        , topic
        , model
        , region
        , prompt
        , SUM(mentions_count) AS mentions_count
    FROM {{ ref('base_profound_visibility') }}
    GROUP BY date
        , source_table
        , category_id
        , category
        , asset_name
        , topic
        , model
        , region
        , prompt
),

PERSONAS AS (
    SELECT date
        , source_table
        , asset_name
        , topic
        , model
        , region
        , prompt
        , LISTAGG(DISTINCT persona, ', ') WITHIN GROUP (ORDER BY persona) AS personas
    FROM {{ ref('base_profound_visibility') }}
    GROUP BY date
        , source_table
        , asset_name
        , topic
        , model
        , region
        , prompt
),

TAGS AS (
    SELECT date
        , source_table
        , asset_name
        , topic
        , model
        , region
        , prompt
        , LISTAGG(DISTINCT tags, ', ') WITHIN GROUP (ORDER BY tags) AS tags
    FROM {{ ref('base_profound_visibility') }}
    GROUP BY date
        , source_table
        , asset_name
        , topic
        , model
        , region
        , prompt
)

SELECT RAW_DATA.date
    , RAW_DATA.source_table
    , RAW_DATA.category_id
    , RAW_DATA.category
    , RAW_DATA.asset_name
    , RAW_DATA.topic
    , RAW_DATA.model
    , RAW_DATA.region
    , RAW_DATA.prompt
    , PERSONAS.personas
    , TAGS.tags
    , RAW_DATA.mentions_count
FROM RAW_DATA

LEFT JOIN PERSONAS
ON RAW_DATA.date = PERSONAS.date
AND RAW_DATA.asset_name = PERSONAS.asset_name
AND RAW_DATA.topic = PERSONAS.topic
AND RAW_DATA.model = PERSONAS.model
AND RAW_DATA.region = PERSONAS.region
AND RAW_DATA.prompt = PERSONAS.prompt

LEFT JOIN TAGS
ON RAW_DATA.date = TAGS.date
AND RAW_DATA.asset_name = TAGS.asset_name
AND RAW_DATA.topic = TAGS.topic
AND RAW_DATA.model = TAGS.model
AND RAW_DATA.region = TAGS.region
AND RAW_DATA.prompt = TAGS.prompt