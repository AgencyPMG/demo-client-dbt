WITH RAW_DATA AS (
    SELECT date
        , source_table
        , category_id
        , category
        , model
        , topic
        , region
        , url
        , host_name
        , SUM(citations_count) AS citations_count
    FROM {{ ref('base_profound_citations') }}
    GROUP BY date
        , source_table
        , category_id
        , category
        , model
        , topic
        , region
        , url
        , host_name
),

PERSONA AS (
    SELECT LISTAGG(DISTINCT persona, ', ') WITHIN GROUP (ORDER BY persona) AS personas
        , date
        , model
        , topic
        , region
        , url
        , host_name
    FROM {{ ref('base_profound_citations') }}
    GROUP BY date
        , model
        , topic
        , region
        , url
        , host_name
),

TAGS AS (
    SELECT LISTAGG(DISTINCT tags, ', ') WITHIN GROUP (ORDER BY tags) AS tags
        , date
        , model
        , topic
        , region
        , url
        , host_name
    FROM {{ ref('base_profound_citations') }}
    GROUP BY date
        , model
        , topic
        , region
        , url
        , host_name
)

SELECT RAW_DATA.date
    , RAW_DATA.source_table
    , RAW_DATA.category_id
    , RAW_DATA.category
    , RAW_DATA.model
    , RAW_DATA.topic
    , PERSONA.personas
    , RAW_DATA.region
    , TAGS.tags
    , RAW_DATA.url
    , RAW_DATA.host_name
    , RAW_DATA.citations_count
FROM RAW_DATA

LEFT JOIN TAGS
ON RAW_DATA.date = TAGS.date
AND RAW_DATA.model = TAGS.model
AND RAW_DATA.topic = TAGS.topic
AND RAW_DATA.region = TAGS.region
AND RAW_DATA.url = TAGS.url

LEFT JOIN PERSONA
ON RAW_DATA.date = PERSONA.date
AND RAW_DATA.model = PERSONA.model
AND RAW_DATA.topic = PERSONA.topic
AND RAW_DATA.region = PERSONA.region
AND RAW_DATA.url = PERSONA.url