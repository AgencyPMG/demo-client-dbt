WITH RAW_DATA AS (
    SELECT date
        , source_table
        , category_id
        , category
        , model
        , topic
        , region
        , personas
        , tags
        , url
        , host_name
        , citations_count
    FROM {{ ref('stg__profound_citations') }}
),

HOSTNAME_TOTALS AS (
    SELECT date
        , category_id
        , topic
        , region
        , host_name
        , SUM(citations_count) AS citations_count
    FROM RAW_DATA
    GROUP BY date
        , category_id
        , topic
        , region
        , host_name
),

TOTALS AS (
    SELECT date
        , category_id
        , topic
        , region
        , SUM(citations_count) AS total_citations
    FROM RAW_DATA
    GROUP BY date
        , category_id
        , topic
        , region
)

SELECT RAW_DATA.date
    , RAW_DATA.source_table
    , RAW_DATA.category_id
    , RAW_DATA.category
    , RAW_DATA.model
    , RAW_DATA.topic
    , RAW_DATA.region
    , RAW_DATA.personas
    , RAW_DATA.tags
    , RAW_DATA.url
    , RAW_DATA.host_name
    , RAW_DATA.citations_count
    , RAW_DATA.citations_count AS occurrences
    , HOSTNAME_TOTALS.citations_count::DECIMAL(18,6) / NULLIF(TOTALS.total_citations, 0) AS citation_share
FROM RAW_DATA

LEFT JOIN HOSTNAME_TOTALS
ON RAW_DATA.date = HOSTNAME_TOTALS.date
AND RAW_DATA.category_id = HOSTNAME_TOTALS.category_id
AND RAW_DATA.topic = HOSTNAME_TOTALS.topic
AND RAW_DATA.region = HOSTNAME_TOTALS.region
AND RAW_DATA.host_name = HOSTNAME_TOTALS.host_name

LEFT JOIN TOTALS
ON RAW_DATA.date = TOTALS.date
AND RAW_DATA.category_id = TOTALS.category_id
AND RAW_DATA.topic = TOTALS.topic
AND RAW_DATA.region = TOTALS.region