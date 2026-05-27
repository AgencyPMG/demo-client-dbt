WITH RAW_DATA AS (
    SELECT *
    FROM {{ ref('profound_citations') }}
),

MANIP_DATA AS (
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
        , occurrences
        , citation_share
    FROM RAW_DATA
)

SELECT date
    , CAST(DATE_TRUNC('week', date + INTERVAL '1 day') - INTERVAL '1 day' AS date) AS week
    , source_table
    , category_id
    , category
    , 'citations' AS profound_insight
    , model
    , topic
    , region
    , personas
    , tags
    , url
    , host_name
    , CASE WHEN {{ qualify ('demo.agentic_url_directory','(MANIP_DATA.url)')}} = 'unknown' THEN NULL
        ELSE {{ qualify ('demo.agentic_url_directory','(MANIP_DATA.url)')}} END AS citation_page_source
    , CASE WHEN {{ qualify ('demo.agentic_promo_content','(MANIP_DATA.url)')}} = 'unknown' THEN NULL
        ELSE {{ qualify ('demo.agentic_promo_content','(MANIP_DATA.url)')}} END AS citation_promo_content
    , CASE WHEN {{ qualify ('demo.media_type','(MANIP_DATA.url)')}} = 'unknown' THEN NULL
        ELSE {{ qualify ('demo.media_type','(MANIP_DATA.url)')}} END AS citation_media_type
    , SUM(citations_count) AS citations_count
    , SUM(occurrences) AS occurrences
    , AVG(citation_share) AS citation_share
FROM MANIP_DATA
GROUP BY date
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