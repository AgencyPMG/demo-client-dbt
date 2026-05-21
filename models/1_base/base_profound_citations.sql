WITH RAW_DATA AS (
    SELECT *
    FROM {{ source('demo','profound_citations') }}
),

MANIP_DATA AS (
    SELECT DISTINCT CAST(date AS date) AS date
        , CAST('demo.profound_citations' AS {{ type_string() }}) AS source_table
        , CAST(category_id AS {{ type_string() }}) AS category_id
        , CAST('greystar' AS {{ type_string() }}) AS category
        , LOWER(model) AS model
        , CASE WHEN LOWER(persona) = '(none)' THEN NULL
            ELSE LOWER(persona) END
            AS persona
        , LOWER(region) AS region
        , CASE WHEN LOWER("tag") = '(none)' THEN NULL
            ELSE LOWER("tag") END
            AS tags
        , LOWER(topic) AS topic
        , LOWER(url) AS url
        , CASE WHEN POSITION('//' IN url) > 0
            THEN SPLIT_PART( SPLIT_PART(url, '//', 2), '/', 1 )
            ELSE SPLIT_PART(url, '/', 1)
        END AS host_cleaned
        , count AS citations_count
    FROM RAW_DATA
)

SELECT date
    , source_table
    , category_id
    , category
    , model
    , persona
    , region
    , tags
    , topic
    , url
    , CASE WHEN host_cleaned LIKE '%.%.%'
        THEN SPLIT_PART(host_cleaned, '.', 2) || '.' || SPLIT_PART(host_cleaned, '.', 3)
        ELSE host_cleaned
    END AS host_name
    , citations_count
FROM MANIP_DATA