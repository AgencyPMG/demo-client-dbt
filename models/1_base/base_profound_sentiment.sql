WITH RAW_DATA AS (
    SELECT *
    FROM {{ source('demo','profound_sentiment') }}
)

SELECT DISTINCT CAST(date AS date) AS date
    , CAST('demo.profound_sentiment' AS {{ type_string() }}) AS source_table
    , CAST(category_id AS {{ type_string() }}) AS category_id
    , CAST('greystar' AS {{ type_string() }}) AS category
    , LOWER(asset_name) AS asset_name
    , LOWER(model) AS model
    , CASE WHEN LOWER(persona) = '(none)' THEN NULL
        ELSE LOWER(persona) END
        AS persona
    , LOWER(prompt) AS prompt
    , LOWER(region) AS region
    , LOWER(sentiment_type) AS sentiment_type
    , CASE WHEN LOWER("tag") = '(none)' THEN NULL
        ELSE LOWER("tag") END
        AS tags
    , LOWER(theme) AS theme
    , LOWER(topic) AS topic
    , occurrences
    , negative
    , positive
FROM RAW_DATA