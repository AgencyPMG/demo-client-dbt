WITH RAW_DATA AS (
    SELECT *
    FROM {{ source('demo','profound_responses') }}
)

SELECT DISTINCT CAST(created_at AS date) AS date
    , CAST('demo.profound_responses' AS {{ type_string() }}) AS source_table
    , CAST(category_id AS {{ type_string() }}) AS category_id
    , CAST('greystar' AS {{ type_string() }}) AS category
    , CAST(model_id AS {{ type_string() }}) AS model_id
    , LOWER(model) AS model
    , LOWER(region) AS region
    , LOWER(prompt) AS prompt
    , LOWER(REGEXP_REPLACE(response, '[\r\n]+', ' ')) AS response
    , LOWER(brand_present) AS brand_present
    , CAST(brand_rank_among_brands AS {{ type_float() }}) AS  brand_response_rank
    , LOWER(brands_in_order) AS response_brands_in_order
FROM RAW_DATA