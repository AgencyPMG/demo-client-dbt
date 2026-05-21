WITH RAW_DATA AS (
    SELECT *
    FROM {{ ref('profound_responses') }}
),

MANIP_DATA AS (
    SELECT date
        , category_id
        , category
        , model
        , region
        , prompt
        , nike_product_present
        , response_brands_in_order
        , nike_present
        , nike_response_rank
    FROM RAW_DATA
)

SELECT date
    , CAST(DATE_TRUNC('week', date + INTERVAL '1 day') - INTERVAL '1 day' AS date) AS week
    , category_id
    , category
    , model
    , region
    , prompt
    , nike_product_present
    , response_brands_in_order
    , nike_present
    , nike_response_rank
FROM MANIP_DATA