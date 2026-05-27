WITH RAW_DATA AS (
    SELECT DISTINCT date
        , category_id
        , category
        , model
        , region
        , prompt
        , response
        , response_brands_in_order
        , brand_present
        , brand_response_rank
    FROM {{ ref('base_profound_responses') }}
)

SELECT date
    , category_id
    , category
    , model
    , region
    , prompt
    , {{ qualify ('demo.agentic_response_assets','(response)')}} AS product_present
    , response_brands_in_order
    , brand_present
    , brand_response_rank
FROM RAW_DATA WUTITDO
QUALIFY  ROW_NUMBER()
OVER (PARTITION BY prompt, model, region, category_id, date ORDER BY prompt, model, region, category_id, date) = 1