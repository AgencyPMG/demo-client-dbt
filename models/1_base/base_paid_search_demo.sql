WITH RAW_DATA AS (
    SELECT day AS date
        , month
        , metro_area_matched_
        , dma_region_matched_
        , brand_or_nb
        , impr_
        , clicks
        , cost
    FROM {{ source('demo', 'paid_search_demo') }}
)

SELECT CAST(date AS date) AS date
    , EXTRACT(MONTH FROM date) AS month
    , EXTRACT(YEAR FROM date) AS year
    , LOWER(metro_area_matched_) AS metro_area_matched
    , LOWER(dma_region_matched_) AS dma
    , LOWER(brand_or_nb) AS tactic
    , impr_ AS impressions
    , clicks
    , cost AS media_spend
FROM RAW_DATA