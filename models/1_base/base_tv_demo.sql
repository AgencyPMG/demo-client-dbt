WITH RAW_DATA AS (
    SELECT *
    FROM {{ source('demo','tv_demo') }}
)

SELECT LOWER(dma) AS dma
    , CAST(broadcast_year AS {{ type_string() }}) AS broadcast_year
    , CAST(LOWER(broadcast_month) AS {{ type_string() }}) AS broadcast_month
    , CAST(broadcast_period AS {{ type_string() }}) AS broadcast_period
    , CAST(LOWER(broadcast_week_full_name) AS {{ type_string() }}) AS broadcast_week_full_name
    , CAST(LOWER(business_unit) AS {{ type_string() }}) AS business_unit
    , CAST(LOWER(campaign_name) AS {{ type_string() }}) AS campaign_name
    , CAST(fiscal_month AS {{ type_string() }}) AS fiscal_month
    , CAST(LOWER(format) AS {{ type_string() }}) AS format
    , CAST(LOWER(media_tactic) AS {{ type_string() }}) AS media_tactic
    , CAST(LOWER(media_type) AS {{ type_string() }}) AS media_type
    , CAST(period AS date) AS date
    , REGEXP_SUBSTR(broadcast_week_full_name, 'Wk [0-9]+') AS week
    , REGEXP_SUBSTR(broadcast_week_full_name, '[0-9]{4}-[0-9]{2}-[0-9]{2}') AS week_begins
    , REGEXP_SUBSTR(broadcast_week_full_name, '[0-9]{4}-[0-9]{2}-[0-9]{2}$') AS week_ends
    , expense AS expense
    , gross_calls AS gross_calls
    , grp_circ_impr AS grp_circ_impr
    , offered_calls AS offered_calls
FROM RAW_DATA