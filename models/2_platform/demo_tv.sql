WITH RAW_DATA AS (
    SELECT *
    FROM {{ ref('base_tv_demo') }}
),


transformed AS (
    SELECT dma
        , broadcast_year
        , broadcast_month
        , broadcast_period
        , broadcast_week_full_name
        , business_unit
        , campaign_name
        , fiscal_month
        , format
        , media_tactic
        , media_type
        , date
        , week
        , week_begins AS week_begins
        , week_ends AS week_ends
        , SUM(expense) AS expense
        , SUM(gross_calls) AS gross_calls
        , SUM(grp_circ_impr) AS grp_circ_impr
        , SUM(offered_calls) AS offered_calls
    FROM RAW_DATA
    GROUP BY dma
        , broadcast_year
        , broadcast_month
        , broadcast_period
        , broadcast_week_full_name
        , business_unit
        , campaign_name
        , fiscal_month
        , format
        , media_tactic
        , media_type
        , date
        , week
        , week_begins
        , week_ends
)

SELECT *
FROM transformed