WITH RAW_DATA AS (
    SELECT *
    FROM {{ source('playground', 'calendar_demo') }}
)

SELECT CAST(date AS date) AS date
    , LOWER(CAST(fiscal_month AS {{ type_string() }})) AS fiscal_month
    , LOWER(CAST(week AS {{ type_string() }})) AS week
    , week_begins
    , week_ends
FROM RAW_DATA