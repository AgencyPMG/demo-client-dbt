{{
config(
materialized = 'table'
)
}}

WITH search AS (
SELECT * FROM {{ source('playground','paid_search_demo') }}
)



SELECT *
FROM search