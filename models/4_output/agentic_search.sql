WITH CITATIONS AS (
    SELECT date
        , week
        , source_table
        , profound_insight
        , category_id
        , category
        , model
        , personas
        , region
        , tags
        , topic
        , host_name
        , url
        , citation_page_source
        , citation_promo_content
        , citation_media_type
        , citations_count
        , occurrences
        , citation_share
    FROM {{ ref('citations') }}
),

VISIBILITY AS (
    SELECT date
        , week
        , source_table
        , profound_insight
        , category_id
        , category
        , asset_name
        , model
        , personas
        , prompt
        , region
        , tags
        , topic
        , nike_product_present
        , response_brands_in_order
        , nike_present
        , nike_response_rank
        , brand_mentions
        , visibility_score
        , share_of_voice
        , occurrences
    FROM {{ ref('visibility') }}
),

SENTIMENT AS (
    SELECT date
        , week
        , source_table
        , profound_insight
        , category_id
        , category
        , asset_name
        , model
        , personas
        , prompt
        , region
        , sentiment_type
        , tags
        , theme
        , nike_product_present
        , response_brands_in_order
        , nike_present
        , nike_response_rank
        , occurrences
        , negative
        , positive
        , positive_sentiment
        , negative_sentiment
        , net_sentiment
    FROM {{ ref('sentiment') }}
),

STAK AS (
    SELECT date
        , week
        , source_table
        , profound_insight
        , category_id
        , category
        , asset_name
        , model
        , personas
        , prompt
        , region
        , sentiment_type
        , tags
        , theme
        , {{ default_null() }} AS topic
        , {{ default_null() }} AS host_name
        , {{ default_null() }} AS url
        , {{ default_null() }} AS citation_page_source
        , {{ default_null() }} AS citation_promo_content
        , {{ default_null() }} AS citation_media_type
        , nike_product_present
        , response_brands_in_order
        , nike_present
        , nike_response_rank
        , occurrences
        , negative
        , positive
        , positive_sentiment
        , negative_sentiment
        , net_sentiment
        , 0 AS citations_count
        , 0 AS brand_mentions
        , 0 AS visibility_score
        , 0 AS share_of_voice
        , 0 AS citation_share
    FROM SENTIMENT

    UNION ALL

    SELECT date
        , week
        , source_table
        , profound_insight
        , category_id
        , category
        , asset_name
        , model
        , personas
        , prompt
        , region
        , {{ default_null() }} AS sentiment_type
        , tags
        , {{ default_null() }} AS theme
        , topic
        , {{ default_null() }} AS host_name
        , {{ default_null() }} AS url
        , {{ default_null() }} AS citation_page_source
        , {{ default_null() }} AS citation_promo_content
        , {{ default_null() }} AS citation_media_type
        , nike_product_present
        , response_brands_in_order
        , nike_present
        , nike_response_rank
        , occurrences
        , 0 AS negative
        , 0 AS positive
        , 0 AS positive_sentiment
        , 0 AS negative_sentiment
        , 0 AS net_sentiment
        , 0 AS citations_count
        , brand_mentions
        , visibility_score
        , share_of_voice
        , 0 AS citation_share
    FROM VISIBILITY

    UNION ALL

    SELECT date
        , week
        , source_table
        , profound_insight
        , category_id
        , category
        , {{ default_null() }} AS asset_name
        , model
        , personas
        , {{ default_null() }} AS prompt
        , region
        , {{ default_null() }} AS sentiment_type
        , tags
        , {{ default_null() }} AS theme
        , topic
        , host_name
        , url
        , citation_page_source
        , citation_promo_content
        , citation_media_type
        , {{ default_null() }} AS nike_product_present
        , {{ default_null() }} AS response_brands_in_order
        , {{ default_null() }} AS nike_present
        , 0 AS nike_response_rank
        , occurrences
        , 0 AS negative
        , 0 AS positive
        , 0 AS positive_sentiment
        , 0 AS negative_sentiment
        , 0 AS net_sentiment
        , citations_count
        , 0 AS brand_mentions
        , 0 AS visibility_score
        , 0 AS share_of_voice
        , citation_share
    FROM CITATIONS
)

SELECT STAK.date
    , STAK.week
    , STAK.source_table
    , STAK.profound_insight
    , STAK.category_id
    , STAK.category
    , STAK.asset_name
    , CASE WHEN {{ qualify ('demo.agentic_competitors','(STAK.asset_name)')}} = 'unknown' THEN NULL
        ELSE {{ qualify ('demo.agentic_competitors','(STAK.asset_name)')}} END AS nike_competitor
    , STAK.model
    , CASE WHEN {{ qualify ('demo.agentic_model_tiers','(STAK.model)')}} = 'unknown' THEN NULL
        ELSE {{ qualify ('demo.agentic_model_tiers','(STAK.model)')}} END AS model_tier
    , STAK.personas
    , STAK.prompt
    , STAK.region
    , STAK.sentiment_type
    , STAK.tags
    , STAK.theme
    , STAK.topic
    , STAK.host_name
    , STAK.url
    , STAK.nike_product_present
    , STAK.citation_page_source
    , STAK.citation_promo_content
    , STAK.citation_media_type
    , STAK.response_brands_in_order
    , STAK.nike_present
    , STAK.nike_response_rank
    , STAK.occurrences
    , STAK.negative
    , STAK.positive
    , STAK.brand_mentions
    , STAK.citations_count
    , STAK.visibility_score
    , STAK.share_of_voice
    , STAK.citation_share
    , STAK.positive_sentiment
    , STAK.negative_sentiment
    , STAK.net_sentiment
FROM STAK