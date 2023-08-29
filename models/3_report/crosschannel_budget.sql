{{
  config(
    materialized = 'table'
  )
}}
WITH daily_budgets AS (
    SELECT * FROM {{ref('budget')}}
),
social AS (
    SELECT * FROM {{ref('social_channel')}}
),
search AS (
    SELECT * FROM {{ref('search_channel')}}
),
progo AS (
    SELECT * FROM {{ref('programmatic_channel')}}
),

local AS (
    SELECT * FROM {{ref('local_channel')}}
),

affiliates AS (
    SELECT * FROM {{ref('affiliate_channel')}}
)
SELECT date
    ,{{date_part('month','date')}} AS month
    ,{{date_diff('month_start','month_end')}} AS daysinmonth
    ,{{date_diff('current_date','month_end')}} + 1 AS daysremaining
    ,{{date_diff('month_start','current_date')}} AS dayspassed
    ,month_end
    ,month_start
    ,source_table
    ,channel
    ,platform
    ,geo
    ,campaign
    ,tactic
    ,SUM(cost) AS cost
    ,SUM(cost_budget) AS cost_budget



FROM (
     SELECT
        CAST(date AS date) AS date
        ,max(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_end
        ,min(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_start
        ,source_table
        ,channel
        ,platform
        ,geo
        ,campaign
        ,tactic
        ,current_spend_budget AS cost_budget
        ,0 AS cost

     FROM daily_budgets

     UNION ALL

     SELECT
        CAST(date AS date) AS date
        ,max(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_end
        ,min(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_start
        ,source_table
        ,channel
        ,platform
        ,nat_flag AS geo
        ,{{ qualify('nothing_bundt_cakes.budget_campaign(campaign_name)') }} AS campaign
        ,tactic
        ,0 AS cost_budget
        ,cost


     FROM search

     UNION ALL

     SELECT
         CAST(date AS date) AS date
        ,max(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_end
        ,min(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_start
        ,source_table
        ,channel
        ,platform
        ,nat_flag AS geo
        ,{{ qualify('nothing_bundt_cakes.budget_campaign(campaign_name)') }} AS campaign
        ,tactic
        ,0 AS cost_budget
        ,cost

     FROM progo

     UNION ALL

     SELECT
        CAST(date AS date) AS date
        ,max(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_end
        ,min(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_start
        ,source_table
        ,channel
        ,platform
        ,nat_flag AS geo
        ,{{ qualify('nothing_bundt_cakes.budget_campaign(campaign_name)') }} AS campaign
        ,tactic
        ,0 AS cost_budget
        ,cost

     FROM social

     UNION ALL

     SELECT
         CAST(date AS date) AS date
        ,max(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_end
        ,min(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_start
        ,source_table
        ,channel
        ,platform
        ,'n/a' AS geo
        ,{{qualify('nothing_bundt_cakes.budget_campaign(campaign_name)') }} AS campaign
        ,'n/a' AS tactic
        ,0 AS cost_budget
        ,cost

     FROM local

     UNION ALL

     SELECT
         CAST(date AS date) AS date
        ,max(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_end
        ,min(date) over (partition by {{date_part('month','date')}}, {{date_part('year','date')}}) as month_start
        ,source_table
        ,channel
        ,platform
        ,'n/a' AS geo
        ,{{ qualify('nothing_bundt_cakes.budget_campaign(campaign_name)') }} AS campaign
        ,'n/a' AS tactic
        ,0 AS cost_budget
        ,cost

     FROM affiliates

) AS STAK
GROUP BY date
    ,month_end
    ,month_start
    ,source_table
    ,channel
    ,platform
    ,geo
    ,campaign
    ,tactic
