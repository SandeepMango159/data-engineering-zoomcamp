{{ 
    config(materialized='view'
    ) 
}}

with y2020 as (
    select *
    from 
        {{ ref('fct_taxi_trips_quarterly_revenue') }}
    where 
        year = 2020
)
select
    service_type,
    {# Best = highest (least-negative) yoy #}
    (
        select year_quarter
        from y2020 y
        where y.service_type = t.service_type
        order by yoy_growth desc
        limit 1)  
    as best_q,

    {# Worst = lowest (most-negative) yoy #}
    (
        select year_quarter
        from y2020 y
        where y.service_type = t.service_type
        order by yoy_growth asc
        limit 1)
    as worst_q
from 
{# Distinct will return two rows, one containing green and one yellow, gets treated as a temporary table named t
Now the outer query will execute once for each row in t
and in the subqueries we reference the current t.service type #}
    (select distinct service_type 
        from y2020) t