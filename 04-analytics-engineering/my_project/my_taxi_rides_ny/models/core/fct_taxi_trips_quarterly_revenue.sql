{{ config(
    materialized = 'table'
    )
}}

{# First we join trips with all the calender info for each ride 
We keep the total amount for each ride and the calender infos #}
with trips as (

    
    select
        d.service_type,
        d.year,
        d.quarter,
        d.year_quarter,
        f.total_amount
    from 
        {{ ref('fact_trips') }} f
        join {{ ref('dim_taxi_trips') }} d
      on d.trip_id = f.trip_id

), 
{# Then from the trips above we group everything that belongs to the same service type and within the same year and within the same quarter
Keep year_quarter for readability purposes, but but year and quarter already make the row unique enough #}
quarterly_revenue as (
    {# 1. revenue per fleet-quarter-year #}
    select
        service_type,
        year,
        quarter,
        year_quarter,
        sum(total_amount) as revenue
    from trips
    group by 1,2,3,4
), 
year_over_year_revenue as (

    {# 2. Year over year growth = (this-year – last-year) / last-year to create % 
    Lag will get the previous year's revenue for the same service type and the same quarter 
    Lag will take from the same service type and quarter, and look at the years, since ordered by years, so it'll get the revenue of last year
    Safe divide will divide the difference of the current revenue with the previous revenue, by the previous revenue, to get the % #}
    select
        q.*,
        lag(revenue) over (
            partition by service_type, quarter
            order by year
        ) as prev_year_revenue,

        safe_divide(
            revenue - lag(revenue) over (
            partition by service_type, quarter
            order by year
            ),
            lag(revenue) over (
                partition by service_type, quarter
                order by year
            )
        ) as yoy_growth
    from quarterly_revenue q
    where year >= 2019               -- make sure a previous year exists
)

select *
from year_over_year_revenue;
