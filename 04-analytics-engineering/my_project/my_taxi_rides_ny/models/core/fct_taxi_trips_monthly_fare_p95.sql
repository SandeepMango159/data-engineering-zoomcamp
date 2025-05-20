{{ 
    config(
        materialized = 'view'
    )
}}

with 
filtered_trips
 as (

    {# Join the fact trips table with the dim taxi trips for calender info #}
    select
        d.service_type,
        d.year,
        d.month,
        f.fare_amount,
        f.trip_distance,
        f.payment_type_description
    from 
        {{ ref('fact_trips') }} f
        join {{ ref('dim_taxi_trips') }} d
        using (tripid)

    {# Allow only valid entries, so filter out invalid ones #}
    where
        fare_amount > 0
        and trip_distance > 0
        and payment_type_description in ('Cash','Credit card')

), 
with_percentiles as (

    select
        service_type,
        year,
        month,
        {# Calcualate the percentile in the partition by service type, year and month
        The calculated result will be returned to every row #}
        percentile_cont(fare_amount, 0.97) over (
            partition by service_type, year, month
        ) as p97,

        percentile_cont(fare_amount, 0.95) over (
            partition by service_type, year, month
        ) as p95,

        percentile_cont(fare_amount, 0.90) over (
            partition by service_type, year, month
        ) as p90
    from filtered_trips
)
{# Then group by those partitions and get the aggregate value, can use min or max to pick it, 
because all rows share the same value in the partition #}
select
    service_type,
    year,
    month,
    min(p97) as p97,
    min(p95) as p95,
    min(p90) as p90
from with_percentiles
group by service_type, year, month
