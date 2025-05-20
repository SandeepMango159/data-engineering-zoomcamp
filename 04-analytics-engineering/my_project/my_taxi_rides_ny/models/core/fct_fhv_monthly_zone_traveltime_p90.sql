{{ 
    config(
        materialized='view'
    )
}}
{# Gets the trips and computes timestamp diff #}
with fhv_trips as (

    select
        trip_id,
        service_type,
        year,
        month,
        pickup_locationid,
        pickup_zone,
        dropoff_locationid,
        dropoff_zone,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
),
{# Calculats p90 percentile over the partition #}
with_percentiles as (

    select
        trip_id,
        service_type,
        year,
        month,
        pickup_zone,
        dropoff_zone,
        percentile_cont(trip_duration, 0.90)
          over (partition by year, month, pickup_locationid, dropoff_locationid) as p90
    from fhv_trips
)
{# Distinct makes sure it's one row per year, month, pickupzone,dropoffzone #}
select distinct
    year,
    month,
    pickup_zone,
    dropoff_zone,
    p90
from with_percentiles
