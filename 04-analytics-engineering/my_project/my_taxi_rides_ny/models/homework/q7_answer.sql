{{ 
    config(
        materialized='view'
    )
}}
{# Gets all the trips from 2019 november in the pick up areas #}
with november_2019_trips as (
  select
    pickup_zone,
    dropoff_zone,
    p90
  from {{ ref('fct_fhv_monthly_zone_traveltime_p90') }}
  where year  = 2019
    and month = 11
    and pickup_zone in (
      'Newark Airport',
      'SoHo',
      'Yorkville East'
    )
),
{# From those trips, we get the columns and we give a row number per pickup zone sorted by the p90,  #}
ranked as (
  select
    pickup_zone,
    dropoff_zone,
    p90,
    row_number() over (
      partition by pickup_zone
      order by p90 desc
    ) as rn
  from november_2019_trips
)
{# Then we slect every pickupzone where the row numer was 2, will give us the pickup zone and the 2nd biggest p90 #}
select
  pickup_zone,
  dropoff_zone as second_longest_p90_zone,
  p90 as second_longest_p90_seconds
from ranked
where rn = 2
order by pickup_zone