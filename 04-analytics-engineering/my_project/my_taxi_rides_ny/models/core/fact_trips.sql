{# Fact trips table to put together all the green and yellow tripdata, and encase it with the dim zones table to make it more readable #}

{# Persistent table because this is a big table, joining lots of data, so table is better for more performant and efficient queries #}
{{
  config(
    materialized = 'table',
    )
}}

{# Get the green data, add service type Green to identify it form the yellow ones #}
with green_tripdata as (
    select *,
    'Green' as service_type
    from {{ ref('base_staging_green_tripdata_external') }}
),
yellow_tripdata as (
    select *,
    'Yellow' as service_type
    from {{ ref('base_staging_yellow_tripdata_external') }}
),
{# Union to add the data together, just adds them as rows below, and works perfectly because we cleaned the data and made the same rows #}
trips_unioned as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
),
{# Remove unknown zones #}
dim_zones as (
    select * 
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
{# In th select we select all the columns that we had in the yellow and green staging tables
And we add the human readable pickup borough and zone to them as columns, same for dropoff
Possible since we joined on the dim zones #}
select 
trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
{# Joins to clear up the pickup locationid and to clear up the dropofflocationid
Inner join because we only want all the data that doesn't have a borough that's unknown
Inner join twice because we want both pickup and dropoff to be cleared up  #}
from 
    trips_unioned
    inner join dim_zones as pickup_zone
    on trips_unioned.pickup_locationid = pickup_zone.locationid
    inner join dim_zones as dropoff_zone
    on trips_unioned.dropoff_locationid = dropoff_zone.locationid