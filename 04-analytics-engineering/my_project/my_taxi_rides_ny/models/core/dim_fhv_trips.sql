{{ 
    config(
        materialized='table'
    )
}}
{# Get the FHV tripdata from the staging table, add a primary key and a service type #}
with fhv_tripdata as (
    select
        {{ dbt_utils.generate_surrogate_key([
             'pickup_datetime',
             'dropOff_datetime',
             'pickup_locationid',
             'dropoff_locationid',
             'dispatching_base_num'
             ])
        }} as trip_id,
        *,
        'FHV' as service_type
    from 
        {{ ref('base_staging_fhv_tripdata_external') }} 
),
{# Get dim zoens and remove unknown zones #}
dim_zones as (
    select * 
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
{# Combine fhv trips with dim zones for better readability and add other columns #}
select
    fhv_tripdata.trip_id,
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.affiliated_base_number,
    fhv_tripdata.service_type,

    extract(year from fhv_tripdata.pickup_datetime) as year,
    extract(month from fhv_tripdata.pickup_datetime) as month,

    fhv_tripdata.pickup_locationid as pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,

    fhv_tripdata.dropoff_locationid as dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,

    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,

    fhv_tripdata.sr_flag

from 
    fhv_tripdata 
    inner join dim_zones as pickup_zone
        on fhv_tripdata.pickup_locationid = pickup_zone.locationid
    inner join dim_zones as dropoff_zone
        on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid