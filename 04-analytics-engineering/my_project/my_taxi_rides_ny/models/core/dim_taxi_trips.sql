{# Table to  create a dimension table for taxi trips, this one will hold #}
{#  keep it physical; this table is reused a lot #}
{# optional but helps in BigQuery #}
{{ config(
    materialized = 'table',          
    cluster_by   = ['year', 'service_type']   
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
unioned_trips as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
)

select
    {# Primary key #}
    unioned_trips.tripid,

    {# Timestamps you’ll still need for granular checks #}
    unioned_trips.pickup_datetime,
    unioned_trips.dropoff_datetime,

    unioned_trips.service_type,

    {# Columns to helpl with dates and calenders #}
    extract(year from unioned_trips.pickup_datetime) as year,
    extract(quarter from unioned_trips.pickup_datetime) as quarter,
    concat(
        cast(extract(year from pickup_datetime) as string),
        '/Q',
        cast(extract(quarter from pickup_datetime) as string)) 
        as year_quarter,
    extract(month from pickup_datetime) as month,

from unioned_trips