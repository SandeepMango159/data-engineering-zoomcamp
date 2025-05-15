{{
    config(
        materialized='view'
    )
}}
 
 {# 
 This is a common table expression CTE
 It selects everything from the source which is staging, and our green_tripdata_external table
 And adds another extra column called "rn" made from the vendorid and lpep datetime
 Partitions over the same columns as what creates your primary keys, so if there are duplicates they'll be numbered as 1,2,3... etc and at the end you only keep number1, okay... 
 #}
with tripdata as (
  select 
    *,
    row_number() over(partition by vendorid, lpep_pickup_datetime) as rn
  from 
    {{ source('staging','green_tripdata_external') }}
  where 
    vendorid is not null 
)
  select
    -- identifiers
    {{dbt_utils.generate_surrogate_key([adapter.quote("VendorID"), adapter.quote("lpep_pickup_datetime")] )}} as tripid, 
    {{ dbt.safe_cast(adapter.quote("VendorID"), api.Column.translate_type("integer"))}} as vendorid
    {{ dbt.safe_cast(adapter.quote("RatecodeID"), api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast(adapter.quote("PULocationID"), api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast(adapter.quote("DOLocationID"), api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast({{adapter.quote("lpep_pickup_datetime")}} as timestamp) as pickup_datetime,
    cast({{adapter.quote("lpep_dropoff_datetime")}} as timestamp) as dropoff_datetime,


    -- trip info
    {{ adapter.quote("store_and_fwd_flag") }},
    {{ dbt.safe_cast(adapter.quote("passenger_count"), api.Column.translate_type("integer")) }} as passenger_count,
    {{ dbt.safe_cast(adapter.quote("trip_distance"), api.Column.translate_type("numeric")) }} as trip_distance,
  
    {# Yellow cabs are always street-hail so no e-hail fee #}
    {# Trip type for yellow taxi's is 1, so no need to ingest or safe cast etc #}
    1 as trip_type,

    -- payment info
    {{ dbt.safe_cast("fare_amount", api.Column.translate_type("numeric")) }} as fare_amount,
    {{ dbt.safe_cast(adapter.quote("extra"), api.Column.translate_type("numeric")) }} as extra,
    {{ dbt.safe_cast(adapter.quote("mta_tax"), api.Column.translate_type("numeric")) }} as mta_tax,
    {{ dbt.safe_cast(adapter.quote("tip_amount"), api.Column.translate_type("numeric")) }} as tip_amount,
    {{ dbt.safe_cast(adapter.quote("tolls_amount"), api.Column.translate_type("numeric")) }} as tolls_amount,
    {{ dbt.safe_cast("0", api.Column.translate_type("numeric")) }} as ehail_fee,
    {{ dbt.safe_cast(adapter.quote("improvement_surcharge"), api.Column.translate_type("numeric")) }} as improvement_surcharge,
    {{ dbt.safe_cast(adapter.quote("total_amount"), api.Column.translate_type("numeric")) }} as total_amount,
    coalesce({{ dbt.safe_cast(adapter.quote("payment_type"), api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description( adapter.quote("payment_type") )}} as payment_type_description
    
    {# Do not keep surcharge in the columns, as old csv don't have this column #}
    {# {{ adapter.quote("congestion_surcharge") }} #}

from tripdata
where rn = 1


{# Variables #}
-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}