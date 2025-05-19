{{ 
  config(
    materialized='view'
  ) 
}}
 
with tripdata as 
(
  select *,
    row_number() over(partition by cast(vendorid as int64), tpep_pickup_datetime) as rn
  from {{ source('staging','yellow_tripdata_external_schema') }}
  where vendorid is not null 
)
select
   {# -- identifiers #}
    {{ dbt_utils.generate_surrogate_key(["'yellow'", adapter.quote("VendorID"), adapter.quote("tpep_pickup_datetime"), adapter.quote("PULocationID"), adapter.quote("DOLocationID")] ) }} as tripid,    
    {{ dbt.safe_cast(adapter.quote("VendorID"), api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast(adapter.quote("RatecodeID"), api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast(adapter.quote("PULocationID"), api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast(adapter.quote("DOLocationID"), api.Column.translate_type("integer")) }} as dropoff_locationid,

    {# -- timestamps #}
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    {{ adapter.quote("store_and_fwd_flag") }},
    {{ dbt.safe_cast(adapter.quote("passenger_count"), api.Column.translate_type("integer")) }} as passenger_count,
    {{ dbt.safe_cast(adapter.quote("trip_distance"), api.Column.translate_type("numeric")) }} as trip_distance,
    
    {# Yellow cabs are always street-hail so no e-hail fee #}
    {# Trip type for yellow taxi's is 1, so no need to ingest or safe cast etc #}
    1 as trip_type,
	
    
    {# -- payment info #}
    {{ dbt.safe_cast("fare_amount", api.Column.translate_type("numeric")) }} as fare_amount,
    {{ dbt.safe_cast(adapter.quote("extra"), api.Column.translate_type("numeric")) }} as extra,
    {{ dbt.safe_cast(adapter.quote("mta_tax"), api.Column.translate_type("numeric")) }} as mta_tax,
    {{ dbt.safe_cast(adapter.quote("tip_amount"), api.Column.translate_type("numeric")) }} as tip_amount,
    {{ dbt.safe_cast(adapter.quote("tolls_amount"), api.Column.translate_type("numeric")) }} as tolls_amount,
    {{dbt.safe_cast("0", api.Column.translate_type("numeric")) }}  as ehail_fee,
    {{ dbt.safe_cast(adapter.quote("improvement_surcharge"), api.Column.translate_type("numeric")) }} as improvement_surcharge,
    {{ dbt.safe_cast(adapter.quote("total_amount"), api.Column.translate_type("numeric")) }} as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description

    {# Do not keep surcharge in the columns, as some old csvs don't have this column and we don't bother#}
    {# {{ adapter.quote("congestion_surcharge") }} #}

from tripdata
where rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}