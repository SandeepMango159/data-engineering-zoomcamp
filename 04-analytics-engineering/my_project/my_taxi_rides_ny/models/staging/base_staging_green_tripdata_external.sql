 with
 
{# Select  everything from the staging source, defined in schema.yml, and selects the green ext table#}
 source as (
        select * from {{ source('staging', 'green_tripdata_external') }}
  ),
  renamed as (
      select

        {{dbt_utils.generate_surrogate_key([adapter.quote("VendorID"), adapter.quote("lpep_pickup_datetime")] )}} as tripid, 
        {{ adapter.quote("VendorID") }},
        {{ adapter.quote("lpep_pickup_datetime") }},
        {{ adapter.quote("lpep_dropoff_datetime") }},
        {{ adapter.quote("store_and_fwd_flag") }},
        {{ adapter.quote("RatecodeID") }},
        {{ adapter.quote("PULocationID") }},
        {{ adapter.quote("DOLocationID") }},
        {{ adapter.quote("passenger_count") }},
        {{ adapter.quote("trip_distance") }},
        {{ adapter.quote("fare_amount") }},
        {{ adapter.quote("extra") }},
        {{ adapter.quote("mta_tax") }},
        {{ adapter.quote("tip_amount") }},
        {{ adapter.quote("tolls_amount") }},
        {{ adapter.quote("ehail_fee") }},
        {{ adapter.quote("improvement_surcharge") }},
        {{ adapter.quote("total_amount") }},
        {{ get_payment_type_description( adapter.quote("payment_type") )}} as payment_type_description,
        {{ adapter.quote("trip_type") }},
        {{ adapter.quote("congestion_surcharge") }}

      from source
  )
  select * from renamed
    