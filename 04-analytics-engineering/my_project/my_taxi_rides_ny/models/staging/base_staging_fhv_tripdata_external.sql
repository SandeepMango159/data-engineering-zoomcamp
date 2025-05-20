{{
    config(
        materialized='view'
    )
}}
 

 with 
tripdata as (
    select *
    from {{ source('staging', 'fhv_tripdata_external_schema') }}
    where dispatching_base_num is not null
)
select 
    {{ dbt.safe_cast(adapter.quote("dispatching_base_num"), api.Column.translate_type("string"))}} as dispatching_base_num,
    {{ dbt.safe_cast(adapter.quote("pickup_datetime"), api.Column.translate_type("timestamp"))}} as pickup_datetime,
    {{ dbt.safe_cast(adapter.quote("dropoff_datetime"), api.Column.translate_type("timestamp"))}} as dropoff_datetime,
    {{ dbt.safe_cast(adapter.quote("PUlocationID"), api.Column.translate_type("integer"))}} as pickup_locationid,
    {{ dbt.safe_cast(adapter.quote("DOlocationID"), api.Column.translate_type("integer"))}} as dropoff_locationid,
    {{ dbt.safe_cast(adapter.quote("SR_Flag"), api.Column.translate_type("numeric"))}} as sr_flag,
    {{ dbt.safe_cast(adapter.quote("Affiliated_base_number"), api.Column.translate_type("string"))}} as affiliated_base_number
from tripdata

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}