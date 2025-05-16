{# Dim zones sql to create taxi zones lookup dim zones table
Replace Boro by Green, since that's what it really means
Can reference the csv file immediately using just the filename, that's the from 
#}
{{ 
    config(
        materialized='table'
    ) 
}}

select 
    locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone 
from {{ ref('taxi_zone_lookup') }}

