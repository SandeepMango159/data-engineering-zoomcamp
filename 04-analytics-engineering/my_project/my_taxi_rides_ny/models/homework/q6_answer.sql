{{ 
    config(
        materialized = 'view'
    )
}}

{# To get the values of p97, p95, p90 for Green Taxi and Yellow Taxi, in April 2020? #}
SELECT *
FROM   {{ ref('fct_taxi_trips_monthly_fare_p95') }}
WHERE  year = 2020 AND month = 4