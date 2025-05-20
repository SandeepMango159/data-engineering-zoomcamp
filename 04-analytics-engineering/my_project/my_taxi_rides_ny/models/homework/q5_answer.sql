{{ config(materialized = 'view') }}   
{# Distinct will return two rows, one containing green and one yellow, gets treated as a temporary table named t
Now the outer query will execute once for each row in t
and in the subqueries we reference the current t.service type #}

{# Select only the year 2020 #}
WITH y2020 AS (
    SELECT *
    FROM {{ ref('fct_taxi_trips_quarterly_revenue') }}
    WHERE year = 2020
),
{# From 2020 we pick columns, and
we rank over each service type and yoy growth by either asc or desc, and that means best or worst
So creates a column to rank per service type #}
ranked AS (
    SELECT
        service_type,
        year_quarter,
        yoy_growth,

        {# Best = highest (least-negative) yoy #}
        RANK() OVER (
            PARTITION BY service_type
            ORDER BY yoy_growth DESC
        ) AS r_best,

        {# Worst = lowest (most-negative) yoy #}
        RANK() OVER (
            PARTITION BY service_type
            ORDER BY yoy_growth ASC
        ) AS r_worst
    FROM y2020
)
{# For every row, checks if it is r_best and r_worst with rank 1, if so it'll get the year quarter value for those rows, else it will get a null
And after that max will get the highest values of each group, meaning each service type, meaning per yellow and green  #}
SELECT
    service_type,
    MAX(IF(r_best  = 1, year_quarter, NULL))  AS best_q,
    MAX(IF(r_worst = 1, year_quarter, NULL))  AS worst_q
FROM ranked
GROUP BY service_type
