{{ config(materialized='table') }}
SELECT
    "Country or region" AS country,
    "Score"             AS happiness_score,
    "GDP per capita"    AS gdp_per_capita
FROM {{ ref('2019') }}
WHERE "Score" > 6
ORDER BY "Score" DESC