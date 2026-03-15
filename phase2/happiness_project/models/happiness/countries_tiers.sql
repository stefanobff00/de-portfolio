{{ config(materialized='table') }}
SELECT
    country,
    happiness_score,
    gdp_per_capita,
    CASE
        WHEN happiness_score >= 7.5 THEN 'Top'
        WHEN happiness_score >= 7.0 THEN 'High'
        ELSE 'Mid'
    END AS tier
FROM {{ ref('top_countries') }}