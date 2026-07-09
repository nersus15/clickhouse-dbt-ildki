{{ config(materialized='view', alias='resource_daily_growth') }}

-- Chart: "Tren Ingest Resource Harian per Tipe"
SELECT
    toDate(res_published) AS ingest_date,
    res_type,
    count(*) AS total
FROM {{ ref('resource') }}
GROUP BY toDate(res_published), res_type
