{{ config(
    materialized='table',
    alias='resource_daily_growth',
    engine='MergeTree()',
    order_by=['ingest_date', 'res_type']
) }}

-- Chart: "Tren Ingest Resource Harian per Tipe"
-- resource materialized='incremental' -- WAJIB FINAL.
SELECT
    toDate(res_published) AS ingest_date,
    res_type,
    count(*) AS total
FROM {{ ref('resource') }} FINAL
GROUP BY toDate(res_published), res_type
