{{ config(
    materialized='view',
    alias='resource_daily_growth'
) }}

-- VIEW: sumbernya sudah tabel silver yang bersih, jadi tidak perlu rebuild berkala.

-- Chart: "Tren Ingest Resource Harian per Tipe"
-- resource materialized='incremental' -- WAJIB FINAL.
SELECT
    toDate(res_published) AS ingest_date,
    res_type,
    count(*) AS total
FROM {{ ref('resource') }} FINAL
GROUP BY toDate(res_published), res_type
