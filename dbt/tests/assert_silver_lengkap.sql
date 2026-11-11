-- Gagal kalau silver tertinggal dari bronze lebih dari 0,1% (atau >5 baris) per res_type --
-- tanda watermark incremental melewatkan baris CDC. Selisih kecil wajar karena lag antar run.
-- Jalankan SETELAH dbt run incremental: `dbt test --select assert_silver_lengkap`.

WITH bronze AS (
    SELECT r.res_type, count() AS n
    FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
    WHERE r._peerdb_is_deleted = 0
        AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
        AND r.res_type IN ('Encounter', 'Condition', 'Observation', 'CarePlan', 'Organization', 'Patient')
    GROUP BY r.res_type
),

silver AS (
    SELECT 'Encounter' AS res_type, count() AS n FROM {{ ref('encounters') }} FINAL
    UNION ALL SELECT 'Condition', count() FROM {{ ref('conditions') }} FINAL
    UNION ALL SELECT 'Observation', count() FROM {{ ref('observations') }} FINAL
    UNION ALL SELECT 'CarePlan', count() FROM {{ ref('careplans') }} FINAL
    UNION ALL SELECT 'Organization', count() FROM {{ ref('organizations') }} FINAL
    UNION ALL SELECT 'Patient', count() FROM {{ ref('patients') }} FINAL
)

SELECT b.res_type, b.n AS bronze_n, s.n AS silver_n, b.n - s.n AS selisih
FROM bronze AS b
JOIN silver AS s ON s.res_type = b.res_type
WHERE abs(toInt64(b.n) - toInt64(s.n)) > greatest(5, b.n * 0.001)
