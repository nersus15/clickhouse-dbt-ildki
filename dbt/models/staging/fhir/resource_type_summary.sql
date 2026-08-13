{{ config(
    materialized='view',
    alias='resource_type_summary'
) }}

-- VIEW: sumbernya sudah tabel silver yang bersih.

-- Chart: "Distribusi Resource FHIR per Tipe" (dashboard tambahan/arsip -- terlalu teknis untuk
-- dashboard kebijakan utama, tapi tetap disediakan sebagai referensi teknis)
-- resource materialized='incremental' -- WAJIB FINAL.
SELECT
    res_type,
    count(*) AS total,
    countIf(NOT is_deleted) AS active,
    countIf(is_deleted) AS deleted
FROM {{ ref('resource') }} FINAL
GROUP BY res_type
