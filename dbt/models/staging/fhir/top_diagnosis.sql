{{ config(
    materialized='table',
    alias='top_diagnosis',
    engine='MergeTree()',
    order_by=['kode_icd10']
) }}

-- Chart: "10 Besar Penyakit (Diagnosis Terbanyak)"
-- conditions materialized='incremental' -- WAJIB FINAL.
SELECT
    icd10_code AS kode_icd10,
    diagnosis_name AS nama_diagnosis,
    count(*) AS jumlah_kasus
FROM {{ ref('conditions') }} FINAL
GROUP BY icd10_code, diagnosis_name
