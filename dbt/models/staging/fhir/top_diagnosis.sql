{{ config(
    materialized='view',
    alias='top_diagnosis'
) }}

-- VIEW: sumbernya sudah tabel silver yang bersih.

-- Chart: "10 Besar Penyakit (Diagnosis Terbanyak)"
-- conditions materialized='incremental' -- WAJIB FINAL.
SELECT
    icd10_code AS kode_icd10,
    diagnosis_name AS nama_diagnosis,
    count(*) AS jumlah_kasus,
    uniqExact(patient_ref) AS jumlah_pasien
FROM {{ ref('conditions') }} FINAL
GROUP BY icd10_code, diagnosis_name
