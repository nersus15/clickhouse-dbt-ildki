{{ config(materialized='view', alias='mart_top_diagnosis') }}

-- Chart: "10 Besar Penyakit (Diagnosis Terbanyak)"
SELECT
    icd10_code AS kode_icd10,
    diagnosis_name AS nama_diagnosis,
    count(*) AS jumlah_kasus
FROM {{ ref('stg_conditions') }}
GROUP BY icd10_code, diagnosis_name
