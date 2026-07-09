{{ config(materialized='view', alias='mart_tren_tensi_harian') }}

-- Chart: "Tren Rata-rata Tekanan Darah Harian (Sistol/Diastol)"
-- Nilai 0 dikecualikan karena diindikasikan data tidak valid/placeholder.
SELECT
    tanggal_periksa,
    avg(CASE WHEN nama_pengukuran = 'Systolic blood pressure' AND nilai > 0 THEN nilai END) AS rata_sistol,
    avg(CASE WHEN nama_pengukuran = 'Diastolic blood pressure' AND nilai > 0 THEN nilai END) AS rata_diastol
FROM {{ ref('stg_observations') }}
WHERE loinc_code = '8480-6'
GROUP BY tanggal_periksa
