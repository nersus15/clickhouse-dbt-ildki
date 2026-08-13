{{ config(
    materialized='view',
    alias='tren_tensi_harian'
) }}

-- VIEW: sumbernya sudah tabel silver yang bersih.
-- PERLU DIUKUR: filter loinc_code tidak ada di order_by observations, jadi full
-- scan + FINAL. Kalau lambat, ubah observations ke order_by=['loinc_code','observation_id'].

-- Chart: "Tren Rata-rata Tekanan Darah Harian (Sistol/Diastol)"
-- Nilai 0 dikecualikan karena diindikasikan data tidak valid/placeholder.
-- observations materialized='incremental' -- WAJIB FINAL.
SELECT
    tanggal_periksa,
    avg(CASE WHEN nama_pengukuran = 'Systolic blood pressure' AND nilai > 0 THEN nilai END) AS rata_sistol,
    avg(CASE WHEN nama_pengukuran = 'Diastolic blood pressure' AND nilai > 0 THEN nilai END) AS rata_diastol
FROM {{ ref('observations') }} FINAL
WHERE loinc_code = '8480-6'
GROUP BY tanggal_periksa
