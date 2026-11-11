{{ config(
    materialized='view',
    alias='tren_tensi_harian'
) }}

-- Chart: "Tren Rata-rata Tekanan Darah Harian (Sistol/Diastol)"
-- Sistol (LOINC 8480-6) dan diastol (8462-4) adalah Observation terpisah -- dua-duanya
-- harus masuk filter. Nilai di luar rentang plausibel (termasuk placeholder 0) dikecualikan.
-- observations materialized='incremental' -- WAJIB FINAL.
SELECT
    tanggal_periksa,
    avgIf(nilai, loinc_code = '8480-6' AND nilai BETWEEN 50 AND 300) AS rata_sistol,
    avgIf(nilai, loinc_code = '8462-4' AND nilai BETWEEN 30 AND 200) AS rata_diastol
FROM {{ ref('observations') }} FINAL
WHERE loinc_code IN ('8480-6', '8462-4')
    AND tanggal_periksa != '1970-01-01'
GROUP BY tanggal_periksa
