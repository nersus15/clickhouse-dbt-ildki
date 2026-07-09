{{ config(materialized='view', alias='mart_deteksi_dini_hipertensi') }}

-- Chart: "Deteksi Dini Risiko Hipertensi (Observasi vs Diagnosis Formal)"
-- Klasifikasi pasien dari hasil pengukuran tensi aktual (LOINC 8480-6), lalu dicek
-- apakah sudah punya diagnosis formal Hipertensi (Condition ICD-10 I1x).
--
-- CATATAN: dipakai IN (subquery tidak berkorelasi), BUKAN correlated EXISTS -- ClickHouse
-- tidak mendukung correlated subquery dengan baik, jadi logikanya ditulis ulang dari versi StarRocks.

WITH bp_reading AS (
    SELECT
        patient_ref,
        tanggal_periksa,
        max(CASE WHEN nama_pengukuran = 'Systolic blood pressure' THEN nilai END) AS sistol,
        max(CASE WHEN nama_pengukuran = 'Diastolic blood pressure' THEN nilai END) AS diastol
    FROM {{ ref('stg_observations') }}
    WHERE loinc_code = '8480-6'
    GROUP BY patient_ref, tanggal_periksa
),

klasifikasi AS (
    SELECT
        patient_ref,
        tanggal_periksa,
        CASE
            WHEN sistol IS NULL AND diastol IS NULL THEN NULL
            WHEN coalesce(sistol, 0) = 0 AND coalesce(diastol, 0) = 0 THEN NULL
            WHEN sistol >= 140 OR diastol >= 90 THEN 'Hipertensi (>=140/90)'
            WHEN sistol >= 130 OR diastol >= 85 THEN 'Waspada/Pra-Hipertensi (>=130/85)'
            ELSE 'Normal'
        END AS kategori_tensi
    FROM bp_reading
),

pasien_terdiagnosis AS (
    SELECT DISTINCT patient_ref
    FROM {{ ref('stg_conditions') }}
    WHERE kategori_ptm = 'Hipertensi'
)

SELECT
    kategori_tensi,
    CASE
        WHEN k.patient_ref IN (SELECT patient_ref FROM pasien_terdiagnosis) THEN 'Sudah Terdiagnosis Hipertensi'
        ELSE 'Belum Ada Diagnosis Formal'
    END AS status_diagnosis,
    count(DISTINCT k.patient_ref) AS jumlah_pasien
FROM klasifikasi AS k
WHERE kategori_tensi IS NOT NULL
GROUP BY kategori_tensi, status_diagnosis
