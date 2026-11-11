{{ config(
    materialized='view',
    alias='deteksi_dini_hipertensi'
) }}

-- Chart: "Deteksi Dini Risiko Hipertensi (Observasi vs Diagnosis Formal)"
-- Klasifikasi tiap pasien dari pemeriksaan tensi TERAKHIR, lalu dicek apakah sudah punya
-- diagnosis formal Hipertensi (Condition ICD-10 I10-I15).
--
-- SATUSEHAT mengirim sistol (LOINC 8480-6) dan diastol (8462-4) sebagai Observation
-- TERPISAH -- keduanya dipasangkan per encounter. Ambang klasifikasi untuk DEWASA dan
-- berbasis satu kali pengukuran, bukan pengganti diagnosis klinis.
-- maxIf/argMax mengembalikan 0 kalau angka tidak ada, jadi pembacaan tunggal (cuma sistol
-- atau cuma diastol) tetap terklasifikasi dari angka yang ada.
-- Rentang plausibel (sistol 50-300, diastol 30-200) membuang placeholder 0 & salah ketik.
-- Nama CTE kolom (kategori_raw) sengaja beda dari alias akhir: di ClickHouse alias SELECT
-- menimpa nama kolom di WHERE/GROUP BY.
-- observations & conditions materialized='incremental' -- WAJIB FINAL.

WITH bp AS (
    SELECT
        patient_ref,
        encounter_ref,
        max(tanggal_periksa) AS tanggal_periksa,
        maxIf(nilai, loinc_code = '8480-6' AND nilai BETWEEN 50 AND 300) AS sistol,
        maxIf(nilai, loinc_code = '8462-4' AND nilai BETWEEN 30 AND 200) AS diastol
    FROM {{ ref('observations') }} FINAL
    WHERE loinc_code IN ('8480-6', '8462-4')
    GROUP BY patient_ref, encounter_ref
),

terakhir AS (
    SELECT
        patient_ref,
        argMax(sistol, tanggal_periksa) AS s,
        argMax(diastol, tanggal_periksa) AS d
    FROM bp
    WHERE sistol > 0 OR diastol > 0
    GROUP BY patient_ref
),

klasifikasi AS (
    SELECT
        patient_ref,
        multiIf(
            s >= 140 OR d >= 90, 'Hipertensi (>=140/90)',
            s >= 130 OR d >= 85, 'Waspada/Pra-Hipertensi (>=130/85)',
            'Normal'
        ) AS kategori_raw
    FROM terakhir
),

pasien_terdiagnosis AS (
    SELECT DISTINCT patient_ref
    FROM {{ ref('conditions') }} FINAL
    WHERE kategori_ptm = 'Hipertensi'
)

SELECT
    kategori_raw AS kategori_tensi,
    if(patient_ref IN (SELECT patient_ref FROM pasien_terdiagnosis),
       'Sudah Terdiagnosis Hipertensi', 'Belum Ada Diagnosis Formal') AS status_diagnosis,
    count() AS jumlah_pasien
FROM klasifikasi
GROUP BY kategori_tensi, status_diagnosis
