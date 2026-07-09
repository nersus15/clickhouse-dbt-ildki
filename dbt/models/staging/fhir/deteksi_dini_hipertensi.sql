{{ config(
    materialized='table',
    alias='deteksi_dini_hipertensi',
    engine='MergeTree()',
    order_by=['kategori_tensi', 'status_diagnosis']
) }}

-- Chart: "Deteksi Dini Risiko Hipertensi (Observasi vs Diagnosis Formal)"
-- Klasifikasi pasien dari hasil pengukuran tensi aktual (LOINC 8480-6), lalu dicek
-- apakah sudah punya diagnosis formal Hipertensi (Condition ICD-10 I1x).
--
-- CATATAN 1: dipakai IN (subquery tidak berkorelasi), BUKAN correlated EXISTS -- ClickHouse
-- tidak mendukung correlated subquery dengan baik.
-- CATATAN 2: observations & conditions materialized='incremental' -- WAJIB FINAL.
-- CATATAN 3 (edge case, BUKAN error): kondisi "sistol >= 140 OR diastol >= 90" bisa hasilkan
-- NULL (bukan TRUE/FALSE) kalau salah satu dari sistol/diastol NULL dan yang lain di bawah
-- ambang -- CASE WHEN akan skip ke baris berikutnya (perilaku standar SQL untuk NULL di WHEN).
-- Dampaknya: pasien yang cuma punya SATU dari dua angka tensi (jarang, tapi mungkin) bisa
-- ke-klasifikasi 'Normal' padahal semestinya masuk kategori lebih tinggi kalau angka yang
-- hilang itu diisi. Belum diperbaiki karena data saat ini selalu punya sistol+diastol lengkap
-- (sudah divalidasi ke ClickHouse langsung) -- kalau nanti sumber data lain punya pola
-- pengukuran tunggal, logic ini perlu ditinjau ulang (mis. pakai coalesce yang lebih eksplisit).
-- CATATAN 4: CASE pada klasifikasi.kategori_tensi punya cabang "THEN NULL" (baris tanpa
-- pengukuran valid, sengaja dibuang lewat WHERE kategori_tensi IS NOT NULL di bawah) --
-- akibatnya ClickHouse infer TIPE kolomnya Nullable(String), walau NILAI akhirnya (setelah
-- filter) tidak pernah NULL. Karena kolom ini dipakai di order_by (sorting key) tabel
-- MergeTree, ClickHouse menolak ("Sorting key contains nullable columns"). Makanya di SELECT
-- akhir dibungkus assumeNotNull() -- aman karena WHERE kategori_tensi IS NOT NULL sudah
-- menjamin tidak ada baris NULL yang lolos sampai situ.

WITH bp_reading AS (
    SELECT
        patient_ref,
        tanggal_periksa,
        max(CASE WHEN nama_pengukuran = 'Systolic blood pressure' THEN nilai END) AS sistol,
        max(CASE WHEN nama_pengukuran = 'Diastolic blood pressure' THEN nilai END) AS diastol
    FROM {{ ref('observations') }} FINAL
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
    FROM {{ ref('conditions') }} FINAL
    WHERE kategori_ptm = 'Hipertensi'
)

SELECT
    assumeNotNull(kategori_tensi) AS kategori_tensi,
    CASE
        WHEN k.patient_ref IN (SELECT patient_ref FROM pasien_terdiagnosis) THEN 'Sudah Terdiagnosis Hipertensi'
        ELSE 'Belum Ada Diagnosis Formal'
    END AS status_diagnosis,
    count(DISTINCT k.patient_ref) AS jumlah_pasien
FROM klasifikasi AS k
WHERE kategori_tensi IS NOT NULL
GROUP BY kategori_tensi, status_diagnosis
