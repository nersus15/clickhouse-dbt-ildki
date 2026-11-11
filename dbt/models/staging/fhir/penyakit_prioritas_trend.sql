{{ config(
    materialized='view',
    alias='penyakit_prioritas_trend'
) }}

-- Chart: "Tren Kasus Hipertensi & Diabetes Melitus"
-- Tanggal layanan diambil dari Encounter diagnosis (onset/recordedDate jarang diisi faskes);
-- fallback ke tanggal_diagnosis kalau Encounter-nya belum ada di silver.
-- conditions & encounters materialized='incremental' -- WAJIB FINAL.
SELECT
    -- LEFT JOIN tanpa match -> Date default 1970-01-01 (tanggal_kunjungan sendiri tidak pernah epoch)
    if(e.tanggal_kunjungan = toDate(0), c.tanggal_diagnosis, e.tanggal_kunjungan) AS tanggal_layanan,
    c.kategori_ptm AS kategori_penyakit,
    c.diagnosis_name AS nama_diagnosis,
    count() AS jumlah_kasus,
    uniqExact(c.patient_ref) AS jumlah_pasien
FROM {{ ref('conditions') }} AS c FINAL
LEFT JOIN (
    SELECT concat('Encounter/', encounter_fhir_id) AS encounter_ref, tanggal_kunjungan
    FROM {{ ref('encounters') }} FINAL
) AS e ON e.encounter_ref = c.encounter_ref
WHERE c.kategori_ptm IN ('Hipertensi', 'Diabetes Melitus')
GROUP BY tanggal_layanan, kategori_penyakit, nama_diagnosis
