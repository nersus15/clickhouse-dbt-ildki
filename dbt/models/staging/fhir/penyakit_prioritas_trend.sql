{{ config(
    materialized='table',
    alias='penyakit_prioritas_trend',
    engine='MergeTree()',
    order_by=['tanggal_layanan', 'kategori_penyakit']
) }}

-- Chart: "Tren Kasus Hipertensi & Diabetes Melitus"
-- conditions materialized='incremental' -- WAJIB FINAL.
SELECT
    tanggal_diagnosis AS tanggal_layanan,
    kategori_ptm AS kategori_penyakit,
    diagnosis_name AS nama_diagnosis,
    count(*) AS jumlah_kasus
FROM {{ ref('conditions') }} FINAL
WHERE kategori_ptm IN ('Hipertensi', 'Diabetes Melitus')
GROUP BY tanggal_diagnosis, kategori_ptm, diagnosis_name
