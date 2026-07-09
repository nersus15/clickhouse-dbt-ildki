{{ config(materialized='view', alias='penyakit_prioritas_trend') }}

-- Chart: "Tren Kasus Hipertensi & Diabetes Melitus"
SELECT
    tanggal_diagnosis AS tanggal_layanan,
    kategori_ptm AS kategori_penyakit,
    diagnosis_name AS nama_diagnosis,
    count(*) AS jumlah_kasus
FROM {{ ref('conditions') }}
WHERE kategori_ptm IN ('Hipertensi', 'Diabetes Melitus')
GROUP BY tanggal_diagnosis, kategori_ptm, diagnosis_name
