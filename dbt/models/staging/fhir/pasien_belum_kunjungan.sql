{{ config(
    materialized='view',
    alias='pasien_belum_kunjungan'
) }}

-- Chart: "Pasien Belum Ada Kunjungan"
-- Satu baris = satu pasien tanpa Encounter sama sekali, jadi COUNT(*) di Superset
-- memberi angka watchlist dan daftarnya tetap bisa di-drill.
-- Nol akses bronze: relasi Encounter.subject sudah tersedia di res_link.
-- patients & res_link materialized='incremental' -- WAJIB FINAL.
SELECT
    p.patient_res_id,
    p.patient_id
FROM {{ ref('patients') }} AS p FINAL
WHERE p.patient_res_id NOT IN (
    SELECT target_resource_id
    FROM {{ ref('res_link') }} FINAL
    WHERE source_resource_type = 'Encounter'
        AND src_path = 'Encounter.subject'
)
