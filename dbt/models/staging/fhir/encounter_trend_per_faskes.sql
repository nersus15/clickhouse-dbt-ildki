{{ config(
    materialized='view',
    alias='encounter_trend_per_faskes'
) }}

-- VIEW: sumbernya sudah tabel silver yang bersih.
-- Sempat dipertimbangkan incremental 'delete+insert', tapi unique_key
-- (tanggal, nama_puskesmas) tidak aman kalau nama faskes berubah -- baris lama
-- tidak pernah terhapus.
-- TODO: kalau organizations jadi dictionary, ganti JOIN ketiga dengan dictGet().

-- Chart: "Tren Pengiriman Data Kunjungan (Encounter) per Puskesmas"
-- Satu baris = satu hari x satu puskesmas -> dipakai untuk line chart multi-garis di Superset.
-- encounters, res_link, organizations materialized='incremental' -- WAJIB FINAL.
SELECT
    e.tanggal_kunjungan,
    o.nama_puskesmas,
    count(*) AS jumlah_kunjungan
FROM {{ ref('encounters') }} AS e FINAL
JOIN {{ ref('res_link') }} AS l FINAL
    ON l.src_resource_id = e.encounter_id
    AND l.source_resource_type = 'Encounter'
    AND l.src_path = 'Encounter.serviceProvider'
JOIN {{ ref('organizations') }} AS o FINAL ON l.target_resource_id = o.organization_id
GROUP BY e.tanggal_kunjungan, o.nama_puskesmas
