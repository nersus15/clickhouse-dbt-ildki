{{ config(materialized='view', alias='encounter_trend_per_faskes') }}

-- Chart: "Tren Pengiriman Data Kunjungan (Encounter) per Puskesmas"
-- Satu baris = satu hari x satu puskesmas -> dipakai untuk line chart multi-garis di Superset.
SELECT
    e.tanggal_kunjungan,
    o.nama_puskesmas,
    count(*) AS jumlah_kunjungan
FROM {{ ref('encounters') }} AS e
JOIN {{ ref('res_link') }} AS l
    ON l.src_resource_id = e.encounter_id
    AND l.source_resource_type = 'Encounter'
    AND l.src_path = 'Encounter.serviceProvider'
JOIN {{ ref('organizations') }} AS o ON l.target_resource_id = o.organization_id
GROUP BY e.tanggal_kunjungan, o.nama_puskesmas
