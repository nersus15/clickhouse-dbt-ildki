{{ config(
    materialized='view',
    alias='utilisasi_faskes'
) }}

-- VIEW: sumbernya sudah tabel silver yang bersih.
-- TODO: kalau organizations jadi dictionary, ganti JOIN dengan dictGet().

-- Chart: "Utilisasi Layanan per Puskesmas"
-- res_link & organizations materialized='incremental' -- WAJIB FINAL.
SELECT
    o.nama_puskesmas,
    count(*) AS jumlah_kunjungan
FROM {{ ref('res_link') }} AS l FINAL
JOIN {{ ref('organizations') }} AS o FINAL ON l.target_resource_id = o.organization_id
WHERE l.source_resource_type = 'Encounter'
    AND l.src_path = 'Encounter.serviceProvider'
    AND l.target_resource_type = 'Organization'
GROUP BY o.nama_puskesmas
