{{ config(materialized='view', alias='mart_utilisasi_faskes') }}

-- Chart: "Utilisasi Layanan per Puskesmas"
SELECT
    o.nama_puskesmas,
    count(*) AS jumlah_kunjungan
FROM {{ ref('stg_res_link') }} AS l
JOIN {{ ref('stg_organizations') }} AS o ON l.target_resource_id = o.organization_id
WHERE l.source_resource_type = 'Encounter'
    AND l.src_path = 'Encounter.serviceProvider'
    AND l.target_resource_type = 'Organization'
GROUP BY o.nama_puskesmas
