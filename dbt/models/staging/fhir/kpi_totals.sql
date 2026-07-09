{{ config(materialized='view', alias='kpi_totals') }}

-- Chart: "KPI Kebijakan Kesehatan"
-- KPI ringkas untuk pejabat/pengambil keputusan: total pasien, kunjungan, diagnosis,
-- puskesmas aktif, kasus hipertensi & diabetes, dan kesegaran data.
-- view (bukan table): murah, cuma baca dari resource/conditions/res_link/organizations yang sudah berupa table.
SELECT
    (SELECT count(*) FROM {{ ref('resource') }} WHERE res_type = 'Patient' AND NOT is_deleted) AS total_pasien,
    (SELECT count(*) FROM {{ ref('resource') }} WHERE res_type = 'Encounter' AND NOT is_deleted) AS total_kunjungan,
    (SELECT count(*) FROM {{ ref('resource') }} WHERE res_type = 'Condition' AND NOT is_deleted) AS total_diagnosis_tercatat,
    (
        SELECT count(DISTINCT o.nama_puskesmas)
        FROM {{ ref('res_link') }} AS l
        JOIN {{ ref('organizations') }} AS o ON l.target_resource_id = o.organization_id
        WHERE l.source_resource_type = 'Encounter'
            AND l.src_path = 'Encounter.serviceProvider'
            AND l.target_resource_type = 'Organization'
    ) AS puskesmas_aktif,
    (SELECT count(*) FROM {{ ref('conditions') }} WHERE kategori_ptm = 'Hipertensi') AS kasus_hipertensi,
    (SELECT count(*) FROM {{ ref('conditions') }} WHERE kategori_ptm = 'Diabetes Melitus') AS kasus_diabetes,
    -- Kesegaran data dihitung dari bronze langsung (termasuk resource yang sudah dihapus/diupdate)
    (SELECT max(res_updated) FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL WHERE r._peerdb_is_deleted = 0) AS data_terakhir_diperbarui
