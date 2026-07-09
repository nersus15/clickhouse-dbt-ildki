{{ config(materialized='view', alias='mart_kpi_totals') }}

-- Chart: "KPI Kebijakan Kesehatan"
-- KPI ringkas untuk pejabat/pengambil keputusan: total pasien, kunjungan, diagnosis,
-- puskesmas aktif, kasus hipertensi & diabetes, dan kesegaran data.
SELECT
    (SELECT count(*) FROM {{ ref('stg_resource') }} WHERE res_type = 'Patient' AND NOT is_deleted) AS total_pasien,
    (SELECT count(*) FROM {{ ref('stg_resource') }} WHERE res_type = 'Encounter' AND NOT is_deleted) AS total_kunjungan,
    (SELECT count(*) FROM {{ ref('stg_resource') }} WHERE res_type = 'Condition' AND NOT is_deleted) AS total_diagnosis_tercatat,
    (
        SELECT count(DISTINCT o.nama_puskesmas)
        FROM {{ ref('stg_res_link') }} AS l
        JOIN {{ ref('stg_organizations') }} AS o ON l.target_resource_id = o.organization_id
        WHERE l.source_resource_type = 'Encounter'
            AND l.src_path = 'Encounter.serviceProvider'
            AND l.target_resource_type = 'Organization'
    ) AS puskesmas_aktif,
    (SELECT count(*) FROM {{ ref('stg_conditions') }} WHERE kategori_ptm = 'Hipertensi') AS kasus_hipertensi,
    (SELECT count(*) FROM {{ ref('stg_conditions') }} WHERE kategori_ptm = 'Diabetes Melitus') AS kasus_diabetes,
    -- Kesegaran data dihitung dari bronze langsung (termasuk resource yang sudah dihapus/diupdate)
    (SELECT max(res_updated) FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL WHERE r._peerdb_is_deleted = 0) AS data_terakhir_diperbarui
