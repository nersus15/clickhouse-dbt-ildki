{{ config(
    materialized='table',
    alias='kpi_totals',
    engine='MergeTree()',
    order_by=['data_terakhir_diperbarui']
) }}

-- Chart: "KPI Kebijakan Kesehatan"
-- KPI ringkas untuk pejabat/pengambil keputusan: total pasien, kunjungan, diagnosis,
-- puskesmas aktif, kasus hipertensi & diabetes, dan kesegaran data.
-- REFACTOR: sebelumnya 7 sub-query terpisah (masing-masing scan ulang tabel), sekarang
-- resource & conditions cuma di-scan SEKALI pakai countIf() (conditional aggregation).
-- resource, res_link, organizations, conditions materialized='incremental' -- WAJIB FINAL.
WITH resource_summary AS (
    SELECT
        countIf(res_type = 'Patient' AND NOT is_deleted) AS total_pasien,
        countIf(res_type = 'Encounter' AND NOT is_deleted) AS total_kunjungan,
        countIf(res_type = 'Condition' AND NOT is_deleted) AS total_diagnosis_tercatat
    FROM {{ ref('resource') }} FINAL
),
condition_summary AS (
    SELECT
        countIf(kategori_ptm = 'Hipertensi') AS kasus_hipertensi,
        countIf(kategori_ptm = 'Diabetes Melitus') AS kasus_diabetes
    FROM {{ ref('conditions') }} FINAL
),
faskes_summary AS (
    SELECT count(DISTINCT o.nama_puskesmas) AS puskesmas_aktif
    FROM {{ ref('res_link') }} AS l FINAL
    JOIN {{ ref('organizations') }} AS o FINAL ON l.target_resource_id = o.organization_id
    WHERE l.source_resource_type = 'Encounter'
        AND l.src_path = 'Encounter.serviceProvider'
        AND l.target_resource_type = 'Organization'
),
freshness AS (
    -- Kesegaran data dihitung dari bronze langsung (termasuk resource yang sudah dihapus/diupdate)
    SELECT max(res_updated) AS data_terakhir_diperbarui
    FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
    WHERE r._peerdb_is_deleted = 0
)
SELECT
    rs.total_pasien,
    rs.total_kunjungan,
    rs.total_diagnosis_tercatat,
    fk.puskesmas_aktif,
    cs.kasus_hipertensi,
    cs.kasus_diabetes,
    fr.data_terakhir_diperbarui
FROM resource_summary AS rs
CROSS JOIN condition_summary AS cs
CROSS JOIN faskes_summary AS fk
CROSS JOIN freshness AS fr
