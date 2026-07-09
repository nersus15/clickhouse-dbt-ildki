{{ config(
    materialized='table',
    alias='clinical_status_summary',
    engine='MergeTree()',
    order_by=['res_type', 'status']
) }}

-- Chart: "Breakdown Status Klinis per Resource Type" (dashboard tambahan/arsip -- terlalu teknis
-- untuk dashboard kebijakan utama, tapi tetap disediakan sebagai referensi teknis)
-- Sumbernya bronze langsung (hfj_spidx_token, indeks search parameter FHIR bertipe token) --
-- BUKAN dari salah satu tabel ekstraksi (resource/encounters/dst) karena field "status" FHIR
-- ada di hampir semua jenis resource dengan makna yang beda-beda (mis. Encounter.status vs
-- Observation.status), dan HAPI FHIR sudah mengekstraknya seragam ke sini lewat sp_name='status'.
-- FINAL + filter _peerdb_is_deleted dipakai karena tabel bronze ReplacingMergeTree (dedup CDC PeerDB).
SELECT
    res_type,
    sp_value AS status,
    count(*) AS total
FROM {{ source('fhirhapi_prod', 'hfj_spidx_token') }} AS t FINAL
WHERE t._peerdb_is_deleted = 0
    AND t.sp_name = 'status'
GROUP BY res_type, sp_value
