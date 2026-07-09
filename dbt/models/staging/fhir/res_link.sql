{{ config(
    materialized='incremental',
    alias='res_link',
    engine="ReplacingMergeTree(sp_updated)",
    order_by=['source_resource_type', 'src_path', 'src_resource_id', 'target_resource_type', 'target_resource_id']
) }}

-- Layer SILVER: relasi antar-resource (foreign key FHIR), dibersihkan dari duplikasi CDC.
-- Dipakai di model agregasi untuk join Encounter->Organization, CarePlan->Encounter, dst.
-- INCREMENTAL: relasi tumbuh proporsional dengan Encounter/Condition dkk. Watermark pakai
-- sp_updated (timestamp update relasi ini di HAPI FHIR), bukan res_updated (tabel ini bukan
-- per-resource, jadi tidak ada kolom res_updated). Baca ulang -> WAJIB FINAL.
SELECT
    src_path,
    source_resource_type,
    src_resource_id,
    target_resource_type,
    target_resource_id,
    sp_updated
FROM {{ source('fhirhapi_prod', 'hfj_res_link') }} AS l FINAL
WHERE l._peerdb_is_deleted = 0
{% if is_incremental() %}
    AND l.sp_updated > (SELECT max(sp_updated) FROM {{ this }})
{% endif %}
