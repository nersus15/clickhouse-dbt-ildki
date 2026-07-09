{{ config(materialized='view', alias='stg_res_link') }}

-- Layer SILVER: relasi antar-resource (foreign key FHIR), dibersihkan dari duplikasi CDC.
-- Dipakai di layer marts untuk join Encounter->Organization, CarePlan->Encounter, dst,
-- supaya model marts tidak perlu baca tabel bronze hfj_res_link secara langsung.
SELECT
    src_path,
    source_resource_type,
    src_resource_id,
    target_resource_type,
    target_resource_id
FROM {{ source('fhirhapi_prod', 'hfj_res_link') }} AS l FINAL
WHERE l._peerdb_is_deleted = 0
