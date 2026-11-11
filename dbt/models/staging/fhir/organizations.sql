{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='organizations',
    engine="ReplacingMergeTree(_synced_at, is_deleted)",
    order_by=['organization_id']
) }}

-- Layer SILVER: daftar Puskesmas/faskes (resource Organization) dengan nama bersih dari JSON.
-- INCREMENTAL -- baca ulang WAJIB FINAL. Watermark & delete: macros/cdc_incremental.sql.
SELECT
    r.res_id AS organization_id,
    JSON_VALUE(v.res_text_vc, '$.name') AS nama_puskesmas,
    r.res_updated,
    {{ cdc_is_deleted('r') }},
    {{ cdc_synced_at(['r', 'v']) }}
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
LEFT JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE {{ cdc_res_type_filter('Organization', 'organization_id') }}
    AND (v.res_id != 0 OR is_deleted = 1)
    {{ cdc_changed_res_ids('Organization') }}
