{{ config(
    materialized='table',
    alias='organizations',
    engine='MergeTree()',
    order_by=['organization_id']
) }}

-- Layer SILVER: daftar Puskesmas/faskes (resource Organization) dengan nama bersih dari JSON.
SELECT
    r.res_id AS organization_id,
    JSON_VALUE(v.res_text_vc, '$.name') AS nama_puskesmas
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE r._peerdb_is_deleted = 0
    AND v._peerdb_is_deleted = 0
    AND r.res_type = 'Organization'
    AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
