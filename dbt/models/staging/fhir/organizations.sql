{{ config(
    materialized='incremental',
    alias='organizations',
    engine="ReplacingMergeTree(res_updated)",
    order_by=['organization_id']
) }}

-- Layer SILVER: daftar Puskesmas/faskes (resource Organization) dengan nama bersih dari JSON.
-- INCREMENTAL: volume Organization sendiri rendah, tapi dibuat konsisten dengan model ekstraksi
-- lain supaya polanya seragam. Baca ulang -> WAJIB FINAL (lihat model-model agregasi).
SELECT
    r.res_id AS organization_id,
    JSON_VALUE(v.res_text_vc, '$.name') AS nama_puskesmas,
    r.res_updated
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE r._peerdb_is_deleted = 0
    AND v._peerdb_is_deleted = 0
    AND r.res_type = 'Organization'
    AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
{% if is_incremental() %}
    AND r.res_updated > (SELECT max(res_updated) FROM {{ this }})
{% endif %}
