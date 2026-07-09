{{ config(
    materialized='table',
    alias='encounters',
    engine='MergeTree()',
    order_by=['encounter_id']
) }}

-- Layer SILVER: kunjungan (Encounter) dengan status & kelas layanan terekstrak dari JSON.
SELECT
    r.res_id AS encounter_id,
    toDate(r.res_published) AS tanggal_kunjungan,
    r.res_published,
    JSON_VALUE(v.res_text_vc, '$.status') AS status,
    JSON_VALUE(v.res_text_vc, '$.class.code') AS class_code,
    JSON_VALUE(v.res_text_vc, '$.subject.reference') AS patient_ref
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE r._peerdb_is_deleted = 0
    AND v._peerdb_is_deleted = 0
    AND r.res_type = 'Encounter'
    AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
