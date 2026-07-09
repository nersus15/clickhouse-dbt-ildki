{{ config(
    materialized='table',
    alias='careplans',
    engine='MergeTree()',
    order_by=['careplan_id']
) }}

-- Layer SILVER: rencana perawatan (CarePlan) -- termasuk program weight faltering/risiko stunting.
SELECT
    r.res_id AS careplan_id,
    toDate(r.res_published) AS tanggal,
    r.res_published,
    JSON_VALUE(v.res_text_vc, '$.status') AS status,
    JSON_VALUE(v.res_text_vc, '$.subject.reference') AS patient_ref,
    JSON_VALUE(v.res_text_vc, '$.encounter.reference') AS encounter_ref,
    JSON_VALUE(v.res_text_vc, '$.category[0].coding[0].display') AS kategori_careplan,
    JSON_VALUE(v.res_text_vc, '$.description') AS deskripsi
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE r._peerdb_is_deleted = 0
    AND v._peerdb_is_deleted = 0
    AND r.res_type = 'CarePlan'
    AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
