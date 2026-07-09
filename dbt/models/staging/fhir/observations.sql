{{ config(
    materialized='table',
    alias='observations',
    engine='MergeTree()',
    order_by=['patient_ref', 'tanggal_periksa', 'loinc_code']
) }}

-- Layer SILVER: hasil pengukuran vital sign (Observation) terekstrak dari JSON.
-- Nilai 0 pada value_quantity dianggap data tidak valid/placeholder (dikecualikan di model agregasi, bukan di sini,
-- supaya model ini tetap representasi 1:1 dari data mentah).
SELECT
    r.res_id AS observation_id,
    toDate(JSON_VALUE(v.res_text_vc, '$.effectiveDateTime')) AS tanggal_periksa,
    JSON_VALUE(v.res_text_vc, '$.subject.reference') AS patient_ref,
    JSON_VALUE(v.res_text_vc, '$.encounter.reference') AS encounter_ref,
    JSON_VALUE(v.res_text_vc, '$.code.coding[0].code') AS loinc_code,
    JSON_VALUE(v.res_text_vc, '$.code.coding[0].display') AS nama_pengukuran,
    toFloat64OrNull(JSON_VALUE(v.res_text_vc, '$.valueQuantity.value')) AS nilai,
    JSON_VALUE(v.res_text_vc, '$.valueQuantity.unit') AS satuan
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE r._peerdb_is_deleted = 0
    AND v._peerdb_is_deleted = 0
    AND r.res_type = 'Observation'
    AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
