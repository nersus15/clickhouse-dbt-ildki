{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='conditions',
    engine="ReplacingMergeTree(res_updated)",
    order_by=['condition_id']
) }}

-- Layer SILVER: diagnosis (Condition) dengan kode & nama ICD-10 terekstrak dari JSON.
-- INCREMENTAL: volume Condition ikut tinggi seiring pertumbuhan Encounter. Baca ulang -> WAJIB FINAL.
SELECT
    r.res_id AS condition_id,
    toDate(r.res_published) AS tanggal_diagnosis,
    r.res_published,
    r.res_updated,
    JSON_VALUE(v.res_text_vc, '$.subject.reference') AS patient_ref,
    JSON_VALUE(v.res_text_vc, '$.code.coding[0].code') AS icd10_code,
    JSON_VALUE(v.res_text_vc, '$.code.coding[0].display') AS diagnosis_name,
    JSON_VALUE(v.res_text_vc, '$.clinicalStatus.coding[0].code') AS clinical_status,
    -- Flag khusus penyakit tidak menular prioritas, dipakai berulang di model agregasi
    CASE
        WHEN JSON_VALUE(v.res_text_vc, '$.code.coding[0].code') LIKE 'I1%' THEN 'Hipertensi'
        WHEN JSON_VALUE(v.res_text_vc, '$.code.coding[0].code') LIKE 'E1%' THEN 'Diabetes Melitus'
        ELSE 'Lainnya'
    END AS kategori_ptm
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE r._peerdb_is_deleted = 0
    AND v._peerdb_is_deleted = 0
    AND r.res_type = 'Condition'
    AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
{% if is_incremental() %}
    AND r.res_updated > (SELECT max(res_updated) FROM {{ this }})
{% endif %}
