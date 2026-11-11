{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='careplans',
    engine="ReplacingMergeTree(_synced_at, is_deleted)",
    order_by=['careplan_id']
) }}

-- Layer SILVER: rencana perawatan (CarePlan), termasuk tatalaksana weight faltering.
-- INCREMENTAL -- baca ulang WAJIB FINAL. Watermark & delete: macros/cdc_incremental.sql.
-- tanggal = CarePlan.created (WIB), bukan res_published.
SELECT
    r.res_id AS careplan_id,
    {{ fhir_date_wib("JSON_VALUE(v.res_text_vc, '$.created')", 'r.res_published') }} AS tanggal,
    r.res_published,
    r.res_updated,
    JSON_VALUE(v.res_text_vc, '$.status') AS status,
    JSON_VALUE(v.res_text_vc, '$.subject.reference') AS patient_ref,
    JSON_VALUE(v.res_text_vc, '$.encounter.reference') AS encounter_ref,
    JSON_VALUE(v.res_text_vc, '$.category[0].coding[0].code') AS kategori_code,
    JSON_VALUE(v.res_text_vc, '$.category[0].coding[0].display') AS kategori_careplan,
    JSON_VALUE(v.res_text_vc, '$.description') AS deskripsi,
    {{ cdc_is_deleted('r') }},
    {{ cdc_synced_at(['r', 'v']) }}
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
LEFT JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE {{ cdc_res_type_filter('CarePlan', 'careplan_id') }}
    AND (v.res_id != 0 OR is_deleted = 1)
    {{ cdc_changed_res_ids('CarePlan') }}
