{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='conditions',
    engine="ReplacingMergeTree(_synced_at, is_deleted)",
    order_by=['condition_id']
) }}

-- Layer SILVER: diagnosis (Condition) dengan kode & nama ICD-10 terekstrak dari JSON.
-- INCREMENTAL -- baca ulang WAJIB FINAL. Watermark & delete: macros/cdc_incremental.sql.
-- tanggal_diagnosis: onsetDateTime -> recordedDate -> res_published. Kedua field klinis
-- jarang diisi faskes (253 dari 78 ribu, 2026-11-11), jadi model agregasi sebaiknya
-- pakai tanggal Encounter lewat encounter_ref (lihat penyakit_prioritas_trend).
SELECT
    r.res_id AS condition_id,
    {{ fhir_date_wib(
        "coalesce(nullIf(JSON_VALUE(v.res_text_vc, '$.onsetDateTime'), ''), JSON_VALUE(v.res_text_vc, '$.recordedDate'))",
        'r.res_published') }} AS tanggal_diagnosis,
    r.res_published,
    r.res_updated,
    JSON_VALUE(v.res_text_vc, '$.subject.reference') AS patient_ref,
    JSON_VALUE(v.res_text_vc, '$.encounter.reference') AS encounter_ref,
    JSON_VALUE(v.res_text_vc, '$.code.coding[0].code') AS icd10_code,
    JSON_VALUE(v.res_text_vc, '$.code.coding[0].display') AS diagnosis_name,
    JSON_VALUE(v.res_text_vc, '$.clinicalStatus.coding[0].code') AS clinical_status,
    -- Penyakit prioritas, dipakai berulang di model agregasi. Rentang mengikuti blok ICD-10:
    -- I10-I15 penyakit hipertensi, E10-E14 diabetes melitus (E15/E16 hipoglikemia BUKAN DM),
    -- E45 stunting ("nutritional stunting / short stature" ada di inclusion term E45).
    multiIf(
        match(JSON_VALUE(v.res_text_vc, '$.code.coding[0].code'), '^I1[0-5]'), 'Hipertensi',
        match(JSON_VALUE(v.res_text_vc, '$.code.coding[0].code'), '^E1[0-4]'), 'Diabetes Melitus',
        match(JSON_VALUE(v.res_text_vc, '$.code.coding[0].code'), '^E45'), 'Stunting',
        'Lainnya'
    ) AS kategori_ptm,
    {{ cdc_is_deleted('r') }},
    {{ cdc_synced_at(['r', 'v']) }}
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
LEFT JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE {{ cdc_res_type_filter('Condition', 'condition_id') }}
    AND (v.res_id != 0 OR is_deleted = 1)
    {{ cdc_changed_res_ids('Condition') }}
