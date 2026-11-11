{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='observations',
    engine="ReplacingMergeTree(_synced_at, is_deleted)",
    order_by=['observation_id']
) }}

-- Layer SILVER: hasil pengukuran (Observation) terekstrak dari JSON.
-- INCREMENTAL -- baca ulang WAJIB FINAL. Watermark & delete: macros/cdc_incremental.sql.
-- Nilai 0 pada nilai dianggap placeholder (dikecualikan di model agregasi, bukan di sini,
-- supaya model ini tetap 1:1 dengan data mentah).
-- loinc_code berisi Observation.code.coding[0].code apa pun sistemnya (LOINC, SNOMED, atau
-- clinical-term Kemkes) -- nama kolom dipertahankan demi kompatibilitas dataset Superset;
-- pakai code_system untuk membedakan.
-- tanggal_periksa: effectiveDateTime dikonversi ke tanggal WIB. Fallback epoch supaya kolom
-- tetap non-Nullable; baris '1970-01-01' = effectiveDateTime kosong/rusak.
-- JANGAN tambah loinc_code ke order_by: ReplacingMergeTree dedup per seluruh kunci, jadi
-- Observation yang kodenya dikoreksi akan meninggalkan baris lama yang tak pernah terhapus.
SELECT
    r.res_id AS observation_id,
    r.res_updated,
    {{ fhir_date_wib("JSON_VALUE(v.res_text_vc, '$.effectiveDateTime')", "toDateTime64('1970-01-01 00:00:00', 6)") }} AS tanggal_periksa,
    JSON_VALUE(v.res_text_vc, '$.subject.reference') AS patient_ref,
    JSON_VALUE(v.res_text_vc, '$.encounter.reference') AS encounter_ref,
    JSON_VALUE(v.res_text_vc, '$.code.coding[0].system') AS code_system,
    JSON_VALUE(v.res_text_vc, '$.code.coding[0].code') AS loinc_code,
    JSON_VALUE(v.res_text_vc, '$.code.coding[0].display') AS nama_pengukuran,
    toFloat64OrNull(JSON_VALUE(v.res_text_vc, '$.valueQuantity.value')) AS nilai,
    JSON_VALUE(v.res_text_vc, '$.valueQuantity.unit') AS satuan,
    JSON_VALUE(v.res_text_vc, '$.interpretation[0].coding[0].code') AS interpretasi_code,
    {{ cdc_is_deleted('r') }},
    {{ cdc_synced_at(['r', 'v']) }}
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
LEFT JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE {{ cdc_res_type_filter('Observation', 'observation_id') }}
    AND (v.res_id != 0 OR is_deleted = 1)
    {{ cdc_changed_res_ids('Observation') }}
