{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='encounters',
    engine="ReplacingMergeTree(_synced_at, is_deleted)",
    order_by=['encounter_id']
) }}

-- Layer SILVER: kunjungan (Encounter). INCREMENTAL -- baca ulang WAJIB FINAL.
-- Watermark & penanganan delete: lihat macros/cdc_incremental.sql. Baris resource yang
-- dihapus tetap ditulis dengan is_deleted=1 supaya FINAL membuang versi aktif lamanya.
-- tanggal_kunjungan = Encounter.period.start (waktu pelayanan, WIB), bukan res_published
-- (waktu data masuk server -- bisa telat sampai 66 hari, audit 2026-11-10).
SELECT
    r.res_id AS encounter_id,
    r.fhir_id AS encounter_fhir_id,
    {{ fhir_date_wib("JSON_VALUE(v.res_text_vc, '$.period.start')", 'r.res_published') }} AS tanggal_kunjungan,
    r.res_published,
    r.res_updated,
    JSON_VALUE(v.res_text_vc, '$.status') AS status,
    JSON_VALUE(v.res_text_vc, '$.class.code') AS class_code,
    JSON_VALUE(v.res_text_vc, '$.subject.reference') AS patient_ref,
    {{ cdc_is_deleted('r') }},
    {{ cdc_synced_at(['r', 'v']) }}
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
LEFT JOIN {{ source('fhirhapi_prod', 'hfj_res_ver') }} AS v FINAL
    ON r.res_id = v.res_id AND r.res_ver = v.res_ver
WHERE {{ cdc_res_type_filter('Encounter', 'encounter_id') }}
    -- versi JSON belum tiba (CDC telat) -> tunda, kecuali resource-nya memang sudah dihapus
    AND (v.res_id != 0 OR is_deleted = 1)
    {{ cdc_changed_res_ids('Encounter') }}
