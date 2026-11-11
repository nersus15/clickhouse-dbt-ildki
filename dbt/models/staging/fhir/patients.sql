{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='patients',
    engine="ReplacingMergeTree(_synced_at, is_deleted)",
    order_by=['patient_res_id']
) }}

-- Layer SILVER: pasien (Patient), ekstraksi 1:1 dari bronze.
-- Sumbernya tabel tunggal (tanpa JOIN hfj_res_ver), jadi aman dikonversi jadi
-- materialized_view nanti. Baca ulang -> WAJIB FINAL. Watermark & delete: macros/cdc_incremental.sql.
SELECT
    r.res_id AS patient_res_id,
    r.fhir_id AS patient_id,
    r.res_updated,
    {{ cdc_is_deleted('r') }},
    {{ cdc_synced_at(['r']) }}
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
WHERE {{ cdc_res_type_filter('Patient', 'patient_res_id') }}
{% if is_incremental() %}
    AND r._peerdb_synced_at > {{ cdc_watermark() }}
{% endif %}
