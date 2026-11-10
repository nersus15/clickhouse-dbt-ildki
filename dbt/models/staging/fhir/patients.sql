{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='patients',
    engine="ReplacingMergeTree(res_updated)",
    order_by=['patient_res_id']
) }}

-- Layer SILVER: pasien (Patient), ekstraksi 1:1 dari bronze.
-- Sumbernya tabel tunggal (tanpa JOIN hfj_res_ver), jadi aman dikonversi jadi
-- materialized_view nanti. Baca ulang -> WAJIB FINAL.
SELECT
    r.res_id AS patient_res_id,
    r.fhir_id AS patient_id,
    r.res_updated
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
WHERE r._peerdb_is_deleted = 0
    AND r.res_type = 'Patient'
    AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
{% if is_incremental() %}
    AND r.res_updated > (SELECT max(res_updated) FROM {{ this }})
{% endif %}
