{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='observations',
    engine="ReplacingMergeTree(res_updated)",
    order_by=['observation_id']
) }}

-- Layer SILVER: hasil pengukuran vital sign (Observation) terekstrak dari JSON.
-- INCREMENTAL: Observation biasanya resource dengan volume PALING TINGGI (beberapa pengukuran
-- per kunjungan) -- prioritas utama pola incremental. Baca ulang -> WAJIB FINAL.
-- Nilai 0 pada value_quantity dianggap data tidak valid/placeholder (dikecualikan di model agregasi, bukan di sini,
-- supaya model ini tetap representasi 1:1 dari data mentah).
-- CATATAN: effectiveDateTime dari FHIR berformat ISO 8601 LENGKAP dengan jam & timezone offset
-- (mis. '2026-07-03T09:09:00+07:00'), BUKAN cuma tanggal. toDate() ClickHouse tidak bisa parse
-- string seperti ini langsung (cuma nerima 'YYYY-MM-DD') -- makanya di-parse dulu lewat
-- parseDateTimeBestEffortOrNull() (yang paham berbagai format ISO 8601 + timezone), baru dibungkus
-- toDate(). *OrNull dipakai supaya baris dengan effectiveDateTime kosong/rusak tidak bikin seluruh
-- dbt run gagal -- hasilnya jadi NULL, bukan exception.
-- CATATAN EDGE CASE (bukan error): toDate() pada DateTime hasil parse akan pakai timezone
-- server ClickHouse (default UTC) untuk menentukan tanggal kalender -- pengukuran jam 00:xx
-- WIB (+07:00) yang sebenarnya "hari X" bisa jadi tercatat sebagai "hari X-1" di UTC. Dampak
-- ke chart tren harian minimal (cuma di sekitar tengah malam), belum diperbaiki karena butuh
-- keputusan eksplisit timezone mana yang jadi acuan pelaporan (WIB vs UTC).
SELECT
    r.res_id AS observation_id,
    r.res_updated,
    toDate(parseDateTimeBestEffortOrNull(JSON_VALUE(v.res_text_vc, '$.effectiveDateTime'))) AS tanggal_periksa,
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
{% if is_incremental() %}
    AND r.res_updated > (SELECT max(res_updated) FROM {{ this }})
{% endif %}
