{{ config(
    materialized='table',
    alias='patients',
    engine='MergeTree()',
    order_by=['patient_res_id']
) }}

-- Layer SILVER: pasien (Patient) beserta encounter terkait (kalau ada), dari relasi Encounter.subject.
-- SENGAJA MASIH 'table' (full rebuild), BUKAN incremental, walau model ekstraksi lain sudah:
-- model ini JOIN 2 resource type (Patient + Encounter) + link table sekaligus, jadi baris hasil
-- akhirnya bisa berubah walau res_updated milik Patient itu sendiri TIDAK berubah (mis. Encounter
-- barunya update, bukan Patient-nya). Watermark sederhana berbasis 1 kolom res_updated tidak cukup
-- untuk mendeteksi ini -> incremental di sini rawan silently miss data. Volume Patient juga jauh
-- lebih rendah dibanding Encounter/Observation, jadi biaya full rebuild relatif kecil.
-- FINAL + filter _peerdb_is_deleted dipakai karena tabel bronze ReplacingMergeTree (dedup CDC PeerDB).
-- res_deleted_at di ClickHouse TIDAK Nullable -- "belum dihapus" direpresentasikan sebagai epoch
-- '1970-01-01 00:00:00', BUKAN NULL. Filter 'IS NULL' tidak akan pernah match (selalu 0 baris).

WITH fhir_resources AS (
    SELECT
        res_id,
        fhir_id AS patient_id,
        res_type
    FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
    WHERE r._peerdb_is_deleted = 0
      AND r.res_type = 'Patient'
      AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
),

fhir_encounters AS (
    SELECT
        res_id,
        fhir_id AS encounter_id,
        res_type
    FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
    WHERE r._peerdb_is_deleted = 0
      AND r.res_type = 'Encounter'
      AND r.res_deleted_at = toDateTime64('1970-01-01 00:00:00', 6)
),

resource_links AS (
    SELECT
        src_resource_id,
        target_resource_id
    FROM {{ source('fhirhapi_prod', 'hfj_res_link') }} AS l FINAL
    WHERE l._peerdb_is_deleted = 0
      AND l.src_path = 'Encounter.subject'
)

SELECT
    p.res_id AS patient_res_id,
    p.patient_id,
    e.encounter_id
FROM fhir_resources AS p
LEFT JOIN resource_links AS rl ON p.res_id = rl.target_resource_id
LEFT JOIN fhir_encounters AS e ON rl.src_resource_id = e.res_id
