{{ config(
    materialized='table',
    alias='resource',
    engine='MergeTree()',
    order_by=['res_type', 'res_id']
) }}

-- Layer SILVER: metadata resource FHIR yang sudah dibersihkan.
-- materialized='table': dedup (FINAL) dihitung SEKALI saat dbt run, bukan tiap kali chart Superset dibuka.
-- FINAL dipakai karena tabel bronze bertipe ReplacingMergeTree (dedup versi dari CDC PeerDB).
-- res_deleted_at di ClickHouse tidak Nullable -- nilai '1970-01-01 00:00:00' (epoch) berarti belum dihapus.
SELECT
    res_id,
    res_type,
    res_ver,
    res_published,
    res_updated,
    (res_deleted_at != toDateTime64('1970-01-01 00:00:00', 6)) AS is_deleted
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
WHERE _peerdb_is_deleted = 0
