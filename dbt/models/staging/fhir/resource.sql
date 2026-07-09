{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='resource',
    engine="ReplacingMergeTree(res_updated)",
    order_by=['res_type', 'res_id']
) }}

-- Layer SILVER: metadata resource FHIR yang sudah dibersihkan.
-- INCREMENTAL (append + ReplacingMergeTree): tiap dbt run cuma ambil baris dengan res_updated
-- lebih baru dari watermark terakhir, lalu APPEND (bukan rebuild total). Versi duplikat (res_id
-- sama) di-dedup oleh ReplacingMergeTree(res_updated) -- menang yang res_updated lebih besar.
-- Konsekuensi: SEMUA query pembaca tabel ini WAJIB pakai FINAL (lihat model-model agregasi).
-- FINAL di FROM tetap dipakai untuk baca bronze (StarRocks/ClickHouse ReplacingMergeTree bawaan CDC PeerDB).
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
{% if is_incremental() %}
  AND r.res_updated > (SELECT max(res_updated) FROM {{ this }})
{% endif %}
