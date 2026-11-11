{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='resource',
    engine="ReplacingMergeTree(_synced_at)",
    order_by=['res_id']
) }}

-- Layer SILVER: metadata resource FHIR yang sudah dibersihkan.
-- INCREMENTAL (append + ReplacingMergeTree): versi duplikat (res_id sama) di-dedup oleh
-- ReplacingMergeTree(_synced_at). SEMUA query pembaca tabel ini WAJIB pakai FINAL.
-- order_by cuma res_id: baris hard delete PeerDB bisa saja cuma berisi PK (res_type kosong),
-- jadi kunci dedup tidak boleh memuat kolom selain PK. Resource yang di-expunge dengan
-- cara itu akan muncul dengan res_type '' di resource_type_summary.
-- Beda dari model ekstraksi lain: is_deleted SENGAJA bukan parameter engine, karena
-- resource_type_summary & kpi_totals justru perlu menghitung resource yang dihapus.
-- Watermark: macros/cdc_incremental.sql.
SELECT
    res_id,
    res_type,
    res_ver,
    res_published,
    res_updated,
    (res_deleted_at != toDateTime64('1970-01-01 00:00:00', 6) OR _peerdb_is_deleted = 1) AS is_deleted,
    {{ cdc_synced_at(['r']) }}
FROM {{ source('fhirhapi_prod', 'hfj_resource') }} AS r FINAL
{% if is_incremental() %}
WHERE r._peerdb_synced_at > {{ cdc_watermark() }}
{% endif %}
