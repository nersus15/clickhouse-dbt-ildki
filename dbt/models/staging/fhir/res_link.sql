{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='res_link',
    engine="ReplacingMergeTree(_synced_at, is_deleted)",
    order_by=['pid']
) }}

-- Layer SILVER: relasi antar-resource (foreign key FHIR), dibersihkan dari duplikasi CDC.
-- Dipakai di model agregasi untuk join Encounter->Organization, CarePlan->Encounter, dst.
-- INCREMENTAL -- baca ulang WAJIB FINAL. Watermark: macros/cdc_incremental.sql.
-- Kunci dedup = pid saja: saat resource di-update HAPI menghapus baris link lama (pid lama)
-- lalu menulis baru. Baris hapus ikut ditulis dengan is_deleted=1 supaya FINAL membuang
-- link lama -- sebelumnya link basi menumpuk selamanya. Baris hapus PeerDB bisa cuma berisi
-- PK, jadi kunci & versi (_synced_at) tidak boleh bergantung pada kolom lain.
SELECT
    pid,
    src_path,
    source_resource_type,
    src_resource_id,
    target_resource_type,
    target_resource_id,
    sp_updated,
    l._peerdb_is_deleted AS is_deleted,
    {{ cdc_synced_at(['l']) }}
FROM {{ source('fhirhapi_prod', 'hfj_res_link') }} AS l FINAL
{% if is_incremental() %}
WHERE l._peerdb_synced_at > {{ cdc_watermark() }}
{% endif %}
