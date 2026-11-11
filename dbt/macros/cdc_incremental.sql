{#
    Helper incremental untuk model ekstraksi dari bronze (CDC PeerDB).

    Watermark pakai _peerdb_synced_at (waktu baris TIBA di ClickHouse), BUKAN res_updated
    (waktu menurut HAPI). CDC tidak menjamin urutan: baris hfj_res_ver bisa tiba setelah
    hfj_resource-nya, atau transaksi Postgres commit tidak urut res_updated. Dengan
    watermark res_updated baris yang telat tiba hilang permanen (audit 2026-11-10: 318
    Observation). Lookback menutup selisih jam antar tabel; baris yang terbaca dua kali
    di-dedup ReplacingMergeTree.
#}

{# Versi terbaru _peerdb_synced_at dari tabel-tabel bronze yang di-JOIN, disimpan di silver sebagai watermark. #}
{% macro cdc_synced_at(aliases) -%}
    greatest({% for a in aliases %}{{ a }}._peerdb_synced_at{% if not loop.last %}, {% endif %}{% endfor %}) AS _synced_at
{%- endmacro %}

{% macro cdc_watermark(lookback='15 MINUTE') -%}
    (SELECT max(_synced_at) - INTERVAL {{ lookback }} FROM {{ this }})
{%- endmacro %}

{#
    Filter resource bertipe res_type yang berubah di hfj_resource ATAU hfj_res_ver sejak
    watermark. Dipakai sebagai `r.res_id IN (...)` supaya filter turun ke kedua sisi JOIN
    (OR lintas tabel setelah JOIN tidak bisa di-pushdown).

    Baris hard delete PeerDB bisa saja cuma berisi primary key (tergantung REPLICA IDENTITY
    tabel Postgres) -- res_type-nya kosong. Karena itu baris _peerdb_is_deleted=1 juga
    dicocokkan lewat res_id yang sudah ada di tabel ini (id_col).
#}
{% macro cdc_res_type_filter(res_type, id_col) -%}
    (r.res_type = '{{ res_type }}'
    {%- if is_incremental() %}
        OR (r._peerdb_is_deleted = 1 AND r.res_id IN (SELECT {{ id_col }} FROM {{ this }}))
    {%- endif %})
{%- endmacro %}

{% macro cdc_changed_res_ids(res_type) -%}
    {% if is_incremental() -%}
    AND r.res_id IN (
        SELECT res_id FROM {{ source('fhirhapi_prod', 'hfj_resource') }}
        WHERE (res_type = '{{ res_type }}' OR _peerdb_is_deleted = 1)
            AND _peerdb_synced_at > {{ cdc_watermark() }}
        UNION ALL
        SELECT res_id FROM {{ source('fhirhapi_prod', 'hfj_res_ver') }}
        WHERE res_type = '{{ res_type }}' AND _peerdb_synced_at > {{ cdc_watermark() }}
    )
    {%- endif %}
{%- endmacro %}

{#
    1 = resource sudah dihapus (soft delete HAPI: res_deleted_at terisi, atau hard
    delete/expunge: baris bronze ditandai PeerDB). Dipakai sebagai kolom is_deleted di
    engine ReplacingMergeTree(_synced_at, is_deleted) -- FINAL otomatis membuang baris ini.
    Versi engine = _synced_at, BUKAN res_updated: baris hard delete yang cuma berisi PK
    punya res_updated epoch dan akan kalah dari versi aktif lamanya.
#}
{% macro cdc_is_deleted(alias='r') -%}
    toUInt8({{ alias }}.res_deleted_at != toDateTime64('1970-01-01 00:00:00', 6)
            OR {{ alias }}._peerdb_is_deleted = 1) AS is_deleted
{%- endmacro %}

{# ISO 8601 dari JSON FHIR (dengan offset +07:00) -> Date kalender WIB, fallback ke kolom lain. #}
{% macro fhir_date_wib(json_expr, fallback) -%}
    toDate(coalesce(parseDateTimeBestEffortOrNull({{ json_expr }}), {{ fallback }}), 'Asia/Jakarta')
{%- endmacro %}
