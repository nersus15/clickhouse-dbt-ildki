# ILDKI Data Pipeline: Prefect + dbt + ClickHouse

Versi ClickHouse dari [`prefect-dbt-ildki`](../prefect-dbt-ildki) (yang aslinya untuk StarRocks). Project ini mengimplementasikan layer **silver** dari arsitektur **bronze -> silver -> gold** untuk data HAPI FHIR (SATUSEHAT), dengan **dbt** sebagai lapisan transformasi dan **Prefect** sebagai orchestrator, di atas **ClickHouse** sebagai data warehouse.

## Arsitektur Data

| Layer | Lokasi | Isi |
|---|---|---|
| **Bronze** | Database `fhirhapi_prod` | Raw hasil CDC PeerDB dari PostgreSQL (tabel `public_hfj_*`, skema internal HAPI FHIR JPA) |
| **Silver** | Database `silver`, semua model di `models/staging/fhir/` | Ekstraksi 1:1 dari bronze + agregasi/join siap pakai chart Superset |
| **Gold** | *(belum dipakai)* | Rencana lanjutan: baru diimplementasikan saat perlu menggabungkan data dari lebih dari satu source aplikasi (mis. FHIR + Jaksimpus/sistem lain). Karena dashboard Superset saat ini cuma bersumber dari FHIR, layer silver saja sudah cukup untuk sekarang. |

### Konvensi penamaan

**Tidak ada prefix `stg_`/`mart_` pada nama model atau nama tabel/view di ClickHouse.** Semua model tinggal di satu folder `models/staging/{aplikasi}/` (untuk sekarang cuma `fhir/`), dan namanya cukup deskriptif dari isinya (mis. `conditions`, `top_diagnosis`) — karena "staging vs mart" itu sendiri sudah identik dengan "silver vs gold" secara konsep, prefix di nama akan jadi redundan/membingungkan.

### Strategi materialisasi (Opsi C + incremental, semua model sekarang tersimpan fisik)

| Kategori model | Materialized | Alasan |
|---|---|---|
| **Ekstraksi volume tinggi**<br>`resource`, `organizations`, `encounters`, `conditions`, `observations`, `careplans`, `res_link` | `incremental` (engine `ReplacingMergeTree(res_updated)` / `ReplacingMergeTree(sp_updated)` untuk `res_link`) | Volume Encounter/Observation/Condition di FHIR tumbuh cepat. `incremental` cuma proses baris baru/berubah sejak `dbt run` terakhir (watermark `res_updated`/`sp_updated`), bukan rebuild total tiap kali -- disiapkan dari awal supaya tidak perlu migrasi ulang saat data sudah besar. |
| **Ekstraksi kompleks, volume rendah**<br>`patients` | `table` (full rebuild) | JOIN 2 resource type (Patient+Encounter) + link table -- watermark 1 kolom tidak cukup mendeteksi semua kasus perubahan (lihat komentar di file). Volume Patient jauh lebih kecil dari Encounter, jadi full rebuild masih murah. |
| **Agregasi/join untuk chart**<br>`kpi_totals`, `encounter_class`, `resource_daily_growth`, `top_diagnosis`, `penyakit_prioritas_trend`, `utilisasi_faskes`, `encounter_trend_per_faskes`, `deteksi_dini_hipertensi`, `tren_tensi_harian`, `weight_faltering_per_faskes` | `table` (full rebuild, engine `MergeTree()`) | **Diambil langsung oleh Superset** -- disimpan fisik supaya baca dashboard = baca tabel biasa (instan, tanpa hitung ulang). BUKAN `incremental`: model ini semua `GROUP BY`/agregasi, dan incremental yang benar untuk agregasi butuh `AggregatingMergeTree` + kombinator `-State`/`-Merge` (baris baru bisa MENGUBAH grup yang sudah ada, bukan cuma nambah baris) -- jauh lebih kompleks & rawan salah hitung dibanding append biasa. Karena sumbernya sekarang tabel bersih (bukan bronze mentah), full rebuild agregasi ini tetap murah. |

**Artinya: sudah tidak ada model `view` sama sekali di project ini** -- semua 18 model (kecuali definisi `source`) menghasilkan tabel fisik di ClickHouse.

### Kenapa `incremental` + `ReplacingMergeTree`, bukan `delete+insert`?

dbt-clickhouse punya beberapa `incremental_strategy` (`append`, `delete+insert`, `insert_overwrite`). Project ini pakai pola **`append` + `ReplacingMergeTree`**, BUKAN `delete+insert`, karena:
- `delete+insert` di ClickHouse berjalan lewat **mutation** (`ALTER TABLE ... DELETE`) yang berat & async -- bukan `DELETE` OLTP biasa.
- `append` + `ReplacingMergeTree(res_updated)` jauh lebih murah: baris baru/berubah cuma di-`INSERT`, dan ClickHouse yang urus dedup baris dengan `res_id` sama di background (menang versi `res_updated` terbesar).
- Ini **persis pola yang sudah dipercaya di layer bronze** (`_peerdb_version`, `FINAL`) -- konsisten, bukan teknik baru.

**Konsekuensi yang WAJIB diingat:** karena dedup `ReplacingMergeTree` itu *eventual* (baru benar-benar tergabung saat background merge selesai), **setiap query yang baca tabel `incremental` ini HARUS pakai `FINAL`** supaya tidak menghitung baris duplikat. Ke-10 model `view` (agregasi) di project ini sudah disesuaikan (`{{ ref('conditions') }} FINAL`, dst) -- kalau Anda menambah model baru yang membaca tabel-tabel ini, jangan lupa tambahkan `FINAL` juga.

### Kapan tabel-tabel ini ter-update?

- **SEMUA model sekarang butuh `dbt run`/`dbt build` untuk ter-update** -- tidak ada lagi `view` yang otomatis real-time. Ini konsekuensi langsung dari keputusan "semua tersimpan fisik supaya Superset baca instan".
- Bedanya cuma seberapa besar kerja per run: `incremental` (staging) = cuma baris baru; `table` (semua chart + `patients`) = rebuild total, tapi murah karena sumbernya sudah tabel bersih.
- Schedule `prefect.yaml` (`cron: "* * * * *"`, tiap 1 menit) jadi **satu-satunya** mekanisme kesegaran data sekarang -- kalau job ini berhenti/gagal, seluruh dashboard Superset ikut "beku" di data terakhir, bukan cuma sebagian.
- **Run pertama** untuk model `incremental` otomatis full build juga (belum ada watermark pembanding) -- baru run kedua dan seterusnya benar-benar incremental.
- **Kalau logic SQL suatu model `incremental` diubah** (bukan cuma datanya), histori lama TIDAK otomatis ke-reprocess -- wajib `dbt run --select <model> --full-refresh` supaya rebuild total dengan logic baru. Model `table` biasa tidak perlu ini (selalu full rebuild by design).

## Kenapa migrasi dari StarRocks?

Sebelumnya CDC pakai Apache Flink (PostgreSQL -> StarRocks) yang dinilai terlalu berat secara resource. Sekarang dipakai **PeerDB** sebagai CDC (PostgreSQL -> ClickHouse), dan seluruh data warehouse dipindah ke ClickHouse.

## Perbedaan dari project StarRocks (`prefect-dbt-ildki`)

| Aspek | StarRocks (lama) | ClickHouse (project ini) |
|---|---|---|
| Adapter dbt | `dbt-starrocks` | `dbt-clickhouse` |
| Fungsi ekstraksi JSON | `get_json_string(col, '$.path')` | `JSON_VALUE(col, '$.path')` (syntax JSONPath sama persis) |
| Source data | Database `ildki`, tabel `logs__encounters` dkk (custom, **sudah tidak eksis** di pipeline sekarang) | Database `fhirhapi_prod`, tabel `public_hfj_resource`/`public_hfj_res_ver`/`public_hfj_res_link` (raw HAPI FHIR JPA dari PeerDB CDC) |
| Dedup versi | Otomatis (primary key model StarRocks) | Manual: `FINAL` + filter `_peerdb_is_deleted = 0` (tabel ReplacingMergeTree hasil CDC PeerDB) |
| Representasi "belum dihapus" | `deleted_at IS NULL` | `res_deleted_at = '1970-01-01 00:00:00'` (epoch; kolom tidak Nullable) |
| Fungsi tanggal | `timestampdiff()`, `str_to_date()` | `dateDiff()`, `toDate()`/`parseDateTimeBestEffortOrNull()` |
| Struktur folder model | `staging/` + `marts/` terpisah, prefix `stg_`/`mart_` | Satu folder `staging/{aplikasi}/`, tanpa prefix (lihat "Konvensi penamaan") |
| Env var koneksi | `STARROCKS_HOST/PORT/USER/PASS/DB` | `CLICKHOUSE_HOST/PORT/USER/PASSWORD/DATABASE` |

## Struktur Proyek

```text
├── prefect.yaml           # Konfigurasi Deployment Prefect (K8s job, env var ClickHouse)
├── dbt_runner.py           # Entrypoint Python (General Runner - JANGAN DIUBAH)
└── dbt/
    ├── dbt_project.yml     # Nama project, path model, macro
    ├── profiles.yml        # Konfigurasi koneksi ClickHouse (type: clickhouse)
    ├── macros/
    │   └── generate_schema_name.sql   # Disiapkan untuk nanti kalau layer gold mulai dipakai (belum aktif)
    └── models/
        └── staging/
            └── fhir/                  # Semua model bersumber dari FHIR (HAPI/SATUSEHAT)
                ├── schema.yml           # Definisi source (tabel bronze) + deskripsi tiap model
                │
                ├── patients.sql         ┐
                ├── resource.sql         │ materialized: incremental
                ├── organizations.sql    │ (ReplacingMergeTree + watermark res_updated/sp_updated;
                ├── encounters.sql       │  patients.sql masih 'table', lihat "Strategi materialisasi")
                ├── conditions.sql       │
                ├── observations.sql     │
                ├── careplans.sql        │
                ├── res_link.sql         ┘
                │
                ├── kpi_totals.sql                    ┐
                ├── encounter_class.sql                │
                ├── resource_daily_growth.sql          │
                ├── top_diagnosis.sql                  │ materialized: table
                ├── penyakit_prioritas_trend.sql       │ (agregasi/join, full rebuild tiap dbt run,
                ├── utilisasi_faskes.sql               │  1 model = 1 chart Superset, dibaca langsung
                ├── encounter_trend_per_faskes.sql     │  sebagai tabel -- tidak ada view lagi)
                ├── deteksi_dini_hipertensi.sql        │
                ├── tren_tensi_harian.sql              │
                └── weight_faltering_per_faskes.sql    ┘
```

> Folder `models/marts/` masih ada secara fisik (cuma berisi `schema.yml` kosong/placeholder) karena tool yang dipakai membangun project ini tidak punya kapabilitas hapus file — aman diabaikan, tidak dipakai dbt.

Mapping tiap model ke chart Superset ada di komentar `-- Chart: "..."` pada masing-masing file `.sql`, dan di dokumentasi dashboard (`~/project/ildki/dashboard/superset/dashboard_documentation.md` — catatan: dokumen itu masih pakai nama lama `stg_*`/`mart_*`, perlu di-update menyesuaikan).

## Prasyarat

```bash
pip install prefect prefect-dbt[cli] dbt-clickhouse
```

Docker image `fathur15/dbt:latest` yang dipakai di `prefect.yaml` **sudah** ter-install `dbt-clickhouse`, tidak perlu rebuild image.

**Database `silver` harus sudah ada di ClickHouse sebelum dbt dijalankan** (sudah ada saat ini), dan user yang dipakai (`CLICKHOUSE_USER`) harus punya privilege `CREATE TABLE`/`CREATE VIEW`/`DROP`/`SELECT`/`INSERT`/`ALTER` di database tersebut (privilege `CREATE TABLE`/`INSERT`/`ALTER` baru dibutuhkan sekarang karena model ekstraksi berubah dari `view` ke `table`/`incremental`):
```sql
CREATE DATABASE IF NOT EXISTS silver;
GRANT CREATE TABLE, CREATE VIEW, DROP, SELECT, INSERT, ALTER ON silver.* TO peerdb;
```

## Konfigurasi

1. Copy `.env-example` jadi `.env`, isi kredensial ClickHouse Anda.
2. Untuk deployment K8s lewat Prefect, isi block secret `clickhouse-host`, `clickhouse-user`, `clickhouse-password` (dirujuk di `prefect.yaml`).

## Cara Menjalankan (lokal)

```bash
cd dbt
dbt debug         # cek koneksi
dbt build         # build + test semua model (staging/fhir), urutan otomatis mengikuti ref()
```

Karena semua model sekarang di satu folder `staging/fhir/`, tidak perlu lagi `--select staging` vs `--select marts` terpisah — `dbt build` saja sudah membangun urutan yang benar (model `incremental`/`table` dulu, baru `view` yang bergantung padanya), berkat dependency graph dari `ref()`.

Schedule sudah aktif di `prefect.yaml` (`cron: "* * * * *"`, tiap 1 menit). **Catatan performa:** dengan 7 model `incremental` ini, run tiap 1 menit seharusnya ringan (cuma proses delta) -- beda dengan kalau semuanya masih `table` (full rebuild tiap menit, berat begitu data besar). Kalau ternyata masih terasa berat, cek dulu apakah watermark (`res_updated`) benar-benar mengecilkan volume yang diproses tiap run (lihat log `dbt run` -- jumlah baris yang di-insert per run seharusnya kecil di luar run pertama).

## Cara Menjalankan (lewat Prefect, sama seperti project StarRocks)

```python
from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation

@flow
def run_dbt_transformation():
    result = DbtCoreOperation(
        commands=["dbt build"],
        project_dir="dbt",
        profiles_dir="dbt"
    ).run()
    return result
```

## Catatan Penting / TODO

- **Database `silver` sudah dikosongkan total** (semua `stg_*`/`mart_*` versi lama sudah di-`DROP` manual). `dbt run`/`dbt build` berikutnya akan mulai dari kondisi bersih -- run pertama otomatis full build untuk semua model (termasuk yang `incremental`, karena belum ada watermark pembanding). **Update dataset Superset** ke nama tabel baru tanpa prefix (mis. `silver.top_diagnosis`) begitu model-model ini selesai ter-build.
- **Belum divalidasi eksekusi end-to-end setelah restrukturisasi + incremental ini** (`dbt run`/`dbt build` belum sempat dijalankan langsung). Wajib jalankan `dbt debug` lalu `dbt build` dulu sebagai validasi sebelum dipakai produksi -- terutama untuk memastikan config `engine`/`order_by`/`is_incremental()` diterima dbt-clickhouse tanpa error, dan privilege `INSERT`/`ALTER` sudah di-grant (lihat "Prasyarat").
- Ambang klasifikasi di `deteksi_dini_hipertensi.sql` (>=140/90 dan >=130/85) berdasarkan **satu kali pengukuran** — bukan pengganti penegakan diagnosis klinis.
- `dbt_runner.py` sengaja **tidak** nge-log `CLICKHOUSE_PASSWORD` ke output (beda dari versi StarRocks lama yang nge-log `STARROCKS_PASS` — sebaiknya dihindari untuk keamanan).
- **Jangan pakai `res_deleted_at IS NULL` atau kolom `*_at`/`*_deleted` lain dengan asumsi Nullable** di model manapun -- cek dulu tipe kolomnya di ClickHouse (`DESCRIBE TABLE ...`), karena hasil CDC PeerDB dari Postgres sering mengubah kolom Nullable jadi non-Nullable dengan nilai default/epoch.
- **Kalau menambah model baru yang baca dari model `incremental`** (`resource`, `conditions`, `encounters`, `observations`, `careplans`, `organizations`, `res_link`): jangan lupa tambahkan `FINAL` setelah `{{ ref('nama_model') }}`, atau chart bisa menghitung baris duplikat yang belum ter-merge.
- **`patients.sql` masih `table` (bukan incremental) secara sengaja** -- lihat komentar di file itu untuk alasannya. Kalau ke depan Patient jadi bottleneck juga, perlu didesain ulang (mis. incremental berbasis union watermark Patient + Encounter, bukan cuma satu kolom).

## Arsitektur Multi-Aplikasi (Monorepo vs Multi-repo)

**Rekomendasi: monorepo** -- tetap satu project dbt ini, tambah subfolder baru per aplikasi (`models/staging/jaksimpus/`, `models/staging/{app_lain}/`, dst), lalu model gold nanti juga tinggal di project yang sama (mis. `models/gold/`), BUKAN repo terpisah.

**Alasan utama -- keterbatasan teknis dbt Core (bukan cuma soal preferensi):**
dbt Core (open-source, yang dipakai di sini lewat `dbt-clickhouse`) **tidak mendukung `ref()` lintas project**. Fitur cross-project reference dengan lineage penuh (`dbt Mesh`) itu eksklusif dbt Cloud (berbayar). Kalau staging per aplikasi dipisah jadi repo/project sendiri-sendiri, maka repo "gold" mau tidak mau harus baca tabel silver aplikasi lain lewat `source()` biasa (nunjuk nama tabel mentah), BUKAN `ref()`. Konsekuensinya:
- Lineage graph (`dbt docs generate`) jadi **terputus** antar repo -- gold tidak "tahu" dependensinya ke staging aplikasi lain secara resmi di DAG.
- Kalau ada perubahan skema di staging FHIR (mis. kolom di-rename), dbt TIDAK akan otomatis mendeteksi/mem-flag model gold yang bakal rusak -- beda dengan `ref()` dalam satu project yang langsung ketahuan lewat `dbt build`.
- Kehilangan kemudahan `dbt test`/freshness check yang terhubung otomatis antar layer.

Dengan monorepo, `models/gold/xxx.sql` bisa langsung `{{ ref('conditions') }}` (punya FHIR) dan `{{ ref('kunjungan_klinik') }}` (punya Jaksimpus, misalnya) dalam satu query yang sama -- dbt otomatis tahu urutan build-nya, dan `dbt build` sekali jalan cukup untuk seluruh pipeline dari bronze sampai gold.

**Kapan multi-repo baru masuk akal:** kalau nanti tiap aplikasi punya TIM terpisah yang butuh kontrol akses/rilis independen (bukan kasus Anda sekarang, masih satu pengelola) -- itu pun solusinya biasanya tetap monorepo tapi dengan folder-level `CODEOWNERS` di GitHub + `dbt run --select staging.fhir` / `staging.jaksimpus` untuk build scope terpisah per aplikasi, bukan repo terpisah beneran.

**Struktur yang disarankan ke depan:**
```text
models/
├── staging/
│   ├── fhir/          # sudah ada (project ini)
│   ├── jaksimpus/     # nanti
│   └── {app_lain}/    # nanti
└── gold/               # nanti, lintas-aplikasi, pakai ref() ke semua staging/* di atas
```
Selector `--select staging.fhir` (dari `dbt_runner.py`) akan tetap jalan normal walau ada folder aplikasi lain, karena dbt selector berbasis path folder.

## Dokumentasi Terkait
- Prefect-dbt Documentation
- [dbt-clickhouse Adapter Guide](https://github.com/ClickHouse/dbt-clickhouse)
- `~/project/ildki/dashboard/superset/dashboard_documentation.md` — dokumentasi tiap chart & query (perlu di-update nama modelnya, lihat catatan di atas)
