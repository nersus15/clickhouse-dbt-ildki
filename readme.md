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
| **Ekstraksi**<br>`resource`, `res_link`, `organizations`, `patients`, `encounters`, `conditions`, `observations`, `careplans` | `incremental` (append + `ReplacingMergeTree(_synced_at, is_deleted)`, kunci dedup = PK saja; `resource`: tanpa `is_deleted` karena justru menghitung resource terhapus) | Cuma proses baris yang **tiba** sejak run terakhir (watermark `_peerdb_synced_at` + lookback 15 menit, lihat `macros/cdc_incremental.sql`). Resource yang dihapus ikut ditulis dengan `is_deleted=1` supaya `FINAL` membuang versi aktif lamanya. |
| **Agregasi/join untuk chart**<br>`kpi_totals`, `encounter_class`, `resource_daily_growth`, `top_diagnosis`, `penyakit_prioritas_trend`, `utilisasi_faskes`, `encounter_trend_per_faskes`, `deteksi_dini_hipertensi`, `tren_tensi_harian`, `stunting_per_faskes`, `weight_faltering_per_faskes`, `pasien_belum_kunjungan`, `resource_type_summary`, `clinical_status_summary` | `view` | Sumbernya sudah tabel silver bersih, jadi dihitung saat chart dibuka -- selalu segar mengikuti tabel incremental, tanpa rebuild berkala. Kalau ada yang >3 detik, kandidat Refreshable MV. |

### Kenapa watermark `_peerdb_synced_at`, bukan `res_updated`?

`res_updated` adalah waktu menurut HAPI; CDC PeerDB tidak menjamin baris tiba urut waktu itu (transaksi Postgres commit tidak urut, dan `hfj_res_ver` bisa tiba setelah `hfj_resource`-nya). Dengan watermark `res_updated`, baris yang telat tiba hilang permanen (audit 2026-11-10: 318 Observation). `_peerdb_synced_at` = waktu baris tiba di ClickHouse, ditambah lookback 15 menit; baris yang terbaca dua kali di-dedup `ReplacingMergeTree`. Test `tests/assert_silver_lengkap.sql` memantau selisih bronze vs silver.

### Kenapa `incremental` + `ReplacingMergeTree`, bukan `delete+insert`?

dbt-clickhouse punya beberapa `incremental_strategy` (`append`, `delete+insert`, `insert_overwrite`). Project ini pakai pola **`append` + `ReplacingMergeTree`**, BUKAN `delete+insert`, karena:
- `delete+insert` di ClickHouse berjalan lewat **mutation** (`ALTER TABLE ... DELETE`) yang berat & async -- bukan `DELETE` OLTP biasa.
- `append` + `ReplacingMergeTree(_synced_at, is_deleted)` jauh lebih murah: baris baru/berubah cuma di-`INSERT`, dan ClickHouse yang urus dedup baris dengan PK sama di background (menang versi `_synced_at` terbesar, yaitu yang paling akhir tiba dari CDC -- termasuk baris hapus PeerDB yang mungkin cuma berisi PK).
- Ini **persis pola yang sudah dipercaya di layer bronze** (`_peerdb_version`, `FINAL`) -- konsisten, bukan teknik baru.

**Konsekuensi yang WAJIB diingat:** karena dedup `ReplacingMergeTree` itu *eventual* (baru benar-benar tergabung saat background merge selesai), **setiap query yang baca tabel `incremental` ini HARUS pakai `FINAL`** supaya tidak menghitung baris duplikat. Semua model `view` (agregasi) di project ini sudah disesuaikan (`{{ ref('conditions') }} FINAL`, dst) -- kalau Anda menambah model baru yang membaca tabel-tabel ini, jangan lupa tambahkan `FINAL` juga.

- Tabel `incremental` ter-update oleh deployment Prefect `ildki-dbt-clickhouse-incremental` (`cron: "*/5 * * * *"`, tiap 5 menit, `dbt run` 8 model ekstraksi). Model `view` otomatis ikut segar karena dihitung saat dibaca. Kalau job ini berhenti, seluruh dashboard ikut "beku" di data terakhir.
- **Run pertama** untuk model `incremental` otomatis full build (belum ada watermark pembanding).
- **Kalau logic SQL atau kolom suatu model `incremental` diubah**, histori lama TIDAK otomatis ke-reprocess -- wajib `dbt run --select <model> --full-refresh`. Model `view` cukup `dbt run --select <model>`.

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
    │   ├── cdc_incremental.sql        # Watermark _peerdb_synced_at, is_deleted, tanggal WIB (dipakai 8 model ekstraksi)
    │   └── generate_schema_name.sql   # Disiapkan untuk nanti kalau layer gold mulai dipakai (belum aktif)
    ├── tests/
    │   └── assert_silver_lengkap.sql  # Selisih bronze vs silver per res_type
    └── models/
        └── staging/
            └── fhir/                  # Semua model bersumber dari FHIR (HAPI/SATUSEHAT)
                ├── schema.yml           # Definisi source (tabel bronze) + deskripsi tiap model
                │
                ├── patients.sql         ┐
                ├── resource.sql         │ materialized: incremental
                ├── organizations.sql    │ (ReplacingMergeTree + watermark _peerdb_synced_at,
                ├── encounters.sql       │  lihat "Strategi materialisasi")
                ├── conditions.sql       │
                ├── observations.sql     │
                ├── careplans.sql        │
                ├── res_link.sql         ┘
                │
                ├── kpi_totals.sql                    ┐
                ├── encounter_class.sql                 │
                ├── resource_daily_growth.sql           │
                ├── top_diagnosis.sql                   │ materialized: view
                ├── penyakit_prioritas_trend.sql        │ (agregasi/join dari tabel silver,
                ├── utilisasi_faskes.sql                │  1 model = 1 chart Superset)
                ├── encounter_trend_per_faskes.sql      │
                ├── deteksi_dini_hipertensi.sql         │
                ├── tren_tensi_harian.sql               │
                ├── stunting_per_faskes.sql             │
                ├── pasien_belum_kunjungan.sql          │
                ├── weight_faltering_per_faskes.sql     │  monitoring kualitas data CarePlan, BUKAN stunting
                ├── resource_type_summary.sql           │  ARSIP -- tidak tampil di dashboard utama,
                └── clinical_status_summary.sql         ┘  cuma referensi teknis (lihat bagian Dashboard)
```

> Folder `models/marts/` masih ada secara fisik (cuma berisi `schema.yml` kosong/placeholder) karena tool yang dipakai membangun project ini tidak punya kapabilitas hapus file — aman diabaikan, tidak dipakai dbt.

### Status di dashboard Superset (per model agregasi)

| Model | Tampil di dashboard? |
|---|---|
| `kpi_totals`, `resource_daily_growth`, `top_diagnosis`, `penyakit_prioritas_trend`, `utilisasi_faskes`, `encounter_trend_per_faskes`, `deteksi_dini_hipertensi`, `tren_tensi_harian`, `stunting_per_faskes`, `pasien_belum_kunjungan` | ✅ Ya |
| `weight_faltering_per_faskes` | ⚠️ Turun dari "Risiko Stunting" jadi monitoring kualitas data -- CarePlan CP000001 terkirim untuk mayoritas pasien dewasa (audit 2026-11-10). Indikator stunting pakai `stunting_per_faskes`. |
| `encounter_class` | ❌ Sudah dikeluarkan dari dashboard (dianggap kurang relevan/tidak cocok) -- model & dataset Superset-nya tetap ada, cuma tidak ditampilkan |
| `resource_type_summary`, `clinical_status_summary` | ❌ Arsip/referensi teknis saja, tidak pernah ditampilkan di dashboard kebijakan utama (dari awal dianggap terlalu teknis untuk audiens pejabat/tenaga medis) |

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

Schedule aktif di `prefect.yaml` (`cron: "*/5 * * * *"`, tiap 5 menit). Kalau run terasa berat, cek jumlah baris yang di-insert per run di log `dbt run` -- di luar run pertama seharusnya kecil (delta sejak watermark `_peerdb_synced_at` dikurangi lookback 15 menit).

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

- **Sudah divalidasi eksekusi end-to-end** -- `dbt build --select staging+` berhasil membangun seluruh 17 model (8 ekstraksi + 9 chart aktif) ke database `silver`, dan seluruh chart di dashboard Superset sudah dialihkan ke dataset ClickHouse ini (bukan StarRocks lagi). Dua model tambahan (`resource_type_summary`, `clinical_status_summary`) untuk chart arsip juga sudah dibuatkan tabelnya secara manual di ClickHouse (lihat catatan berikutnya) -- akan otomatis ter-build ulang oleh dbt begitu file-nya di-commit & job Prefect jalan lagi.
- **`resource_type_summary` & `clinical_status_summary` sempat dibuat manual langsung di ClickHouse** (bukan lewat `dbt run`) supaya dataset Superset-nya bisa langsung dipakai tanpa nunggu commit+deploy. Begitu file dbt-nya (`resource_type_summary.sql`, `clinical_status_summary.sql`) sudah di-commit ke repo dan job Prefect jalan, dbt akan `CREATE OR REPLACE TABLE` ulang -- idempotent, aman, tidak perlu tindakan tambahan.
- Ambang klasifikasi di `deteksi_dini_hipertensi.sql` (>=140/90 dan >=130/85) untuk **dewasa**, berdasarkan **pemeriksaan terakhir** — bukan pengganti penegakan diagnosis klinis. Sistol (LOINC `8480-6`) dan diastol (`8462-4`) adalah Observation terpisah di SATUSEHAT.
- **Stunting** (`stunting_per_faskes`): kasus tegak = Condition ICD-10 E45, plus z-score PB/U/TB/U < -2 SD dari Observation begitu var `kode_zscore_tbu` di `dbt_project.yml` diisi (per 2026-11-11 belum ada faskes yang mengirim Observation z-score; kode resmi Playbook SATUSEHAT Gizi v3.2 perlu diverifikasi).
- `dbt_runner.py` sengaja **tidak** nge-log `CLICKHOUSE_PASSWORD` ke output (beda dari versi StarRocks lama yang nge-log `STARROCKS_PASS` — sebaiknya dihindari untuk keamanan).
- **Jangan pakai `res_deleted_at IS NULL` atau kolom `*_at`/`*_deleted` lain dengan asumsi Nullable** di model manapun -- cek dulu tipe kolomnya di ClickHouse (`DESCRIBE TABLE ...`), karena hasil CDC PeerDB dari Postgres sering mengubah kolom Nullable jadi non-Nullable dengan nilai default/epoch.
- **Kalau menambah model baru yang baca dari model `incremental`** (`resource`, `conditions`, `encounters`, `observations`, `careplans`, `organizations`, `res_link`, `patients`): jangan lupa tambahkan `FINAL` setelah `{{ ref('nama_model') }}`, atau chart bisa menghitung baris duplikat yang belum ter-merge.
- **Model ekstraksi baru** yang membaca bronze: pakai macro di `macros/cdc_incremental.sql` (`cdc_changed_res_ids`, `cdc_is_deleted`, `cdc_synced_at`), jangan watermark `res_updated` -- lihat "Kenapa watermark `_peerdb_synced_at`".

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
