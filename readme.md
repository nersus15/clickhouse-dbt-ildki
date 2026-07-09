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

### Strategi materialisasi (Opsi C)

| Kategori model | Materialized | Alasan |
|---|---|---|
| **Ekstraksi 1:1 dari bronze**<br>`patients`, `resource`, `organizations`, `encounters`, `conditions`, `observations`, `careplans`, `res_link` | `table` (engine `MergeTree()`) | Dedup CDC (`FINAL`) + ekstraksi JSON (`JSON_VALUE`) itu mahal secara komputasi. Kalau `view`, biaya ini dihitung ULANG setiap kali chart Superset dibuka. Kalau `table`, biaya ini cuma dihitung SEKALI per `dbt run`, lalu dashboard baca tabel yang sudah jadi (cepat). |
| **Agregasi/join untuk chart**<br>`kpi_totals`, `encounter_class`, `resource_daily_growth`, `top_diagnosis`, `penyakit_prioritas_trend`, `utilisasi_faskes`, `encounter_trend_per_faskes`, `deteksi_dini_hipertensi`, `tren_tensi_harian`, `weight_faltering_per_faskes` | `view` | Sumbernya sudah tabel bersih (bukan bronze mentah), jadi query-nya jauh lebih ringan meski dihitung ulang tiap kali diakses. Selalu real-time mengikuti isi tabel silver di atasnya. |

### Kapan view/table ini ter-update?

- **View** (`kpi_totals` dkk): otomatis real-time, karena ClickHouse jalankan ulang query-nya tiap kali di-`SELECT` — tidak perlu `dbt run` untuk soal kesegaran data.
- **Table** (`resource`, `conditions` dkk): **HANYA ter-update kalau `dbt run` dijalankan lagi** (full rebuild tabel dari bronze terkini). Karena datanya sekarang disimpan (bukan cuma query tersimpan seperti view), ini yang perlu **dijadwalkan** (lewat `prefect.yaml`, cron) supaya dashboard tidak baca data basi. Aman dijalankan berulang kali (`CREATE OR REPLACE TABLE` dampaknya idempotent).

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
                ├── resource.sql         │ materialized: table
                ├── organizations.sql    │ (ekstraksi 1:1 dari bronze,
                ├── encounters.sql       │  dedup + JSON_VALUE dihitung
                ├── conditions.sql       │  sekali saat dbt run)
                ├── observations.sql     │
                ├── careplans.sql        │
                ├── res_link.sql         ┘
                │
                ├── kpi_totals.sql                    ┐
                ├── encounter_class.sql                │
                ├── resource_daily_growth.sql          │
                ├── top_diagnosis.sql                  │ materialized: view
                ├── penyakit_prioritas_trend.sql       │ (agregasi/join, 1 model = 1 chart
                ├── utilisasi_faskes.sql               │  Superset, baca dari table di atas)
                ├── encounter_trend_per_faskes.sql     │
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

**Database `silver` harus sudah ada di ClickHouse sebelum dbt dijalankan** (sudah ada saat ini), dan user yang dipakai (`CLICKHOUSE_USER`) harus punya privilege `CREATE TABLE`/`CREATE VIEW`/`DROP`/`SELECT` di database tersebut (privilege `CREATE TABLE` ini baru dibutuhkan sekarang karena model ekstraksi berubah dari `view` ke `table`):
```sql
CREATE DATABASE IF NOT EXISTS silver;
GRANT CREATE TABLE, CREATE VIEW, DROP, SELECT ON silver.* TO peerdb;
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

Karena semua model sekarang di satu folder `staging/fhir/`, tidak perlu lagi `--select staging` vs `--select marts` terpisah — `dbt build` saja sudah membangun urutan yang benar (model `table` dulu, baru `view` yang bergantung padanya), berkat dependency graph dari `ref()`.

Kalau mau jadwal otomatis (supaya tabel `table` selalu segar), aktifkan `schedule:` di `prefect.yaml` (saat ini masih di-comment):
```yaml
schedule:
  cron: "*/15 * * * *"   # contoh: tiap 15 menit, sesuaikan kebutuhan
  timezone: "Asia/Jakarta"
```

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

- **View/tabel lama dengan nama `stg_*`/`mart_*` masih ada di database `silver`** (peninggalan sebelum restrukturisasi penamaan ini). Setelah `dbt run`/`dbt build` berhasil membuat semua object dengan nama baru (tanpa prefix), hapus manual yang lama di ClickHouse:
  ```sql
  DROP VIEW  IF EXISTS silver.stg_patient;  -- nama lama yang sempat salah alias, sebelum jadi stg_patients
  DROP TABLE IF EXISTS silver.stg_resource;
  DROP TABLE IF EXISTS silver.stg_organizations;
  DROP TABLE IF EXISTS silver.stg_encounters;
  DROP TABLE IF EXISTS silver.stg_conditions;
  DROP TABLE IF EXISTS silver.stg_observations;
  DROP TABLE IF EXISTS silver.stg_careplans;
  DROP TABLE IF EXISTS silver.stg_res_link;
  DROP TABLE IF EXISTS silver.stg_patients;
  DROP VIEW  IF EXISTS silver.mart_kpi_totals;
  DROP VIEW  IF EXISTS silver.mart_encounter_class;
  DROP VIEW  IF EXISTS silver.mart_resource_daily_growth;
  DROP VIEW  IF EXISTS silver.mart_top_diagnosis;
  DROP VIEW  IF EXISTS silver.mart_penyakit_prioritas_trend;
  DROP VIEW  IF EXISTS silver.mart_utilisasi_faskes;
  DROP VIEW  IF EXISTS silver.mart_encounter_trend_per_faskes;
  DROP VIEW  IF EXISTS silver.mart_deteksi_dini_hipertensi;
  DROP VIEW  IF EXISTS silver.mart_tren_tensi_harian;
  DROP VIEW  IF EXISTS silver.mart_weight_faltering_per_faskes;
  ```
  Lalu **update semua dataset Superset** yang tadinya menunjuk ke `silver.stg_*`/`silver.mart_*` supaya menunjuk ke nama baru tanpa prefix (mis. `silver.top_diagnosis`).
- **Belum divalidasi eksekusi end-to-end setelah restrukturisasi ini** (`dbt run`/`dbt build` belum sempat dijalankan langsung). Wajib jalankan `dbt debug` lalu `dbt build` dulu sebagai validasi sebelum dipakai produksi -- terutama untuk memastikan config `engine`/`order_by` pada model `table` diterima dbt-clickhouse tanpa error.
- Ambang klasifikasi di `deteksi_dini_hipertensi.sql` (>=140/90 dan >=130/85) berdasarkan **satu kali pengukuran** — bukan pengganti penegakan diagnosis klinis.
- `dbt_runner.py` sengaja **tidak** nge-log `CLICKHOUSE_PASSWORD` ke output (beda dari versi StarRocks lama yang nge-log `STARROCKS_PASS` — sebaiknya dihindari untuk keamanan).
- **Jangan pakai `res_deleted_at IS NULL` atau kolom `*_at`/`*_deleted` lain dengan asumsi Nullable** di model manapun -- cek dulu tipe kolomnya di ClickHouse (`DESCRIBE TABLE ...`), karena hasil CDC PeerDB dari Postgres sering mengubah kolom Nullable jadi non-Nullable dengan nilai default/epoch.
- **Kalau nanti butuh lebih ringan lagi** (skala data sudah besar): pertimbangkan `materialized='incremental'` untuk model ekstraksi (`resource`, `conditions`, dst) dengan `unique_key` yang sesuai, supaya `dbt run` cuma memproses baris baru/berubah, bukan rebuild total tabel tiap kali. Belum diterapkan sekarang untuk menjaga kesederhanaan & kebenaran data (full rebuild = selalu konsisten, tidak ada risiko logic incremental yang keliru).

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
