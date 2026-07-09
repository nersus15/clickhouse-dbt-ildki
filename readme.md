# ILDKI Data Pipeline: Prefect + dbt + ClickHouse

Versi ClickHouse dari [`prefect-dbt-ildki`](../prefect-dbt-ildki) (yang aslinya untuk StarRocks). Project ini mengimplementasikan layer **silver** dari arsitektur **bronze -> silver -> gold** untuk data HAPI FHIR (SATUSEHAT), dengan **dbt** sebagai lapisan transformasi dan **Prefect** sebagai orchestrator, di atas **ClickHouse** sebagai data warehouse.

## Arsitektur Data

| Layer | Lokasi | Isi |
|---|---|---|
| **Bronze** | Database `fhirhapi_prod` | Raw hasil CDC PeerDB dari PostgreSQL (tabel `public_hfj_*`, skema internal HAPI FHIR JPA) |
| **Silver** | Database `silver` (project ini) | View hasil ekstraksi & pembersihan (`staging/`) + view hasil agregasi/join siap pakai chart (`marts/`) |
| **Gold** | *(belum dibuat)* | Rencana lanjutan, belum diimplementasikan |

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
| Model marts | `engine='OLAP'`, `primary_key`, `distributed_by` | Materialized `view` (lihat catatan di `dbt_project.yml`) |
| Env var koneksi | `STARROCKS_HOST/PORT/USER/PASS/DB` | `CLICKHOUSE_HOST/PORT/USER/PASSWORD/DATABASE` |

## Struktur Proyek

```text
├── prefect.yaml           # Konfigurasi Deployment Prefect (K8s job, env var ClickHouse)
├── dbt_runner.py           # Entrypoint Python (General Runner - JANGAN DIUBAH)
└── dbt/
    ├── dbt_project.yml     # Nama project, folder model, materialization default
    ├── profiles.yml        # Konfigurasi koneksi ClickHouse (type: clickhouse)
    └── models/
        ├── staging/         # Layer 1: ekstraksi 1:1 dari bronze (fhirhapi_prod.public_hfj_*)
        │   ├── schema.yml     # Definisi source (tabel bronze) + deskripsi tiap model staging
        │   ├── stg_resource.sql
        │   ├── stg_organizations.sql
        │   ├── stg_encounters.sql
        │   ├── stg_conditions.sql
        │   ├── stg_observations.sql
        │   ├── stg_careplans.sql
        │   └── stg_res_link.sql
        └── marts/           # Layer 2: agregasi/join, 1 model = 1 chart di dashboard Superset
            ├── schema.yml
            ├── mart_kpi_totals.sql
            ├── mart_encounter_class.sql
            ├── mart_resource_daily_growth.sql
            ├── mart_top_diagnosis.sql
            ├── mart_penyakit_prioritas_trend.sql
            ├── mart_utilisasi_faskes.sql
            ├── mart_encounter_trend_per_faskes.sql
            ├── mart_deteksi_dini_hipertensi.sql
            ├── mart_tren_tensi_harian.sql
            └── mart_weight_faltering_per_faskes.sql
```

Mapping tiap model `marts/` ke chart Superset ada di `dbt/models/marts/schema.yml` dan di dokumentasi dashboard (`~/project/ildki/dashboard/superset/dashboard_documentation.md`).

## Prasyarat

```bash
pip install prefect prefect-dbt[cli] dbt-clickhouse
```

Docker image `fathur15/dbt:latest` yang dipakai di `prefect.yaml` **sudah** ter-install `dbt-clickhouse`, tidak perlu rebuild image.

**Database `silver` harus sudah ada di ClickHouse sebelum dbt dijalankan**, dan user yang dipakai (`CLICKHOUSE_USER`) harus punya privilege `CREATE VIEW`/`DROP`/`SELECT` di database tersebut:
```sql
CREATE DATABASE IF NOT EXISTS silver;
GRANT CREATE VIEW, DROP, SELECT ON silver.* TO <user>;
```

## Konfigurasi

1. Copy `.env-example` jadi `.env`, isi kredensial ClickHouse Anda.
2. Untuk deployment K8s lewat Prefect, isi block secret `clickhouse-host`, `clickhouse-user`, `clickhouse-password` (dirujuk di `prefect.yaml`).

## Cara Menjalankan (lokal)

```bash
cd dbt
dbt debug                  # cek koneksi
dbt run --select staging   # build layer staging dulu
dbt run --select marts     # build layer marts (butuh staging sudah ada)
# atau sekaligus + test:
dbt build
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

- **Belum divalidasi eksekusi end-to-end** (`dbt run`/`dbt build` belum sempat dijalankan langsung saat project ini dibuat karena koneksi ClickHouse sedang tidak stabil). Wajib jalankan `dbt debug` lalu `dbt run --select staging` dulu sebagai validasi awal sebelum dipakai produksi.
- Model `marts/` semuanya `materialized: view` dulu (paling aman, tanpa config `engine`/`order_by`). Kalau nanti volume data besar dan butuh performa lebih cepat, pertimbangkan `materialized: table` atau `incremental` dengan `engine: 'ReplacingMergeTree()'` + `order_by` yang sesuai.
- Ambang klasifikasi di `mart_deteksi_dini_hipertensi.sql` (>=140/90 dan >=130/85) berdasarkan **satu kali pengukuran** — bukan pengganti penegakan diagnosis klinis. Lihat catatan lengkap di dashboard documentation.
- `dbt_runner.py` sengaja **tidak** nge-log `CLICKHOUSE_PASSWORD` ke output (beda dari versi StarRocks lama yang nge-log `STARROCKS_PASS` — sebaiknya dihindari untuk keamanan).

## Dokumentasi Terkait
- Prefect-dbt Documentation
- [dbt-clickhouse Adapter Guide](https://github.com/ClickHouse/dbt-clickhouse)
- `~/project/ildki/dashboard/superset/dashboard_documentation.md` — dokumentasi tiap chart & query aslinya (versi StarRocks, jadi rujukan logika bisnis)
