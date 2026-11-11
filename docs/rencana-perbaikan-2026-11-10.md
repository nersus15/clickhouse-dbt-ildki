# Rencana Perbaikan Model dbt (audit 2026-11-10)

Status: **KODE SELESAI & TERVALIDASI 2026-11-11, BELUM DI-DEPLOY** (lihat "Urutan rilis").

Hasil Fase 0 (2026-11-11): TIDAK ada Observation z-score PB/U/TB/U di data. Stunting tegak
hanya tersedia sebagai Condition ICD-10 E45 (76 kondisi, semua pasien anak: TB rata-rata
75–92 cm) → `stunting_per_faskes` memakai E45; cabang z-score siap tapi dimatikan lewat var
`kode_zscore_tbu: []`. Baris delete PeerDB berisi row lengkap (bukan PK saja), tapi kode
tetap defensif untuk keduanya (versi `_synced_at`, kunci dedup = PK).

## Temuan (sudah diverifikasi ke data ClickHouse 2026-11-10)

| # | Model | Temuan | Bukti |
|---|---|---|---|
| 1 | `weight_faltering_per_faskes` | Semua CarePlan dihitung "kasus stunting" tanpa filter; count CarePlan bukan anak; tanggal dari `res_published` | 30.029 CarePlan semua `CP000001`; 27.634 pasien (63% semua pasien); 81% pasiennya BB ≥40 kg & TB ≥120 cm (dewasa) |
| 2 | `deteksi_dini_hipertensi`, `tren_tensi_harian` | Filter `loinc_code = '8480-6'` (sistol saja) → diastol (`8462-4`) tidak pernah terbaca | `rata_diastol` selalu NULL; 45.406/45.549 pembacaan tanpa diastol |
| 3 | `deteksi_dini_hipertensi` | Alias shadowing: `WHERE kategori_tensi IS NOT NULL` merujuk alias `assumeNotNull(...)` → NULL lolos jadi `''`; pasien multi-kunjungan masuk >1 kategori | 855 pasien kategori `""`; total 44.181 > jumlah pasien |
| 4 | Semua incremental | Watermark `res_updated > max()` melewatkan baris CDC yang telat (mis. `hfj_res_ver` tiba setelah `hfj_resource`) — hilang permanen | 318 Observation aktif di bawah watermark tidak ada di silver |
| 5 | Semua incremental + `res_link` | Baris deleted dibuang di query incremental → delete/update tidak pernah sampai silver; link lama `res_link` menumpuk | Saat ini 0 resource deleted, tapi `res_link` sudah terdampak |
| 6 | `conditions` | `LIKE 'E1%'` ikut menangkap E15/E16 (hipoglikemia, bukan DM) | E16.1, E16.2 masuk "Diabetes Melitus" |
| 7 | `kpi_totals` | `kasus_*` = jumlah Condition, bukan pasien | Hipertensi 7.454 kondisi vs 7.316 pasien |
| 8 | `encounters`, `conditions`, `careplans` | Tanggal dari `res_published` (waktu ingest), bukan tanggal klinis | 3.749 Encounter & 3.253 CarePlan beda hari, maks lag 66 hari |
| 9 | Semua `toDate()` | UTC, bukan WIB | — |
| 10 | `prefect.yaml` | `*/1 * * * *` identik dengan `* * * * *` — fix OOM tidak berlaku | — |
| 11 | `readme.md`, `dbt_project.yml` | Dokumentasi tidak sinkron (agregasi tertulis `table`, di kode `view`; patients) | — |

Catatan data: hanya 952 dari 44.020 Patient yang punya `birthDate` (sisanya stub IHS/NIK) → filter usia hampir tidak mungkin.

## Fase & urutan

| Fase | Isi | Temuan | Full-refresh? |
|---|---|---|---|
| 0 | Verifikasi kode Observation stunting & kolom `_peerdb_synced_at` | — | — |
| 1 | Fondasi incremental: data hilang & delete | #4, #5 | ✅ semua ekstraksi |
| 2 | Tanggal klinis & timezone WIB | #8, #9 | ✅ (gabung Fase 1) |
| 3 | Hipertensi | #2, #3 | ❌ (view) |
| 4 | PTM & KPI | #6, #7 | ✅ `conditions` |
| 5 | Stunting berbasis Observation TB/U | #1 | ✅ `observations`, `careplans` |
| 6 | Operasional, tes, dokumentasi | #10, #11 | — |

Fase 1, 2, 4, 5 (bagian kolom) digabung satu rilis → full-refresh cukup sekali.

---

### Fase 0 — Verifikasi (butuh port-forward ClickHouse 8123 hidup; 2026-11-10 hang)

**0a. Kode Observation stunting.** Playbook SATUSEHAT Gizi: stunting = Observation z-score PB/U (0–24 bln) / TB/U (24–60 bln), `valueQuantity` = z, `interpretation.coding` = status, system `http://terminology.kemkes.go.id/CodeSystem/clinical-term`. Kode TB/U **diubah di Playbook v3.2 (8 Mei 2024)** — kandidat `OC000006`/`OC000007` BELUM terverifikasi, cek langsung dokumen + Lampiran 1 (kode interpretasi).

Sinyal awal: Observation non-LOINC cuma `431855005` & `OC000135`; pencarian teks "stunt/gizi/pendek/z-score" kosong → kemungkinan z-score TB/U belum dikirim faskes. Konfirmasi:

```sql
SELECT JSON_VALUE(v.res_text_vc,'$.code.coding[0].system') sys,
       JSON_VALUE(v.res_text_vc,'$.code.coding[0].code') code,
       any(JSON_VALUE(v.res_text_vc,'$.code.coding[0].display')) disp, count()
FROM fhirhapi_prod.public_hfj_resource r FINAL
JOIN fhirhapi_prod.public_hfj_res_ver v FINAL ON r.res_id=v.res_id AND r.res_ver=v.res_ver
WHERE r.res_type='Observation' AND r._peerdb_is_deleted=0
  AND (positionCaseInsensitive(JSON_VALUE(v.res_text_vc,'$.code.coding[0].display'),'age') > 0
       OR JSON_VALUE(v.res_text_vc,'$.code.coding[0].code') LIKE 'OC0000%')
GROUP BY 1,2
```

Juga cek Condition ICD-10 `R62.%`, `E45` sebagai pembanding.

**0b.** `DESCRIBE fhirhapi_prod.public_hfj_resource` / `public_hfj_res_ver` / `public_hfj_res_link` — pastikan ada `_peerdb_synced_at`.

### Fase 1 — Fondasi incremental

Model: `resource`, `encounters`, `conditions`, `observations`, `careplans`, `organizations`, `patients`.

1. Watermark pakai waktu sinkron CDC + lookback, lewat macro `macros/incremental_filter.sql`:
   ```sql
   greatest(r._peerdb_synced_at, v._peerdb_synced_at) AS _synced_at,
   ...
   {% if is_incremental() %}
     AND (r._peerdb_synced_at > (SELECT max(_synced_at) - INTERVAL 15 MINUTE FROM {{ this }})
       OR v._peerdb_synced_at > (SELECT max(_synced_at) - INTERVAL 15 MINUTE FROM {{ this }}))
   {% endif %}
   ```
2. Tulis baris deleted dengan flag, engine `ReplacingMergeTree(res_updated, is_deleted)` → `FINAL` otomatis buang yang dihapus, model hilir tidak perlu diubah:
   ```sql
   toUInt8(r.res_deleted_at != toDateTime64('1970-01-01 00:00:00', 6)
           OR r._peerdb_is_deleted = 1) AS is_deleted
   ```
   Hapus filter `res_deleted_at = epoch` & `_peerdb_is_deleted = 0` di WHERE; JOIN `hfj_res_ver` → `LEFT JOIN`.
3. `res_link`: `ReplacingMergeTree(sp_updated, is_deleted)`, `is_deleted = l._peerdb_is_deleted`, `order_by=['source_resource_type','src_path','src_resource_id','pid']` (HAPI hapus+insert link per pid saat update), watermark `_peerdb_synced_at`.

Validasi: count silver vs bronze aktif per res_type ≈ 0 selisih.

### Fase 2 — Tanggal klinis & WIB

| Model | Kolom (sumber FHIR) | Fallback |
|---|---|---|
| `encounters` | `tanggal_kunjungan` ← `period.start` | `res_published` |
| `conditions` | `tanggal_diagnosis` ← `onsetDateTime` → `recordedDate` | `res_published` |
| `careplans` | `tanggal` ← `created` | `res_published` |
| `observations` | `tanggal_periksa` (sudah `effectiveDateTime`) | tetap |

```sql
toDate(coalesce(parseDateTimeBestEffortOrNull(JSON_VALUE(v.res_text_vc,'$.period.start')),
                r.res_published), 'Asia/Jakarta')
```

Nama kolom dipertahankan (dataset Superset aman). `res_published` tetap ada untuk `resource_daily_growth`.

### Fase 3 — Hipertensi

`deteksi_dini_hipertensi.sql`:

```sql
WITH bp AS (   -- pasangkan sistol+diastol per encounter
    SELECT patient_ref, encounter_ref,
           max(tanggal_periksa) AS tanggal_periksa,
           maxIf(nilai, loinc_code = '8480-6' AND nilai BETWEEN 50 AND 300) AS sistol,
           maxIf(nilai, loinc_code = '8462-4' AND nilai BETWEEN 30 AND 200) AS diastol
    FROM {{ ref('observations') }} FINAL
    WHERE loinc_code IN ('8480-6', '8462-4')
    GROUP BY patient_ref, encounter_ref
),
terakhir AS (  -- 1 pasien = 1 kategori (pemeriksaan terakhir)
    SELECT patient_ref,
           argMax(sistol, tanggal_periksa) AS s,
           argMax(diastol, tanggal_periksa) AS d
    FROM bp WHERE sistol > 0 OR diastol > 0
    GROUP BY patient_ref
),
klasifikasi AS (
    SELECT patient_ref,
           multiIf(s >= 140 OR d >= 90, 'Hipertensi (>=140/90)',
                   s >= 130 OR d >= 85, 'Waspada/Pra-Hipertensi (>=130/85)',
                   'Normal') AS kategori_raw   -- beda nama dari alias akhir (hindari shadowing)
    FROM terakhir
)
SELECT kategori_raw AS kategori_tensi, <status_diagnosis>, count() AS jumlah_pasien
FROM klasifikasi GROUP BY ...
```

`tren_tensi_harian.sql`:

```sql
avgIf(nilai, loinc_code = '8480-6' AND nilai BETWEEN 50 AND 300) AS rata_sistol,
avgIf(nilai, loinc_code = '8462-4' AND nilai BETWEEN 30 AND 200) AS rata_diastol
... WHERE loinc_code IN ('8480-6','8462-4')
```

Catatan: ~143 Observation kode `8480-6` tapi display diastolik → laporkan ke pengirim. Label chart: "ambang dewasa". Hapus komentar "sudah divalidasi" yang keliru.

### Fase 4 — PTM & KPI

- `conditions.sql`:
  ```sql
  multiIf(match(code, '^I1[0-5]'), 'Hipertensi',
          match(code, '^E1[0-4]'), 'Diabetes Melitus', 'Lainnya') AS kategori_ptm
  ```
- `kpi_totals.sql`: tambah `pasien_hipertensi`, `pasien_diabetes` (`uniqExact(patient_ref)`); `kasus_*` dipertahankan untuk kompatibilitas.
- `top_diagnosis`, `penyakit_prioritas_trend`: tambah kolom `jumlah_pasien` opsional.

### Fase 5 — Stunting (kasus tegak dari Observation)

Definisi (Permenkes 2/2020 Standar Antropometri Anak + Playbook SATUSEHAT Gizi): PB/U (0–24 bln) / TB/U (24–60 bln); **pendek** −3 ≤ z < −2; **sangat pendek** z < −3; buang |z| > 6 (flag WHO). Prioritaskan `interpretation.coding` bila tersedia, z sebagai fallback.

1. `observations.sql` tambah kolom:
   ```sql
   JSON_VALUE(v.res_text_vc, '$.code.coding[0].system')            AS code_system,
   JSON_VALUE(v.res_text_vc, '$.interpretation[0].coding[0].code') AS interpretasi_code,
   ```
2. Var di `dbt_project.yml`: `kode_zscore_tbu` (hasil Fase 0).
3. Model baru `stunting_per_faskes.sql` (view):
   ```sql
   WITH tbu AS (
       SELECT o.patient_ref, o.encounter_ref, o.tanggal_periksa, o.nilai AS z, o.interpretasi_code
       FROM {{ ref('observations') }} AS o FINAL
       WHERE o.code_system = 'http://terminology.kemkes.go.id/CodeSystem/clinical-term'
         AND o.loinc_code IN {{ var('kode_zscore_tbu') }}
         AND o.nilai BETWEEN -6 AND 6
   ),
   status_bulanan AS (   -- 1 anak = 1 status per bulan (pengukuran terakhir)
       SELECT toStartOfMonth(tanggal_periksa) AS bulan, patient_ref,
              argMax(z, tanggal_periksa)                 AS z,
              argMax(interpretasi_code, tanggal_periksa) AS interp,
              argMax(encounter_ref, tanggal_periksa)     AS encounter_ref
       FROM tbu GROUP BY bulan, patient_ref
   )
   SELECT s.bulan, org.nama_puskesmas,
          count()                     AS anak_diukur,   -- penyebut
          countIf(z < -2)             AS anak_stunting,
          countIf(z >= -3 AND z < -2) AS pendek,
          countIf(z < -3)             AS sangat_pendek
   FROM status_bulanan s
   JOIN <Observation.encounter -> Encounter.serviceProvider -> organizations via res_link>
   GROUP BY s.bulan, org.nama_puskesmas
   ```
   Superset: metric prevalensi = `SUM(anak_stunting) / SUM(anak_diukur)` (bukan rata-rata persentase).
4. `weight_faltering_per_faskes`: turunkan dari "Risiko Stunting" → rename tampilan "CarePlan Tatalaksana Weight Faltering" (arsip/monitoring kualitas data). Perbaiki: filter `kategori_code = 'CP000001'`, `uniqExact(patient_ref)`, tanggal dari `created`. Tambah `kategori_code` di `careplans.sql`.
5. Laporkan ke pengembang SIMPUS: CP000001 terkirim untuk ~81% pasien dewasa.
6. Jika Fase 0 membuktikan z-score TB/U belum dikirim: model tetap dibuat (kosong) + catatan di dashboard "menunggu data z-score TB/U dari faskes". Hitung z-score sendiri (WHO LMS) TIDAK layak — `birthDate` cuma ada di 952 Patient.

### Fase 6 — Operasional, tes, dokumentasi

- `prefect.yaml`: cron → `*/5 * * * *`.
- Tests: `tests/assert_silver_lengkap.sql` (selisih bronze vs silver < 0,1%); `not_null` ID; `accepted_values` `kategori_ptm`/`kategori_tensi`; z-score dalam ±6.
- Performa: ukur `deteksi_dini_hipertensi`, `tren_tensi_harian`, `stunting_per_faskes`; jika > 3 detik → Refreshable MV `REFRESH EVERY 10 MINUTE`. JANGAN ubah `order_by` observations ke `(loinc_code, observation_id)` (dedup ReplacingMergeTree per seluruh kunci → baris lama tak terhapus bila kode di-update).
- Sinkronkan `readme.md` & komentar `dbt_project.yml`.

## Urutan rilis

1. Satu PR: Fase 1 + 2 + 4 + 5 (kolom).
2. Pause deployment Prefect `ildki-dbt-clickhouse-incremental`.
3. `dbt run --full-refresh --select resource res_link organizations patients encounters conditions observations careplans`
4. `dbt run --select` semua view (Fase 3, 5) → `dbt test`.
5. Resume Prefect (cron `*/5`).
6. Superset: refresh schema dataset, chart stunting baru (metric prevalensi), ganti label KPI.

## Sumber

- [SATUSEHAT Playbook – Gizi](https://satusehat.kemkes.go.id/platform/docs/id/interoperability/gizi/)
- [Modul Penilaian Status Gizi WHO-2006 (Poltekkes Aceh)](https://gizipoltekkesaceh.ac.id/wp-content/uploads/2023/03/Modul_-Penilaian-Pertumbuhan-BALITA.pdf)
