{{ config(
    materialized='view',
    alias='stunting_per_faskes'
) }}

-- Chart: "Kasus Stunting per Puskesmas" -- menggantikan weight_faltering_per_faskes
-- sebagai indikator stunting. Satu baris = satu bulan x satu puskesmas; satu anak dihitung
-- SEKALI per bulan per puskesmas walau diperiksa berkali-kali.
--
-- Kasus stunting = anak dengan salah satu bukti TEGAK:
--   1. Diagnosis Condition ICD-10 E45 (inclusion term WHO: "nutritional stunting / short
--      stature") -- satu-satunya sumber yang ada di data per 2026-11-11.
--   2. Observation z-score PB/U (0-24 bln) / TB/U (24-60 bln) < -2 SD (Permenkes 2/2020
--      Standar Antropometri Anak; Playbook SATUSEHAT Gizi). Cabang ini MATI selama
--      var kode_zscore_tbu kosong (dbt_project.yml): per 2026-11-11 belum ada faskes yang
--      mengirim Observation z-score, dan kode resminya (diubah di Playbook v3.2) belum
--      terverifikasi. pendek = -3 <= z < -2, sangat_pendek = z < -3, |z| > 6 dibuang (flag WHO).
--
-- Prevalensi di Superset: SUM(anak_stunting_tbu) / SUM(anak_diukur_tbu) -- jangan rata-rata
-- persentase. Kasus dari diagnosis E45 tidak punya penyebut (anak yang tidak stunting tidak
-- di-coding apa-apa), jadi hanya bisa ditampilkan sebagai jumlah.
-- Faskes = Encounter.serviceProvider dari encounter tempat kasus tercatat.
-- conditions, observations, encounters, res_link, organizations incremental -- WAJIB FINAL.

{% set kode_tbu = var('kode_zscore_tbu', []) %}

WITH encounter_org AS (
    SELECT src_resource_id AS encounter_id, target_resource_id AS organization_id
    FROM {{ ref('res_link') }} FINAL
    WHERE source_resource_type = 'Encounter' AND src_path = 'Encounter.serviceProvider'
),

kejadian AS (
    SELECT
        e.tanggal_kunjungan AS tanggal,
        eo.organization_id,
        c.patient_ref,
        1 AS dari_diagnosis,
        CAST(NULL AS Nullable(Float64)) AS z
    FROM {{ ref('conditions') }} AS c FINAL
    JOIN {{ ref('res_link') }} AS l FINAL
        ON l.src_resource_id = c.condition_id
        AND l.source_resource_type = 'Condition' AND l.src_path = 'Condition.encounter'
    JOIN {{ ref('encounters') }} AS e FINAL ON e.encounter_id = l.target_resource_id
    JOIN encounter_org AS eo ON eo.encounter_id = e.encounter_id
    WHERE c.kategori_ptm = 'Stunting'

{% if kode_tbu %}
    UNION ALL

    SELECT
        o.tanggal_periksa AS tanggal,
        eo.organization_id,
        o.patient_ref,
        0 AS dari_diagnosis,
        CAST(o.nilai AS Nullable(Float64)) AS z
    FROM {{ ref('observations') }} AS o FINAL
    JOIN {{ ref('res_link') }} AS l FINAL
        ON l.src_resource_id = o.observation_id
        AND l.source_resource_type = 'Observation' AND l.src_path = 'Observation.encounter'
    JOIN encounter_org AS eo ON eo.encounter_id = l.target_resource_id
    WHERE o.code_system = 'http://terminology.kemkes.go.id/CodeSystem/clinical-term'
        AND o.loinc_code IN ('{{ kode_tbu | join("', '") }}')
        AND o.nilai BETWEEN -6 AND 6
        AND o.tanggal_periksa != '1970-01-01'
{% endif %}
),

per_anak AS (
    -- Status anak per bulan: diagnosis E45 kapan pun di bulan itu, atau z-score TERAKHIR.
    SELECT
        toStartOfMonth(tanggal) AS bulan,
        organization_id,
        patient_ref,
        max(dari_diagnosis) AS ada_diagnosis,
        countIf(z IS NOT NULL) > 0 AS diukur,
        argMaxIf(assumeNotNull(z), tanggal, z IS NOT NULL) AS z_terakhir
    FROM kejadian
    GROUP BY bulan, organization_id, patient_ref
)

SELECT
    p.bulan,
    o.nama_puskesmas,
    countIf(p.ada_diagnosis = 1 OR (p.diukur AND p.z_terakhir < -2)) AS anak_stunting,
    countIf(p.ada_diagnosis = 1) AS anak_stunting_diagnosis,
    countIf(p.diukur) AS anak_diukur_tbu,
    countIf(p.diukur AND p.z_terakhir < -2) AS anak_stunting_tbu,
    countIf(p.diukur AND p.z_terakhir >= -3 AND p.z_terakhir < -2) AS pendek,
    countIf(p.diukur AND p.z_terakhir < -3) AS sangat_pendek
FROM per_anak AS p
JOIN {{ ref('organizations') }} AS o FINAL ON o.organization_id = p.organization_id
GROUP BY p.bulan, o.nama_puskesmas
