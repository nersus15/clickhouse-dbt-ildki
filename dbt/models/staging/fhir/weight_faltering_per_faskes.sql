{{ config(
    materialized='view',
    alias='weight_faltering_per_faskes'
) }}

-- Chart: "CarePlan Tatalaksana Weight Faltering per Puskesmas" (MONITORING KUALITAS DATA)
-- BUKAN indikator stunting -- pakai stunting_per_faskes. Audit 2026-11-10: CarePlan
-- CP000001 terkirim untuk 63% semua pasien, 81% di antaranya BB >=40 kg & TB >=120 cm
-- (dewasa), padahal weight faltering cuma berlaku untuk balita. Model ini dipertahankan
-- untuk memantau perbaikan di aplikasi pengirim.
-- Alur relasi: CarePlan -> (CarePlan.encounter) -> Encounter -> (Encounter.serviceProvider) -> Organization
-- res_link, careplans, organizations materialized='incremental' -- WAJIB FINAL.

WITH careplan_encounter AS (
    SELECT src_resource_id AS careplan_id, target_resource_id AS encounter_id
    FROM {{ ref('res_link') }} FINAL
    WHERE source_resource_type = 'CarePlan' AND src_path = 'CarePlan.encounter'
),

encounter_org AS (
    SELECT src_resource_id AS encounter_id, target_resource_id AS organization_id
    FROM {{ ref('res_link') }} FINAL
    WHERE source_resource_type = 'Encounter' AND src_path = 'Encounter.serviceProvider'
)

SELECT
    cp.tanggal,
    o.nama_puskesmas,
    count() AS jumlah_careplan,
    uniqExact(cp.patient_ref) AS jumlah_pasien
FROM {{ ref('careplans') }} AS cp FINAL
JOIN careplan_encounter AS ce ON ce.careplan_id = cp.careplan_id
JOIN encounter_org AS eo ON eo.encounter_id = ce.encounter_id
JOIN {{ ref('organizations') }} AS o FINAL ON o.organization_id = eo.organization_id
-- CP000001 = "Tatalaksana weight faltering" (CodeSystem kemkes care-plan-category)
WHERE cp.kategori_code = 'CP000001'
GROUP BY cp.tanggal, o.nama_puskesmas
