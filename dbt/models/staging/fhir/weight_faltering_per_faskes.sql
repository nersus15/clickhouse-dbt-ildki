{{ config(
    materialized='table',
    alias='weight_faltering_per_faskes',
    engine='MergeTree()',
    order_by=['tanggal', 'nama_puskesmas']
) }}

-- Chart: "Tren Kasus Weight Faltering (Risiko Stunting) per Puskesmas"
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
    count(*) AS jumlah_kasus
FROM {{ ref('careplans') }} AS cp FINAL
JOIN careplan_encounter AS ce ON ce.careplan_id = cp.careplan_id
JOIN encounter_org AS eo ON eo.encounter_id = ce.encounter_id
JOIN {{ ref('organizations') }} AS o FINAL ON o.organization_id = eo.organization_id
GROUP BY cp.tanggal, o.nama_puskesmas
