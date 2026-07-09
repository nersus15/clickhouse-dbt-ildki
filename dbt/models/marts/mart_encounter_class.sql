{{ config(materialized='view', alias='mart_encounter_class') }}

-- Chart: "Distribusi Kelas Encounter"
SELECT
    class_code AS encounter_class,
    count(*) AS total
FROM {{ ref('stg_encounters') }}
GROUP BY class_code
