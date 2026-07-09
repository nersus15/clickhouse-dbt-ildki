{{ config(materialized='view', alias='encounter_class') }}

-- Chart: "Distribusi Kelas Encounter"
SELECT
    class_code AS encounter_class,
    count(*) AS total
FROM {{ ref('encounters') }}
GROUP BY class_code
