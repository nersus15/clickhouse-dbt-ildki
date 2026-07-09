{{ config(
    materialized='table',
    alias='encounter_class',
    engine='MergeTree()',
    order_by=['encounter_class']
) }}

-- Chart: "Distribusi Kelas Encounter"
-- encounters materialized='incremental' -- WAJIB FINAL.
SELECT
    class_code AS encounter_class,
    count(*) AS total
FROM {{ ref('encounters') }} FINAL
GROUP BY class_code
