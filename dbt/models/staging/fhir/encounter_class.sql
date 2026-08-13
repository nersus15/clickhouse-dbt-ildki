{{ config(
    materialized='view',
    alias='encounter_class'
) }}

-- VIEW: sumbernya sudah tabel silver yang bersih.

-- Chart: "Distribusi Kelas Encounter"
-- encounters materialized='incremental' -- WAJIB FINAL.
SELECT
    class_code AS encounter_class,
    count(*) AS total
FROM {{ ref('encounters') }} FINAL
GROUP BY class_code
