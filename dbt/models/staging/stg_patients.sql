{{ config(
    materialized='view', 
    alias='stg_patient'
) }}

WITH fhir_resources AS (
    SELECT
        res_id,
        fhir_id AS patient_id, 
        res_type,
        res_deleted_at
    FROM {{ source('fhirhapi_prod', 'hfj_resource') }}
    WHERE res_type = 'Patient' 
      AND res_deleted_at IS NULL
),

fhir_encounters AS (
    SELECT
        fhir_id AS encounter_id,
        res_type,
        res_deleted_at
        /* Catatan: Untuk menghubungkan Encounter ke Patient, 
           biasanya kita mengekstrak token/string referensi dari kolom naratif/clob (res_text/res_encoding)
           atau hfj_res_link. Di bawah ini disimulasikan menggunakan tabel hfj_res_link.
        */
    FROM {{ source('fhirhapi_prod', 'hfj_resource') }}
    WHERE res_type = 'Encounter'
      AND res_deleted_at IS NULL
),

resource_links AS (
    SELECT 
        src_resource_id, -- Ini akan mengarah ke Encounter ID
        target_resource_id -- Ini akan mengarah ke Patient ID
    FROM {{ source('fhirhapi_prod', 'hfj_res_link') }}
    WHERE src_path = 'Encounter.subject' -- Menandakan subjek dari encounter adalah Patient
)

SELECT 
    p.res_id,
    p.patient_id,
    e.encounter_id
FROM fhir_resources p
LEFT JOIN resource_links rl ON p.res_id = rl.target_resource_id
LEFT JOIN fhir_encounters e ON rl.src_resource_id = e.encounter_id