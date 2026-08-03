SELECT
    job_id
    , title
FROM {{ ref('int_wremotely__publishable_job_facts') }}
WHERE NULLIF(TRIM(title), '') IS NULL
    OR CHAR_LENGTH(TRIM(title)) > 500
