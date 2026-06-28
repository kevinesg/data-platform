SELECT
    job_id
    , company_id
    , company_name
    , source_domain
FROM {{ ref('wremotely__serving_jobs') }}
WHERE company_id IS NOT NULL
    AND (
        NULLIF(TRIM(company_name), '') IS NULL
        OR NULLIF(TRIM(source_domain), '') IS NULL
    )
