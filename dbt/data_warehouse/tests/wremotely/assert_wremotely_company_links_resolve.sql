SELECT
    j.job_id
    , j.company_id
FROM {{ ref('wremotely__serving_jobs') }} AS j
LEFT JOIN {{ ref('wremotely__companies') }} AS c
    ON j.company_id = c.company_id
WHERE j.company_id IS NOT NULL
    AND c.company_id IS NULL
