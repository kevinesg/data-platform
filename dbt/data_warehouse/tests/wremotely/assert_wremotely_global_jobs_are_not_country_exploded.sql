SELECT
    j.job_id
    , b.country_code
    , b.eligibility_status
FROM {{ ref('wremotely__serving_jobs') }} AS j
INNER JOIN {{ ref('wremotely__job_country_eligibility') }} AS b
    ON j.job_id = b.job_id
WHERE j.country_eligibility_scope = 'GLOBAL'
    AND b.eligibility_status = 'ELIGIBLE'
