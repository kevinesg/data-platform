SELECT
    j.job_id
    , j.country_eligibility_scope
    , b.country_code
FROM {{ ref('wremotely__job_country_eligibility') }} AS b
INNER JOIN {{ ref('wremotely__serving_jobs') }} AS j
    ON b.job_id = j.job_id
WHERE b.eligibility_status = 'EXCLUDED'
    AND j.country_eligibility_scope = 'GLOBAL'
