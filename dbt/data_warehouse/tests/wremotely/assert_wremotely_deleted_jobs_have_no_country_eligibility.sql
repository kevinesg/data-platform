SELECT
    bridge.job_id
    , bridge.country_code
    , bridge.eligibility_status
FROM {{ ref('wremotely__job_country_eligibility') }} AS bridge
INNER JOIN {{ ref('wremotely__serving_jobs') }} AS job
    ON bridge.job_id = job.job_id
WHERE job.is_deleted
