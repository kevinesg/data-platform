WITH specific_jobs AS (
    SELECT job_id
    FROM {{ ref('wremotely__serving_jobs') }}
    WHERE country_eligibility_scope = 'SPECIFIC'
),

eligible_bridge_rows AS (
    SELECT DISTINCT job_id
    FROM {{ ref('wremotely__job_country_eligibility') }}
    WHERE eligibility_status = 'ELIGIBLE'
)

SELECT sj.job_id
FROM specific_jobs AS sj
LEFT JOIN eligible_bridge_rows AS ebr
    ON sj.job_id = ebr.job_id
WHERE ebr.job_id IS NULL
