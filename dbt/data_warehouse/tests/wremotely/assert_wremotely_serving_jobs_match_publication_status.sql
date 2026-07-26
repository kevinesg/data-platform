WITH serving_jobs AS (
    SELECT *
    FROM {{ ref('wremotely__serving_jobs') }}
),

publication_status AS (
    SELECT *
    FROM {{ ref('int_wremotely__job_publication_status') }}
)

SELECT
    serving_jobs.job_id
    , serving_jobs.is_deleted
    , publication_status.publication_status
    , publication_status.publication_status_reason
FROM serving_jobs
LEFT JOIN publication_status
    ON serving_jobs.job_id = publication_status.candidate_id
WHERE publication_status.candidate_id IS NULL
    OR (
        publication_status.publication_status = 'PUBLISHABLE'
        AND serving_jobs.is_deleted
    )
    OR (
        publication_status.publication_status IN ('CLOSED', 'NOT_PUBLISHABLE')
        AND NOT serving_jobs.is_deleted
    )
