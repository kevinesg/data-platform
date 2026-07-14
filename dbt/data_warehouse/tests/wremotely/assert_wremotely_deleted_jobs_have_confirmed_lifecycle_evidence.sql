WITH serving_jobs AS (
    SELECT *
    FROM {{ ref('wremotely__serving_jobs') }}
),

candidate_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__current_candidate_facts') }}
)

SELECT serving_jobs.job_id
FROM serving_jobs
INNER JOIN candidate_facts
    ON serving_jobs.job_id = candidate_facts.candidate_id
WHERE serving_jobs.is_deleted
    != COALESCE(
        candidate_facts.latest_lifecycle_status = 'CLOSED'
        OR (
            candidate_facts.latest_lifecycle_status = 'TERMINAL'
            AND candidate_facts.previous_lifecycle_status = 'TERMINAL'
        )
        , FALSE
    )
