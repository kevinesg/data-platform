WITH serving_jobs AS (
    SELECT job_id
    FROM {{ ref('wremotely__serving_jobs') }}
),

held_candidates AS (
    SELECT candidate_id
    FROM {{ ref('int_wremotely__current_candidate_facts') }}
    WHERE publication_hold_matches_current_content
        AND latest_publication_hold_status IN ('HELD', 'REVIEW_HOLD')
)

SELECT
    s.job_id
FROM serving_jobs AS s
INNER JOIN held_candidates AS h
    ON s.job_id = h.candidate_id
