WITH company_job_counts AS (
    SELECT
        c.company_id
        , c.open_job_count
        , COUNT(j.job_id) AS expected_open_job_count
    FROM {{ ref('wremotely__companies') }} AS c
    LEFT JOIN {{ ref('wremotely__serving_jobs') }} AS j
        ON c.company_id = j.company_id
        AND NOT j.is_deleted
    GROUP BY
        c.company_id
        , c.open_job_count
)

SELECT *
FROM company_job_counts
WHERE open_job_count != expected_open_job_count
    OR expected_open_job_count = 0
