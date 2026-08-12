SELECT
    job.job_id
    , search_tag
FROM {{ ref('wremotely__serving_jobs') }} AS job
CROSS JOIN UNNEST(job.search_tags) AS search_tag
LEFT JOIN {{ ref('wremotely__search_tags') }} AS taxonomy
    ON search_tag = taxonomy.tag_code
WHERE taxonomy.tag_code IS NULL
