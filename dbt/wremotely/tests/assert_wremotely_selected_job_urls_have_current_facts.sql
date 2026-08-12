SELECT
    s.candidate_id
    , s.url
FROM {{ ref('int_wremotely__latest_selected_job_urls') }} AS s
LEFT JOIN {{ ref('int_wremotely__current_candidate_facts') }} AS c
    ON s.candidate_id = c.candidate_id
WHERE c.candidate_id IS NULL
