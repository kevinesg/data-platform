SELECT
    job_id
    , publication_hold_status
    , publication_hold_matches_current_content
FROM {{ ref('int_wremotely__publishable_job_facts') }}
WHERE publication_hold_status != 'RELEASED'
    OR NOT publication_hold_matches_current_content
