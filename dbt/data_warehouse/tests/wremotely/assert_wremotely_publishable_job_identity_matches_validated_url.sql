SELECT
    publishable.job_id
    , publishable.canonical_url
    , status.job_identity_url
    , status.url AS selected_url
    , status.latest_final_url
FROM {{ ref('int_wremotely__publishable_job_facts') }} AS publishable
INNER JOIN {{ ref('int_wremotely__job_publication_status') }} AS status
    ON publishable.job_id = status.candidate_id
WHERE publishable.canonical_url IS DISTINCT FROM COALESCE(
    NULLIF(TRIM(status.job_identity_url), '')
    , status.url
)
