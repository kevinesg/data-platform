SELECT
    job_id
    , canonical_url
    , title
    , remote_scope
    , country_eligibility_scope
    , lifecycle_status
FROM {{ ref('wremotely__serving_jobs') }}
WHERE canonical_url IS NULL
    OR source_url IS NULL
    OR title IS NULL
    OR remote_scope != 'remote'
    OR country_eligibility_scope NOT IN ('global', 'target_country')
    OR COALESCE(lifecycle_status, 'reachable') IN ('closed', 'terminal')
