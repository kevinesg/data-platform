SELECT
    job_id
    , canonical_url
    , title
    , remote_scope
    , country_eligibility_scope
    , eligible_country_codes
    , excluded_country_codes
    , lifecycle_status
    , is_deleted
FROM {{ ref('wremotely__serving_jobs') }}
WHERE canonical_url IS NULL
    OR source_url IS NULL
    OR title IS NULL
    OR remote_scope NOT IN ('REMOTE', 'HYBRID')
    OR country_eligibility_scope NOT IN ('GLOBAL', 'GLOBAL_EXCEPT', 'SPECIFIC')
    OR (
        country_eligibility_scope = 'SPECIFIC'
        AND ARRAY_LENGTH(eligible_country_codes) = 0
    )
