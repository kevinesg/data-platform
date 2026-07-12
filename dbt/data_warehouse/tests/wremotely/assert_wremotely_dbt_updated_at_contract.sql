SELECT
    job_id
    , source_updated_at
    , dbt_updated_at
    , _updated_at
FROM {{ ref('wremotely__serving_jobs') }}
WHERE source_updated_at IS NULL
    OR dbt_updated_at IS NULL
    OR _updated_at IS NULL
    OR _updated_at != dbt_updated_at
