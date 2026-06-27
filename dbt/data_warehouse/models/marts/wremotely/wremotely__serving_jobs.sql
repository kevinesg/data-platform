{{ config(alias="serving_jobs") }}

WITH candidate_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__current_candidate_facts') }}
),

publishable_jobs AS (
    SELECT *
    FROM candidate_facts
    WHERE latest_serving_decision = 'publishable'
        AND latest_job_posting_type = 'job'
        AND latest_remote_scope = 'remote'
        AND latest_country_eligibility_scope IN ('global', 'target_country')
        AND COALESCE(latest_lifecycle_status, 'reachable') NOT IN ('closed', 'terminal')
),

prepared AS (
    SELECT
        candidate_id AS job_id
        , COALESCE(NULLIF(latest_final_url, ''), url) AS canonical_url
        , url AS source_url
        , title
        , company_name
        , candidate_required_location AS location_text
        , publication_at AS source_publication_at
        , latest_observed_at
        , source_domain
        , attribution_name AS source_attribution_name
        , attribution_url AS source_attribution_url
        , latest_remote_scope AS remote_scope
        , latest_country_eligibility_scope AS country_eligibility_scope
        , latest_target_country AS target_country
        , latest_target_country_code AS target_country_code
        , latest_target_country_eligibility AS target_country_eligibility
        , latest_job_status AS source_job_status
        , latest_lifecycle_status AS lifecycle_status
        , latest_lifecycle_checked_at AS lifecycle_checked_at
        , has_lifecycle_recheck
        , LEFT(snippet, 1000) AS public_snippet
    FROM publishable_jobs
),

final AS (
    SELECT
        *
        , TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
            job_id
            , canonical_url
            , source_url
            , title
            , company_name
            , location_text
            , source_publication_at
            , latest_observed_at
            , source_domain
            , source_attribution_name
            , source_attribution_url
            , remote_scope
            , country_eligibility_scope
            , target_country
            , target_country_code
            , target_country_eligibility
            , source_job_status
            , lifecycle_status
            , lifecycle_checked_at
            , has_lifecycle_recheck
            , public_snippet
        )))) AS serving_row_sha256
    FROM prepared
)

SELECT *
FROM final
