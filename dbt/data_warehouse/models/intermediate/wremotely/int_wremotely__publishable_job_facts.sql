WITH candidate_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__current_candidate_facts') }}
),

publishable_jobs AS (
    SELECT *
    FROM candidate_facts
    WHERE latest_job_posting_type = 'JOB'
        AND latest_remote_scope = 'REMOTE'
        AND validated_country_eligibility_scope IN ('GLOBAL', 'GLOBAL_EXCEPT', 'SPECIFIC')
        AND COALESCE(latest_lifecycle_status, 'REACHABLE') NOT IN ('CLOSED', 'TERMINAL')
),

prepared AS (
    SELECT
        candidate_id AS job_id
        , COALESCE(NULLIF(latest_final_url, ''), url) AS canonical_url
        , url AS source_url
        , title
        , NULLIF(TRIM(company_name), '') AS company_name
        , NULLIF(
            REGEXP_REPLACE(LOWER(TRIM(company_name)), r'\s+', ' ')
            , ''
        ) AS normalized_company_name
        , NULLIF(LOWER(TRIM(source_domain)), '') AS normalized_source_domain
        , candidate_required_location AS location_text
        , publication_at AS source_publication_at
        , latest_observed_at
        , source_domain
        , attribution_name AS source_attribution_name
        , attribution_url AS source_attribution_url
        , latest_remote_scope AS remote_scope
        , validated_country_eligibility_scope AS country_eligibility_scope
        , eligible_country_codes
        , excluded_country_codes
        , included_country_group_codes
        , excluded_country_group_codes
        , country_eligibility_evidence_count
        , latest_job_status AS source_job_status
        , latest_lifecycle_status AS lifecycle_status
        , latest_lifecycle_checked_at AS lifecycle_checked_at
        , has_lifecycle_recheck
        , LEFT(snippet, 1000) AS public_snippet
    FROM publishable_jobs
),

company_keyed AS (
    SELECT
        *
        , CASE
            WHEN normalized_company_name IS NOT NULL
                AND normalized_source_domain IS NOT NULL
                THEN TO_JSON_STRING(STRUCT(
                    'company_source_domain_v1' AS identity_version
                    , normalized_source_domain AS source_domain
                    , normalized_company_name AS company_name
                ))
        END AS company_identity_key
        , CASE
            WHEN normalized_company_name IS NOT NULL
                AND normalized_source_domain IS NOT NULL
                THEN 'company_source_domain_v1'
        END AS company_identity_basis
    FROM prepared
),

final AS (
    SELECT
        job_id
        , canonical_url
        , source_url
        , title
        , company_name
        , CASE
            WHEN company_identity_key IS NOT NULL
                THEN CONCAT(
                    'company_'
                    , SUBSTR(TO_HEX(SHA256(company_identity_key)), 1, 32)
                )
        END AS company_id
        , company_identity_basis
        , company_identity_key
        , normalized_company_name
        , normalized_source_domain AS company_identity_source_domain
        , location_text
        , source_publication_at
        , latest_observed_at
        , source_domain
        , source_attribution_name
        , source_attribution_url
        , remote_scope
        , country_eligibility_scope
        , eligible_country_codes
        , excluded_country_codes
        , included_country_group_codes
        , excluded_country_group_codes
        , country_eligibility_evidence_count
        , source_job_status
        , lifecycle_status
        , lifecycle_checked_at
        , has_lifecycle_recheck
        , public_snippet
    FROM company_keyed
)

SELECT *
FROM final
