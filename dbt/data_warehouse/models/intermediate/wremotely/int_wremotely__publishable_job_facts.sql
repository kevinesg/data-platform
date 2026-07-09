WITH candidate_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__current_candidate_facts') }}
),

publishable_jobs AS (
    SELECT *
    FROM candidate_facts
    WHERE latest_job_posting_type = 'JOB'
        AND latest_remote_scope IN ('REMOTE', 'HYBRID')
        AND validated_country_eligibility_scope IN ('GLOBAL', 'GLOBAL_EXCEPT', 'SPECIFIC')
        AND (
            validated_country_eligibility_scope != 'SPECIFIC'
            OR ARRAY_LENGTH(IFNULL(eligible_country_codes, ARRAY<STRING>[])) > 0
        )
        AND COALESCE(latest_lifecycle_status, 'REACHABLE') NOT IN ('CLOSED', 'TERMINAL')
        AND NULLIF(TRIM(title), '') IS NOT NULL
        AND (
            latest_job_fact_declared_language_tag IS NULL
            OR STARTS_WITH(latest_job_fact_declared_language_tag, 'en')
        )
        AND has_publication_hold
        AND publication_hold_matches_current_content
        AND latest_publication_hold_status = 'RELEASED'
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
        , latest_job_fact_raw_valid_through_at AS source_valid_through_at
        , latest_observed_at
        , source_domain
        , attribution_name AS source_attribution_name
        , attribution_url AS source_attribution_url
        , latest_remote_scope AS remote_scope
        , latest_job_fact_raw_work_arrangement AS raw_work_arrangement
        , validated_country_eligibility_scope AS country_eligibility_scope
        , eligible_country_codes
        , excluded_country_codes
        , included_country_group_codes
        , excluded_country_group_codes
        , country_eligibility_evidence_count
        , latest_job_status AS source_job_status
        , job_description
        , latest_job_fact_raw_base_salary_json AS base_salary_json
        , latest_job_fact_raw_estimated_salary_json AS estimated_salary_json
        , latest_job_fact_raw_employment_type AS employment_type
        , latest_job_fact_declared_language_tag AS declared_language_tag
        , latest_lifecycle_status AS lifecycle_status
        , latest_lifecycle_checked_at AS lifecycle_checked_at
        , has_lifecycle_recheck
        , latest_publication_hold_status AS publication_hold_status
        , latest_publication_hold_reason_code AS publication_hold_reason_code
        , latest_publication_hold_evaluated_at AS publication_hold_evaluated_at
        , publication_hold_matches_current_content
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
        , source_valid_through_at
        , latest_observed_at
        , source_domain
        , source_attribution_name
        , source_attribution_url
        , remote_scope
        , raw_work_arrangement
        , country_eligibility_scope
        , eligible_country_codes
        , excluded_country_codes
        , included_country_group_codes
        , excluded_country_group_codes
        , country_eligibility_evidence_count
        , source_job_status
        , job_description
        , base_salary_json
        , estimated_salary_json
        , employment_type
        , declared_language_tag
        , lifecycle_status
        , lifecycle_checked_at
        , has_lifecycle_recheck
        , publication_hold_status
        , publication_hold_reason_code
        , publication_hold_evaluated_at
        , publication_hold_matches_current_content
        , public_snippet
    FROM company_keyed
)

SELECT *
FROM final
