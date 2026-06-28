{{ config(alias="serving_jobs") }}

WITH publishable_job_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__publishable_job_facts') }}
),

prepared AS (
    SELECT
        job_id
        , canonical_url
        , source_url
        , title
        , company_name
        , company_id
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
    FROM publishable_job_facts
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
            , company_id
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
