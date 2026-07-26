{{
    config(
        alias="serving_jobs",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="job_id",
        on_schema_change="append_new_columns"
    )
}}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_updated_at', 'dbt_updated_at']) %}

WITH publishable_job_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__publishable_job_facts') }}
),

search_facets AS (
    SELECT *
    FROM {{ ref('int_wremotely__job_search_facets') }}
),

publication_status AS (
    SELECT
        candidate_id AS job_id
        , publication_status
        , source_updated_at
    FROM {{ ref('int_wremotely__job_publication_status') }}
),

incremental_source AS (
    SELECT
        job.*
        , facets.employment_types
        , facets.search_tags
    FROM publishable_job_facts AS job
    INNER JOIN search_facets AS facets
        USING (job_id)
    {% if incremental_watermark_ready %}
    WHERE NOT EXISTS (
            SELECT 1
            FROM {{ this }} AS current_job
            WHERE current_job.job_id = job.job_id
        )
        OR job.source_updated_at > (
            SELECT current_job.source_updated_at
            FROM {{ this }} AS current_job
            WHERE current_job.job_id = job.job_id
        )
        OR (
            (
                SELECT current_job.is_deleted
                FROM {{ this }} AS current_job
                WHERE current_job.job_id = job.job_id
            )
            AND NOT job.is_deleted
        )
    {% endif %}
),

prepared AS (
    SELECT
        job_id
        , canonical_url
        , source_url
        , title
        , company_name
        , company_id
        , company_identity_basis
        , company_identity_source_domain
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
        , declared_language_tag
        , lifecycle_status
        , lifecycle_checked_at
        , has_lifecycle_recheck
        , is_deleted
        , public_snippet
        , employment_types[SAFE_OFFSET(0)] AS employment_type
        , employment_types
        , search_tags
        , source_updated_at
        , TIMESTAMP('{{ run_started_at.isoformat() }}') AS dbt_updated_at
        , TIMESTAMP('{{ run_started_at.isoformat() }}') AS _updated_at
    FROM incremental_source
),

suppressed AS (
    {% if incremental_watermark_ready %}
    SELECT
        previous.* EXCEPT (
            publication_hold_content_sha256
            , serving_content_sha256
            , serving_row_sha256
        ) REPLACE (
            TRUE AS is_deleted
            , GREATEST(
                previous.source_updated_at
                , COALESCE(status.source_updated_at, previous.source_updated_at)
            ) AS source_updated_at
            , TIMESTAMP('{{ run_started_at.isoformat() }}') AS dbt_updated_at
            , TIMESTAMP('{{ run_started_at.isoformat() }}') AS _updated_at
        )
    FROM {{ this }} AS previous
    INNER JOIN publication_status AS status
        USING (job_id)
    WHERE NOT previous.is_deleted
        AND status.publication_status = 'NOT_PUBLISHABLE'
    {% else %}
    SELECT *
    FROM prepared
    WHERE FALSE
    {% endif %}
),

changes AS (
    SELECT *
    FROM prepared

    UNION ALL BY NAME

    SELECT *
    FROM suppressed
),

content_hashed AS (
    SELECT
        *
        , TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
            title
            , location_text
            , remote_scope
            , raw_work_arrangement
            , country_eligibility_scope
            , eligible_country_codes
            , excluded_country_codes
            , included_country_group_codes
            , excluded_country_group_codes
            , job_description
            , base_salary_json
            , estimated_salary_json
            , employment_types
            , declared_language_tag
        )))) AS publication_hold_content_sha256
        , TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
            job_id
            , canonical_url
            , source_url
            , title
            , company_name
            , company_id
            , company_identity_basis
            , company_identity_source_domain
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
            , employment_types
            , search_tags
            , declared_language_tag
            , lifecycle_status
            , lifecycle_checked_at
            , has_lifecycle_recheck
            , is_deleted
            , public_snippet
        )))) AS serving_content_sha256
    FROM changes
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
            , company_identity_basis
            , company_identity_source_domain
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
            , is_deleted
            , _updated_at
            , source_updated_at
            , dbt_updated_at
            , public_snippet
            , publication_hold_content_sha256
            , serving_content_sha256
        )))) AS serving_row_sha256
    FROM content_hashed
)

SELECT *
FROM final
