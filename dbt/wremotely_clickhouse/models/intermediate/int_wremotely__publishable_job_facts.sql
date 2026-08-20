{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='job_id',
    on_schema_change='append_new_columns',
    order_by="(ifNull(job_id, ''))"
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['dbt_updated_at']) %}
{% set title_cleanup_version_ready = is_incremental()
    and relation_has_columns(this, ['title_cleanup_version']) %}

with publishable_jobs as (
    select *
    from {{ ref('int_wremotely__job_publication_status') }}
    where (
        publication_status in ('PUBLISHABLE', 'CLOSED')
        {% if incremental_watermark_ready %}
        or candidate_id in (select job_id from {{ this }})
        {% endif %}
    )
    {% if incremental_watermark_ready %}
        and (
            dbt_updated_at >= (
                select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
                from {{ this }}
            )
            {% if title_cleanup_version_ready %}
            or candidate_id in (
                select job_id
                from {{ this }}
                where ifNull(title_cleanup_version, '')
                    != {{ wremotely_title_cleanup_version() }}
            )
            {% else %}
            or 1 = 1
            {% endif %}
        )
    {% endif %}
),

prepared as (
    select
        candidate_id as job_id
        , coalesce(nullIf(trim(job_identity_url), ''), url) as canonical_url
        , url as source_url
        , title
        , nullIf(trim(company_name), '') as company_name
        , nullIf(replaceRegexpAll(lower(trim(company_name)), '\\s+', ' '), '')
            as normalized_company_name
        , nullIf(lower(trim(source_domain)), '') as normalized_source_domain
        , candidate_required_location as location_text
        , publication_at as source_publication_at
        , latest_job_fact_raw_valid_through_at as source_valid_through_at
        , latest_observed_at
        , source_domain
        , attribution_name as source_attribution_name
        , attribution_url as source_attribution_url
        , latest_remote_scope as remote_scope
        , latest_job_fact_raw_work_arrangement as raw_work_arrangement
        , validated_country_eligibility_scope as country_eligibility_scope
        , eligible_country_codes
        , excluded_country_codes
        , included_country_group_codes
        , excluded_country_group_codes
        , country_eligibility_evidence_count
        , latest_job_status as source_job_status
        , job_description
        , latest_job_fact_raw_base_salary_json as base_salary_json
        , latest_job_fact_raw_estimated_salary_json as estimated_salary_json
        , latest_job_fact_raw_employment_type_values as raw_employment_type_values
        , latest_job_fact_raw_employment_type as raw_employment_type
        , latest_job_fact_declared_language_tag as declared_language_tag
        , nullIf(latest_lifecycle_status, '') as lifecycle_status
        , latest_lifecycle_checked_at as lifecycle_checked_at
        , has_lifecycle_recheck
        , publication_status != 'PUBLISHABLE' as is_deleted
        , {{ wremotely_title_cleanup_version() }} as title_cleanup_version
        , dbt_updated_at as _updated_at
        , latest_observed_at as source_updated_at
        , left(snippet, 1000) as public_snippet
    from publishable_jobs
),

title_cleaned as (
    select
        prepared.* except (title)
        , {{ wremotely_clean_job_title('prepared.title', 'prepared.company_name') }} as title
    from prepared
),

company_keyed as (
    select
        title_cleaned.*
        , if(
            normalized_company_name is not null and normalized_source_domain is not null
            , concat('company_source_domain_v1|', normalized_source_domain, '|', normalized_company_name)
            , null
        ) as company_identity_key
        , if(
            normalized_company_name is not null and normalized_source_domain is not null
            , 'company_source_domain_v1'
            , null
        ) as company_identity_basis
    from title_cleaned
)

select
    job_id
    , canonical_url
    , source_url
    , title
    , company_name
    , if(company_identity_key is not null,
        concat('company_', substring(hex(SHA256(company_identity_key)), 1, 32)), null)
        as company_id
    , company_identity_basis
    , company_identity_key
    , normalized_company_name
    , normalized_source_domain as company_identity_source_domain
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
    , raw_employment_type_values
    , raw_employment_type
    , declared_language_tag
    , title_cleanup_version
    , lifecycle_status
    , lifecycle_checked_at
    , has_lifecycle_recheck
    , is_deleted
    , _updated_at
    , _updated_at as dbt_updated_at
    , source_updated_at
    , public_snippet
from company_keyed
