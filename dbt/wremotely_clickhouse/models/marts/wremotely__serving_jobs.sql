{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='job_id',
    on_schema_change='append_new_columns',
    order_by="(ifNull(job_id, ''))"
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['dbt_updated_at']) %}
with publishable_jobs as (
    select *
    from {{ ref('int_wremotely__publishable_job_facts') }}
    {% if incremental_watermark_ready %}
    where dbt_updated_at >= (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}
),

search_facets as (
    select *
    from {{ ref('int_wremotely__job_search_facets') }}
),

prepared as (
    select
        jobs.job_id
        , jobs.canonical_url
        , jobs.source_url
        , jobs.title
        , jobs.company_name
        , jobs.company_id
        , jobs.company_identity_basis
        , jobs.company_identity_source_domain
        , jobs.location_text
        , jobs.source_publication_at
        , jobs.source_valid_through_at
        , jobs.latest_observed_at
        , jobs.source_domain
        , jobs.source_attribution_name
        , jobs.source_attribution_url
        , jobs.remote_scope
        , jobs.raw_work_arrangement
        , jobs.country_eligibility_scope
        , jobs.eligible_country_codes
        , jobs.excluded_country_codes
        , jobs.included_country_group_codes
        , jobs.excluded_country_group_codes
        , jobs.country_eligibility_evidence_count
        , jobs.source_job_status
        , jobs.job_description
        , jobs.base_salary_json
        , jobs.estimated_salary_json
        , arrayElement(facets.employment_types, 1) as employment_type
        , facets.employment_types
        , facets.search_tags
        , jobs.declared_language_tag
        , jobs.lifecycle_status
        , jobs.lifecycle_checked_at
        , jobs.has_lifecycle_recheck
        , jobs.is_deleted
        , jobs.dbt_updated_at as _updated_at
        , jobs.source_updated_at
        , jobs.dbt_updated_at
        , jobs.public_snippet
    from publishable_jobs as jobs
    inner join search_facets as facets
        on jobs.job_id = facets.job_id
),

content_hashed as (
    select
        prepared.*
        , hex(SHA256(concat(
            ifNull(title, '')
            , '|', ifNull(location_text, '')
            , '|', ifNull(remote_scope, '')
            , '|', ifNull(raw_work_arrangement, '')
            , '|', ifNull(country_eligibility_scope, '')
            , '|', arrayStringConcat(eligible_country_codes, ',')
            , '|', arrayStringConcat(excluded_country_codes, ',')
            , '|', ifNull(job_description, '')
            , '|', ifNull(base_salary_json, '')
            , '|', ifNull(estimated_salary_json, '')
            , '|', arrayStringConcat(employment_types, ',')
            , '|', ifNull(declared_language_tag, '')
        ))) as publication_hold_content_sha256
        , hex(SHA256(concat(
            ifNull(job_id, '')
            , '|', ifNull(canonical_url, '')
            , '|', ifNull(source_url, '')
            , '|', ifNull(title, '')
            , '|', ifNull(company_name, '')
            , '|', ifNull(company_id, '')
            , '|', ifNull(company_identity_basis, '')
            , '|', ifNull(company_identity_source_domain, '')
            , '|', ifNull(location_text, '')
            , '|', ifNull(source_domain, '')
            , '|', ifNull(remote_scope, '')
            , '|', ifNull(raw_work_arrangement, '')
            , '|', ifNull(country_eligibility_scope, '')
            , '|', arrayStringConcat(eligible_country_codes, ',')
            , '|', arrayStringConcat(excluded_country_codes, ',')
            , '|', arrayStringConcat(employment_types, ',')
            , '|', arrayStringConcat(search_tags, ',')
            , '|', ifNull(job_description, '')
            , '|', toString(is_deleted)
            , '|', ifNull(public_snippet, '')
        ))) as serving_content_sha256
    from prepared
)

select
    content_hashed.*
    , hex(SHA256(concat(
        ifNull(job_id, '')
        , '|', ifNull(canonical_url, '')
        , '|', ifNull(title, '')
        , '|', ifNull(company_id, '')
        , '|', ifNull(serving_content_sha256, '')
        , '|', ifNull(publication_hold_content_sha256, '')
    ))) as serving_row_sha256
from content_hashed
