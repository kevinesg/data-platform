{{ config(materialized='table') }}

with serving_jobs as (
    select *
    from {{ ref('wremotely__serving_jobs') }}
),

companies as (
    select *
    from {{ ref('wremotely__companies') }}
),

job_country_eligibility as (
    select *
    from {{ ref('wremotely__job_country_eligibility') }}
),

job_snapshot as (
    select
        count() as serving_job_count
        , max(dbt_updated_at) as job_publication_watermark_at
        , hex(SHA256(arrayStringConcat(arraySort(groupArray(serving_row_sha256)), '')))
            as serving_job_snapshot_sha256
    from serving_jobs
),

company_snapshot as (
    select
        count() as serving_company_count
        , max(dbt_updated_at) as company_publication_watermark_at
        , hex(SHA256(arrayStringConcat(arraySort(groupArray(company_row_sha256)), '')))
            as serving_company_snapshot_sha256
    from companies
),

active_company_source_snapshot as (
    select
        uniqExactIf(
            lowerUTF8(trim(ifNull(source_attribution_url, '')))
            , not is_deleted and notEmpty(ifNull(source_attribution_url, ''))
        ) as active_company_source_count
    from serving_jobs
),

job_country_snapshot as (
    select
        count() as job_country_eligibility_count
        , hex(SHA256(arrayStringConcat(
            arraySort(groupArray(job_country_eligibility_row_sha256)), ''
        ))) as job_country_eligibility_snapshot_sha256
    from job_country_eligibility
),

snapshot as (
    select
        4 as publication_contract_version
        , 'wremotely_serving_snapshot_v4' as serving_snapshot_contract
        , jobs.serving_job_count
        , active_companies.active_company_source_count as serving_company_count
        , countries.job_country_eligibility_count
        , greatest(
            ifNull(jobs.job_publication_watermark_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(companies.company_publication_watermark_at, toDateTime64('1970-01-01 00:00:00', 3))
        ) as publication_watermark_at
        , jobs.serving_job_snapshot_sha256
        , companies.serving_company_snapshot_sha256
        , countries.job_country_eligibility_snapshot_sha256
        , hex(SHA256(concat(
            toString(jobs.serving_job_count)
            , '|', jobs.serving_job_snapshot_sha256
            , '|', toString(active_companies.active_company_source_count)
            , '|', companies.serving_company_snapshot_sha256
            , '|', toString(countries.job_country_eligibility_count)
            , '|', countries.job_country_eligibility_snapshot_sha256
        ))) as serving_snapshot_sha256
    from job_snapshot as jobs
    cross join company_snapshot as companies
    cross join active_company_source_snapshot as active_companies
    cross join job_country_snapshot as countries
)

select
    concat('wremotely-', substring(serving_snapshot_sha256, 1, 16)) as publication_id
    , publication_contract_version
    , serving_snapshot_contract
    , serving_job_count
    , serving_company_count
    , job_country_eligibility_count
    , nullIf(publication_watermark_at, toDateTime64('1970-01-01 00:00:00', 3))
        as publication_watermark_at
    , serving_job_snapshot_sha256
    , serving_company_snapshot_sha256
    , job_country_eligibility_snapshot_sha256
    , serving_snapshot_sha256
    , 'dbt_modeled_not_signaled' as publication_state
from snapshot
