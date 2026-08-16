{{ config(materialized='table') }}

with serving_jobs as (
    select *
    from {{ ref('wremotely__serving_jobs') }}
),

active_jobs as (
    select *
    from serving_jobs
    where not is_deleted
        and company_id is not null
),

company_change_watermarks as (
    select
        company_id
        , max(dbt_updated_at) as dbt_updated_at
    from serving_jobs
    where company_id is not null
    group by company_id
),

aggregated as (
    select
        active.company_id
        , argMax(active.company_name, tuple(active.latest_observed_at, active.job_id))
            as company_name
        , argMax(active.company_identity_basis, tuple(active.latest_observed_at, active.job_id))
            as company_identity_basis
        , argMax(active.company_identity_source_domain, tuple(active.latest_observed_at, active.job_id))
            as source_domain
        , count() as open_job_count
        , min(active.source_publication_at) as first_source_publication_at
        , min(active.latest_observed_at) as first_observed_at
        , max(active.latest_observed_at) as latest_observed_at
        , max(active.source_updated_at) as source_updated_at
        , max(watermarks.dbt_updated_at) as dbt_updated_at
    from active_jobs as active
    inner join company_change_watermarks as watermarks
        using (company_id)
    group by active.company_id
)

select
    aggregated.*
    , hex(SHA256(concat(
        ifNull(company_id, '')
        , '|', ifNull(company_name, '')
        , '|', ifNull(company_identity_basis, '')
        , '|', ifNull(source_domain, '')
        , '|', toString(open_job_count)
        , '|', toString(first_source_publication_at)
        , '|', toString(first_observed_at)
        , '|', toString(latest_observed_at)
        , '|', toString(source_updated_at)
        , '|', toString(dbt_updated_at)
    ))) as company_row_sha256
from aggregated
