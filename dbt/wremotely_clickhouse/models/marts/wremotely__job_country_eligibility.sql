{{ config(materialized='table') }}

with bridge as (
    select *
    from {{ ref('int_wremotely__job_country_eligibility') }}
),

serving_jobs as (
    select *
    from {{ ref('wremotely__serving_jobs') }}
    where not is_deleted
)

select
    bridge.job_id
    , bridge.country_code
    , bridge.eligibility_status
    , bridge.country_eligibility_scope
    , hex(SHA256(concat(
        ifNull(bridge.job_id, '')
        , '|', ifNull(bridge.country_code, '')
        , '|', ifNull(bridge.eligibility_status, '')
        , '|', ifNull(bridge.country_eligibility_scope, '')
    ))) as job_country_eligibility_row_sha256
    , serving_jobs.source_updated_at
    , serving_jobs.dbt_updated_at
from bridge
inner join serving_jobs
    on bridge.job_id = serving_jobs.job_id
