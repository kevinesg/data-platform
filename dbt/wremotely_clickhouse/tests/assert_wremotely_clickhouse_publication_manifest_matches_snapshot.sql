with job_snapshot as (
    select
        count() as serving_job_count
        , hex(SHA256(arrayStringConcat(arraySort(groupArray(serving_row_sha256)), '')))
            as serving_job_snapshot_sha256
    from {{ ref('wremotely__serving_jobs') }}
), company_snapshot as (
    select
        count() as serving_company_count
        , hex(SHA256(arrayStringConcat(arraySort(groupArray(company_row_sha256)), '')))
            as serving_company_snapshot_sha256
    from {{ ref('wremotely__companies') }}
), active_company_source_snapshot as (
    select
        uniqExactIf(
            lowerUTF8(trim(ifNull(source_attribution_url, '')))
            , not is_deleted and notEmpty(ifNull(source_attribution_url, ''))
        ) as active_company_source_count
    from {{ ref('wremotely__serving_jobs') }}
), country_snapshot as (
    select
        count() as job_country_eligibility_count
        , hex(SHA256(arrayStringConcat(
            arraySort(groupArray(job_country_eligibility_row_sha256)), ''
        ))) as job_country_eligibility_snapshot_sha256
    from {{ ref('wremotely__job_country_eligibility') }}
), expected as (
    select
        concat(
            'wremotely-'
            , substring(hex(SHA256(concat(
                toString(j.serving_job_count)
                , '|', j.serving_job_snapshot_sha256
                , '|', toString(a.active_company_source_count)
                , '|', c.serving_company_snapshot_sha256
                , '|', toString(k.job_country_eligibility_count)
                , '|', k.job_country_eligibility_snapshot_sha256
            ))), 1, 16)
        ) as publication_id
        , 'wremotely_serving_snapshot_v4' as serving_snapshot_contract
        , j.serving_job_count
        , a.active_company_source_count as serving_company_count
        , k.job_country_eligibility_count
        , j.serving_job_snapshot_sha256
        , c.serving_company_snapshot_sha256
        , k.job_country_eligibility_snapshot_sha256
        , hex(SHA256(concat(
            toString(j.serving_job_count)
            , '|', j.serving_job_snapshot_sha256
            , '|', toString(a.active_company_source_count)
            , '|', c.serving_company_snapshot_sha256
            , '|', toString(k.job_country_eligibility_count)
            , '|', k.job_country_eligibility_snapshot_sha256
        ))) as serving_snapshot_sha256
    from job_snapshot as j
    cross join company_snapshot as c
    cross join active_company_source_snapshot as a
    cross join country_snapshot as k
)
select
    actual.publication_id
    , actual.serving_snapshot_sha256
    , expected.publication_id as expected_publication_id
    , expected.serving_snapshot_sha256 as expected_serving_snapshot_sha256
from {{ ref('wremotely__publication_manifest') }} as actual
cross join expected
where actual.publication_id != expected.publication_id
    or actual.serving_snapshot_contract != expected.serving_snapshot_contract
    or actual.serving_job_count != expected.serving_job_count
    or actual.serving_company_count != expected.serving_company_count
    or actual.job_country_eligibility_count != expected.job_country_eligibility_count
    or actual.serving_job_snapshot_sha256 != expected.serving_job_snapshot_sha256
    or actual.serving_company_snapshot_sha256 != expected.serving_company_snapshot_sha256
    or actual.job_country_eligibility_snapshot_sha256
        != expected.job_country_eligibility_snapshot_sha256
    or actual.serving_snapshot_sha256 != expected.serving_snapshot_sha256
