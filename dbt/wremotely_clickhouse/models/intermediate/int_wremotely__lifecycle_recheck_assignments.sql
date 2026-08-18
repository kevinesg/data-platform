{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='candidate_id',
    on_schema_change='append_new_columns',
    order_by="(bucket_index, source_key, source_position, candidate_id)",
    query_settings={
        'max_bytes_before_external_sort': 268435456,
        'max_memory_usage': 4294967296,
        'max_threads': 2
    }
) }}

{% set bucket_count = 7 %}
{% set minimum_posting_age_days = env_var('WREMOTELY_LIFECYCLE_MIN_POSTING_AGE_DAYS', '21') | int %}

with eligible_jobs as (
    select
        assumeNotNull(job_id) as candidate_id
        , assumeNotNull(coalesce(
            nullIf(ifNull(company_id, ''), '')
            , nullIf(ifNull(source_attribution_url, ''), '')
            , nullIf(ifNull(source_domain, ''), '')
            , nullIf(ifNull(canonical_url, ''), '')
            , job_id
        )) as source_key
        , source_publication_at
    from {{ ref('wremotely__serving_jobs') }}
    where ifNull(is_deleted, 0) = 0
        and notEmpty(ifNull(job_id, ''))
        and (
            source_publication_at is null
            or source_publication_at <= now() - interval {{ minimum_posting_age_days }} day
        )
),

{% if is_incremental() %}
new_jobs as (
    select jobs.*
    from eligible_jobs as jobs
    where not exists (
        select 1
        from {{ this }} as assignments
        where assignments.candidate_id = jobs.candidate_id
    )
),

source_offsets as (
    select
        source_key
        , max(source_position) + 1 as next_source_position
    from {{ this }}
    group by source_key
),
{% else %}
new_jobs as (
    select *
    from eligible_jobs
),

source_offsets as (
    select
        cast('' as String) as source_key
        , cast(0 as UInt64) as next_source_position
    where 1 = 0
),
{% endif %}

ranked_new_jobs as (
    select
        jobs.*
        , ifNull(offsets.next_source_position, 0) as source_offset
        , row_number() over (
            partition by jobs.source_key
            order by jobs.candidate_id
        ) as new_source_position_rank
    from new_jobs as jobs
    left join source_offsets as offsets
        on jobs.source_key = offsets.source_key
)

select
    candidate_id
    , source_key
    , toInt64(source_offset + (toInt64(new_source_position_rank) - 1)) as source_position
    , toUInt8({{ bucket_count }}) as bucket_count
    , toUInt8(
        modulo(
            cityHash64(source_key)
                + toUInt64(source_offset + (toInt64(new_source_position_rank) - 1))
            , toUInt64({{ bucket_count }})
        )
    ) as bucket_index
    , 'source_stratified_v1' as assignment_version
    , source_publication_at
    , now64(3) as assigned_at
    , toUInt8(source_publication_at is not null) as posting_date_known
from ranked_new_jobs
