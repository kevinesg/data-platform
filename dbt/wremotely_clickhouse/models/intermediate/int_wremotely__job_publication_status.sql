{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='candidate_id',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''))"
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['dbt_updated_at']) %}

with review_decisions as (
    select *
    from {{ ref('stg_wremotely__publication_review') }}
),

candidate_facts as (
    select *
    from {{ ref('int_wremotely__current_candidate_facts') }} as facts
    {% if incremental_watermark_ready %}
        where facts.dbt_updated_at >= (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    or (
        facts.latest_job_fact_raw_valid_through_at >= now64(3)
        and facts.latest_job_fact_raw_valid_through_at <= now64(3) + interval 1 day
    )
    or facts.candidate_id in (
        select candidate_id
        from review_decisions
        where review_updated_at > (
            select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
            from {{ this }}
        )
    )
    or (
        lowerUTF8(ifNull(facts.latest_job_fact_source_platform_guess, '')) = 'ashby'
        and (
            empty(trim(replaceRegexpAll(
                ifNull(facts.job_description, '')
                , '<[^>]*>'
                , ''
            )))
            or match(
                trim(replaceRegexpAll(
                    ifNull(facts.job_description, '')
                    , '<[^>]*>'
                    , ''
                ))
                , '^https?://[^[:space:]]+$'
            )
        )
    )
    {% endif %}
),

evaluated as (
    select
        candidate_facts.* except (dbt_updated_at)
        , coalesce(nullIf(review.review_status, ''), 'unreviewed')
            as publication_review_status
        , review.review_reason_code as publication_review_reason_code
        , review.review_reason as publication_review_reason
        , review.review_updated_at as publication_review_updated_at
        , greatest(
            candidate_facts.dbt_updated_at
            , ifNull(review.review_updated_at, toDateTime64('1970-01-01 00:00:00', 3))
            -- This model can re-evaluate a row because publication rules changed
            -- even when the source fact did not. Advance the processing watermark
            -- so downstream incremental tombstones and serving rows see that change.
            , now64(3)
        ) as dbt_updated_at
        , facts.latest_job_fact_raw_valid_through_at is not null
            and facts.latest_job_fact_raw_valid_through_at <= now64(3)
            as has_expired_valid_through
        , (
            facts.latest_job_posting_type = 'JOB'
            and facts.latest_remote_scope in ('REMOTE', 'HYBRID', 'ONSITE')
            and facts.validated_country_eligibility_scope
                in ('GLOBAL', 'GLOBAL_EXCEPT', 'SPECIFIC')
            and (
                facts.validated_country_eligibility_scope != 'SPECIFIC'
                or length(facts.eligible_country_codes) > 0
            )
            and notEmpty(ifNull(facts.title, ''))
            and (
                facts.latest_job_fact_declared_language_tag is null
                or startsWith(facts.latest_job_fact_declared_language_tag, 'en')
            )
        ) as meets_content_publication_requirements
        , (
            lowerUTF8(ifNull(facts.latest_job_fact_source_platform_guess, '')) = 'ashby'
            and (
                empty(trim(replaceRegexpAll(
                    ifNull(facts.job_description, '')
                    , '<[^>]*>'
                    , ''
                )))
                or match(
                    trim(replaceRegexpAll(
                        ifNull(facts.job_description, '')
                        , '<[^>]*>'
                        , ''
                    ))
                    , '^https?://[^[:space:]]+$'
                )
            )
        ) as has_ashby_missing_description
        , (
            facts.latest_lifecycle_status = 'CLOSED'
            or (
                facts.latest_lifecycle_status = 'TERMINAL'
                and facts.previous_lifecycle_status = 'TERMINAL'
            )
        ) as has_confirmed_lifecycle_closure
    from candidate_facts as facts
    left join review_decisions as review
        on facts.candidate_id = review.candidate_id
),

final as (
    select
        evaluated.*
        , case
            when not meets_content_publication_requirements then 'NOT_PUBLISHABLE'
            when has_confirmed_lifecycle_closure then 'CLOSED'
            when has_ashby_missing_description then 'NOT_PUBLISHABLE'
            when has_expired_valid_through then 'NOT_PUBLISHABLE'
            when publication_review_status in ('pending', 'held') then 'NOT_PUBLISHABLE'
            else 'PUBLISHABLE'
        end as publication_status
        , case
            when latest_job_posting_type != 'JOB' or latest_job_posting_type is null
                then 'NOT_JOB'
            when latest_remote_scope not in ('REMOTE', 'HYBRID', 'ONSITE')
                or latest_remote_scope is null
                then 'WORK_ARRANGEMENT'
            when validated_country_eligibility_scope
                not in ('GLOBAL', 'GLOBAL_EXCEPT', 'SPECIFIC')
                or validated_country_eligibility_scope is null
                then 'COUNTRY_ELIGIBILITY_SCOPE'
            when validated_country_eligibility_scope = 'SPECIFIC'
                and length(eligible_country_codes) = 0
                then 'COUNTRY_ELIGIBILITY_VALUES'
            when notEmpty(ifNull(title, '')) = 0 then 'MISSING_TITLE'
            when latest_job_fact_declared_language_tag is not null
                and not startsWith(latest_job_fact_declared_language_tag, 'en')
                then 'UNSUPPORTED_LANGUAGE'
            when latest_lifecycle_status = 'CLOSED'
                then 'LIFECYCLE_CLOSED'
            when latest_lifecycle_status = 'TERMINAL'
                and previous_lifecycle_status = 'TERMINAL'
                then 'LIFECYCLE_TERMINAL_CONFIRMED'
            when has_ashby_missing_description
                then 'ASHBY_MISSING_DESCRIPTION'
            when has_expired_valid_through then 'EXPIRED_VALID_THROUGH'
            when publication_review_status in ('pending', 'held')
                then 'PUBLICATION_REVIEW_HELD'
            else 'PUBLICATION_READY'
        end as publication_status_reason
    from evaluated
)

select *
from final
