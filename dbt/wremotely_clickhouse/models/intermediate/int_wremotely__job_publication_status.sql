{{ config(materialized='table') }}

with candidate_facts as (
    select *
    from {{ ref('int_wremotely__current_candidate_facts') }}
),

evaluated as (
    select
        candidate_facts.*
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
            facts.latest_lifecycle_status = 'CLOSED'
            or (
                facts.latest_lifecycle_status = 'TERMINAL'
                and facts.previous_lifecycle_status = 'TERMINAL'
            )
        ) as has_confirmed_lifecycle_closure
    from candidate_facts as facts
),

final as (
    select
        evaluated.*
        , case
            when not meets_content_publication_requirements then 'NOT_PUBLISHABLE'
            when has_confirmed_lifecycle_closure then 'CLOSED'
            when has_expired_valid_through then 'NOT_PUBLISHABLE'
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
            when has_expired_valid_through then 'EXPIRED_VALID_THROUGH'
            else 'PUBLICATION_READY'
        end as publication_status_reason
    from evaluated
)

select *
from final
