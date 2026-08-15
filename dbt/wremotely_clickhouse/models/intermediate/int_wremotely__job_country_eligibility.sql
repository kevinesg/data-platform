{{ config(materialized='table') }}

with candidate_country_eligibility as (
    select *
    from {{ ref('int_wremotely__candidate_country_eligibility') }}
),

explicit_eligible_rows as (
    select
        candidate_id as job_id
        , arrayJoin(eligible_country_codes) as country_code
        , 'ELIGIBLE' as eligibility_status
        , validated_country_eligibility_scope as country_eligibility_scope
    from candidate_country_eligibility
),

explicit_excluded_rows as (
    select
        candidate_id as job_id
        , arrayJoin(excluded_country_codes) as country_code
        , 'EXCLUDED' as eligibility_status
        , validated_country_eligibility_scope as country_eligibility_scope
    from candidate_country_eligibility
)

select *
from explicit_eligible_rows

union all

select *
from explicit_excluded_rows
