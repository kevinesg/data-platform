{{ config(
    materialized='view',
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_group_by': 268435456,
        'max_bytes_before_external_sort': 268435456,
        'max_memory_usage': 1073741824
    }
) }}

with combined as (
    select
        evidence_id
        , source_landing_run_id
        , candidate_id
        , evidence_direction
        , matched_country_code
        , matched_country_group_code
        , match_source
    from {{ ref('int_wremotely__country_eligibility_combined_matches') }}
),

country_match_counts as (
    select
        evidence_id
        , matched_country_count
    from {{ ref('int_wremotely__country_eligibility_match_counts') }}
),

annotated as (
    select
        c.*
        , if(
            ifNull(m.matched_country_count, 0) > 1
            , 'AMBIGUOUS_COUNTRY_ALIAS'
            , 'MATCHED'
        ) as match_status
    from combined as c
    left join country_match_counts as m
        on c.evidence_id = m.evidence_id
)

select
    concat(
        annotated.evidence_id
        , '|', ifNull(annotated.matched_country_code, '')
        , '|', ifNull(annotated.matched_country_group_code, '')
        , '|', annotated.match_source
    ) as match_id
    , annotated.evidence_id
    , annotated.source_landing_run_id
    , annotated.candidate_id
    , annotated.evidence_direction
    , if(
        annotated.match_status = 'AMBIGUOUS_COUNTRY_ALIAS'
        , nullIf('', '')
        , annotated.matched_country_code
    ) as matched_country_code
    , if(
        annotated.match_status = 'AMBIGUOUS_COUNTRY_ALIAS'
        , nullIf('', '')
        , annotated.matched_country_group_code
    ) as matched_country_group_code
    , annotated.match_source
    , annotated.match_status
from annotated
limit 1 by match_id
