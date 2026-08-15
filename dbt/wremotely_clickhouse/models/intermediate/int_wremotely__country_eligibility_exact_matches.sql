{{ config(materialized='table') }}

with country_aliases as (
    select distinct
        country_code
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(alias), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
        , match_kind
    from {{ ref('wremotely__country_aliases') }}

    union distinct

    select distinct
        country_code
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(alias), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
        , match_kind
    from {{ ref('wremotely__country_cldr_aliases') }}

    union distinct

    select
        country_code
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(country_name), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
        , 'phrase' as match_kind
    from {{ ref('wremotely__countries') }}

    union distinct

    select
        country_code
        , nullIf(lowerUTF8(country_code), '') as alias_search_text
        , 'exact_code' as match_kind
    from {{ ref('wremotely__countries') }}

    union distinct

    select
        country_code
        , nullIf(lowerUTF8(alpha_3_code), '') as alias_search_text
        , 'exact_code' as match_kind
    from {{ ref('wremotely__countries') }}
),

country_text_aliases as (
    select
        alias_search_text
        , any(country_code) as matched_country_code
        , arrayElement(splitByChar(' ', ifNull(alias_search_text, '')), 1) as first_token
    from country_aliases
    where match_kind = 'phrase'
        and alias_search_text is not null
    group by alias_search_text
    having uniqExact(country_code) = 1
),

country_group_aliases as (
    select distinct
        country_group_code
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(alias), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
    from {{ ref('wremotely__country_group_aliases') }}
),

country_group_text_aliases as (
    select
        alias_search_text
        , any(country_group_code) as matched_country_group_code
        , arrayElement(splitByChar(' ', ifNull(alias_search_text, '')), 1) as first_token
    from country_group_aliases
    where alias_search_text is not null
    group by alias_search_text
    having uniqExact(country_group_code) = 1
),

reviewed_location_aliases as (
    select distinct
        country_code
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(location_alias), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
        , nullIf(lowerUTF8(source_platform_guess), '') as source_platform_guess
    from {{ ref('wremotely__location_country_aliases') }}
),

subdivision_aliases as (
    select distinct
        country_code
        , subdivision_code
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(subdivision_name), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
    from {{ ref('wremotely__country_subdivisions') }}

    union distinct

    select distinct
        country_code
        , subdivision_code
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(subdivision_code), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
    from {{ ref('wremotely__country_subdivisions') }}
),

subdivision_text_aliases as (
    select
        alias_search_text
        , any(country_code) as matched_country_code
        , arrayElement(splitByChar(' ', ifNull(alias_search_text, '')), 1) as first_token
    from subdivision_aliases
    where alias_search_text is not null
    group by alias_search_text
    having uniqExact(country_code) = 1
),

country_matches as (
    select distinct
        e.evidence_id
        , e.candidate_id
        , e.evidence_direction
        , c.country_code as matched_country_code
        , nullIf('', '') as matched_country_group_code
        , case
            when c.match_kind = 'exact_code' then 'ATOMIC_COUNTRY_ALIAS'
            else 'COUNTRY_TEXT_ALIAS'
        end as match_source
    from {{ ref('int_wremotely__country_eligibility_inputs') }} as e
    inner join country_aliases as c
        on e.normalized_raw_value = c.alias_search_text
    where e.evidence_direction in ('INCLUDED', 'EXCLUDED')
        and c.alias_search_text is not null
),

country_text_matches as (
    select distinct
        e.evidence_id
        , e.candidate_id
        , e.evidence_direction
        , c.matched_country_code as matched_country_code
        , nullIf('', '') as matched_country_group_code
        , 'COUNTRY_TEXT_ALIAS' as match_source
    from {{ ref('int_wremotely__country_eligibility_inputs') }} as e
    inner join country_text_aliases as c
        on e.country_match_mode = 'TEXT'
        and has(splitByChar(' ', ifNull(e.normalized_raw_value, '')), c.first_token)
        and position(
            concat(' ', e.normalized_raw_value, ' ')
            , concat(' ', c.alias_search_text, ' ')
        ) > 0
    where e.evidence_direction in ('INCLUDED', 'EXCLUDED')
),

country_group_matches as (
    select distinct
        e.evidence_id
        , e.candidate_id
        , e.evidence_direction
        , nullIf('', '') as matched_country_code
        , g.country_group_code as matched_country_group_code
        , 'ATOMIC_COUNTRY_GROUP_ALIAS' as match_source
    from {{ ref('int_wremotely__country_eligibility_inputs') }} as e
    inner join country_group_aliases as g
        on e.normalized_raw_value = g.alias_search_text
    where e.evidence_direction in ('INCLUDED', 'EXCLUDED')
        and e.country_match_mode = 'ATOMIC'
        and g.alias_search_text is not null
),

country_group_text_matches as (
    select distinct
        e.evidence_id
        , e.candidate_id
        , e.evidence_direction
        , nullIf('', '') as matched_country_code
        , g.matched_country_group_code as matched_country_group_code
        , 'COUNTRY_GROUP_TEXT_ALIAS' as match_source
    from {{ ref('int_wremotely__country_eligibility_inputs') }} as e
    inner join country_group_text_aliases as g
        on e.country_match_mode = 'TEXT'
        and has(splitByChar(' ', ifNull(e.normalized_raw_value, '')), g.first_token)
        and position(
            concat(' ', e.normalized_raw_value, ' ')
            , concat(' ', g.alias_search_text, ' ')
        ) > 0
    where e.evidence_direction in ('INCLUDED', 'EXCLUDED')
),

reviewed_location_matches as (
    select distinct
        e.evidence_id
        , e.candidate_id
        , e.evidence_direction
        , r.country_code as matched_country_code
        , nullIf('', '') as matched_country_group_code
        , 'REVIEWED_LOCATION_COUNTRY_ALIAS' as match_source
    from {{ ref('int_wremotely__country_eligibility_inputs') }} as e
    inner join reviewed_location_aliases as r
        on e.normalized_raw_value = r.alias_search_text
        and (
            r.source_platform_guess is null
            or r.source_platform_guess = lowerUTF8(ifNull(e.source_platform_guess, ''))
        )
    where e.evidence_direction in ('INCLUDED', 'EXCLUDED')
        and e.is_restricting_location_evidence
        and r.alias_search_text is not null
),

subdivision_matches as (
    select distinct
        e.evidence_id
        , e.candidate_id
        , e.evidence_direction
        , s.country_code as matched_country_code
        , nullIf('', '') as matched_country_group_code
        , case
            when e.country_match_mode = 'ATOMIC' then 'ATOMIC_COUNTRY_SUBDIVISION_ALIAS'
            else 'COUNTRY_SUBDIVISION_TEXT_ALIAS'
        end as match_source
    from {{ ref('int_wremotely__country_eligibility_inputs') }} as e
    inner join subdivision_aliases as s
        on e.normalized_raw_value = s.alias_search_text
    where e.evidence_direction in ('INCLUDED', 'EXCLUDED')
        and e.is_restricting_location_evidence
        and s.alias_search_text is not null
),

subdivision_text_matches as (
    select distinct
        e.evidence_id
        , e.candidate_id
        , e.evidence_direction
        , s.matched_country_code as matched_country_code
        , nullIf('', '') as matched_country_group_code
        , 'COUNTRY_SUBDIVISION_TEXT_ALIAS' as match_source
    from {{ ref('int_wremotely__country_eligibility_inputs') }} as e
    inner join subdivision_text_aliases as s
        on e.country_match_mode = 'TEXT'
        and has(splitByChar(' ', ifNull(e.normalized_raw_value, '')), s.first_token)
        and position(
            concat(' ', e.normalized_raw_value, ' ')
            , concat(' ', s.alias_search_text, ' ')
        ) > 0
    where e.evidence_direction in ('INCLUDED', 'EXCLUDED')
),

combined_raw as (
    select * from country_matches
    union all
    select * from country_text_matches
    union all
    select * from country_group_matches
    union all
    select * from country_group_text_matches
    union all
    select * from reviewed_location_matches
    union all
    select * from subdivision_matches
    union all
    select * from subdivision_text_matches
),

combined as (
    select distinct
        evidence_id
        , candidate_id
        , evidence_direction
        , matched_country_code
        , matched_country_group_code
        , match_source
    from combined_raw
),

country_match_counts as (
    select
        evidence_id
        , uniqExactIf(
            matched_country_code
            , notEmpty(ifNull(matched_country_code, ''))
        ) as matched_country_count
    from combined
    group by evidence_id
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
        evidence_id
        , '|', ifNull(matched_country_code, '')
        , '|', ifNull(matched_country_group_code, '')
        , '|', match_source
    ) as match_id
    , *
from annotated
