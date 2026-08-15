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

country_group_aliases as (
    select distinct
        country_group_code
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(alias), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
    from {{ ref('wremotely__country_group_aliases') }}
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

combined as (
    select * from country_matches
    union all
    select * from country_group_matches
    union all
    select * from reviewed_location_matches
)

select
    concat(
        evidence_id
        , '|', ifNull(matched_country_code, '')
        , '|', ifNull(matched_country_group_code, '')
        , '|', match_source
    ) as match_id
    , *
from combined
