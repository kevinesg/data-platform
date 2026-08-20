{{ config(
    materialized='table',
    on_schema_change='append_new_columns',
    order_by="(ifNull(source_landing_run_id, ''), ifNull(evidence_id, ''), ifNull(match_id, ''))",
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_sort': 268435456,
        'max_memory_usage': 1073741824
    }
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_landing_run_id']) %}

with match_inputs as (
    select *
    from {{ ref('int_wremotely__country_eligibility_match_inputs') }}
    where is_restricting_location_evidence
    {% if incremental_watermark_ready %}
    and source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
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
)

select
    concat(e.evidence_id, '|', r.country_code, '||REVIEWED_LOCATION_COUNTRY_ALIAS') as match_id
    , e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , r.country_code as matched_country_code
    , nullIf('', '') as matched_country_group_code
    , 'REVIEWED_LOCATION_COUNTRY_ALIAS' as match_source
from match_inputs as e
inner join reviewed_location_aliases as r
    on e.normalized_raw_value = r.alias_search_text
    and (
        r.source_platform_guess is null
        or r.source_platform_guess = lowerUTF8(ifNull(e.source_platform_guess, ''))
    )
where r.alias_search_text is not null
group by
    e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , r.country_code
