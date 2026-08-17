{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='match_id',
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
        , arrayJoin(
            arrayDistinct(arrayFilter(
                token -> token != ''
                , splitByChar(' ', ifNull(normalized_raw_value, ''))
            ))
        ) as input_first_token
    from {{ ref('int_wremotely__country_eligibility_match_inputs') }}
    where country_match_mode = 'TEXT'
        and is_restricting_location_evidence
    {% if incremental_watermark_ready %}
    and source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
),

subdivision_text_aliases as (
    select
        alias_search_text
        , any(country_code) as matched_country_code
        , arrayElement(splitByChar(' ', ifNull(alias_search_text, '')), 1) as first_token
    from (
        select distinct
            country_code
            , nullIf(
                trim(replaceRegexpAll(lowerUTF8(subdivision_name), '[^[:alnum:]]+', ' '))
                , ''
            ) as alias_search_text
        from {{ ref('wremotely__country_subdivisions') }}

        union distinct

        select distinct
            country_code
            , nullIf(
                trim(replaceRegexpAll(lowerUTF8(subdivision_code), '[^[:alnum:]]+', ' '))
                , ''
            ) as alias_search_text
        from {{ ref('wremotely__country_subdivisions') }}
    ) as aliases
    where alias_search_text is not null
    group by alias_search_text
    having uniqExact(country_code) = 1
)

select
    concat(e.evidence_id, '|', c.matched_country_code, '|COUNTRY_SUBDIVISION_TEXT_ALIAS') as match_id
    , e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , c.matched_country_code
    , nullIf('', '') as matched_country_group_code
    , 'COUNTRY_SUBDIVISION_TEXT_ALIAS' as match_source
from match_inputs as e
inner join subdivision_text_aliases as c
    on e.input_first_token = c.first_token
    and position(
        concat(' ', e.normalized_raw_value, ' ')
        , concat(' ', c.alias_search_text, ' ')
    ) > 0
group by
    e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , c.matched_country_code
