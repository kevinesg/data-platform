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
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
),

country_aliases as (
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
)

select
    concat(
        e.evidence_id
        , '|', ifNull(c.country_code, '')
        , '|', ''
        , '|'
        , case
            when c.match_kind = 'exact_code' then 'ATOMIC_COUNTRY_ALIAS'
            else 'COUNTRY_TEXT_ALIAS'
        end
    ) as match_id
    , e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , c.country_code as matched_country_code
    , nullIf('', '') as matched_country_group_code
    , case
        when c.match_kind = 'exact_code' then 'ATOMIC_COUNTRY_ALIAS'
        else 'COUNTRY_TEXT_ALIAS'
    end as match_source
from match_inputs as e
inner join country_aliases as c
    on e.normalized_raw_value = c.alias_search_text
where c.alias_search_text is not null
group by
    e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , c.country_code
    , c.match_kind
