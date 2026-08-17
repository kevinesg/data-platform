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
    from {{ ref('int_wremotely__country_eligibility_match_inputs') }}
    where is_restricting_location_evidence
    {% if incremental_watermark_ready %}
    and source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
),

subdivision_aliases as (
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
)

select
    concat(
        e.evidence_id
        , '|', s.country_code
        , '||'
        , case
            when e.country_match_mode = 'ATOMIC' then 'ATOMIC_COUNTRY_SUBDIVISION_ALIAS'
            else 'COUNTRY_SUBDIVISION_TEXT_ALIAS'
        end
    ) as match_id
    , e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , s.country_code as matched_country_code
    , nullIf('', '') as matched_country_group_code
    , case
        when e.country_match_mode = 'ATOMIC' then 'ATOMIC_COUNTRY_SUBDIVISION_ALIAS'
        else 'COUNTRY_SUBDIVISION_TEXT_ALIAS'
    end as match_source
from match_inputs as e
inner join subdivision_aliases as s
    on e.normalized_raw_value = s.alias_search_text
where s.alias_search_text is not null
group by
    e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , e.country_match_mode
    , s.country_code
