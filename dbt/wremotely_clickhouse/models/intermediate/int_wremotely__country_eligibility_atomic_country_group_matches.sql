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
    where country_match_mode = 'ATOMIC'
    {% if incremental_watermark_ready %}
    and source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
),

country_group_aliases as (
    select distinct
        country_group_code
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(alias), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
    from {{ ref('wremotely__country_group_aliases') }}
)

select
    concat(e.evidence_id, '||', g.country_group_code, '|ATOMIC_COUNTRY_GROUP_ALIAS') as match_id
    , e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , nullIf('', '') as matched_country_code
    , g.country_group_code as matched_country_group_code
    , 'ATOMIC_COUNTRY_GROUP_ALIAS' as match_source
from match_inputs as e
inner join country_group_aliases as g
    on e.normalized_raw_value = g.alias_search_text
where g.alias_search_text is not null
group by
    e.evidence_id
    , e.source_landing_run_id
    , e.candidate_id
    , e.evidence_direction
    , g.country_group_code
