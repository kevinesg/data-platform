{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='match_id',
    on_schema_change='append_new_columns',
    order_by="(ifNull(source_landing_run_id, ''), ifNull(evidence_id, ''), ifNull(match_id, ''))",
    query_settings={
        'max_threads': 2,
        'max_bytes_before_external_sort': 268435456,
        'max_bytes_before_external_group_by': 268435456,
        'max_memory_usage': 1073741824
    }
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_landing_run_id']) %}

with combined_raw as (
    select * from {{ ref('int_wremotely__country_eligibility_atomic_matches') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}

    union all

    select * from {{ ref('int_wremotely__country_eligibility_country_text_matches') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}

    union all

    select * from {{ ref('int_wremotely__country_eligibility_country_group_text_matches') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}

    union all

    select * from {{ ref('int_wremotely__country_eligibility_subdivision_text_matches') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
),

combined as (
    select distinct
        evidence_id
        , source_landing_run_id
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
