{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='match_id',
    on_schema_change='append_new_columns',
    order_by="(ifNull(source_landing_run_id, ''), ifNull(evidence_id, ''), ifNull(match_id, ''))",
    query_settings={
        'max_threads': 1,
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
)

select
    match_id
    , evidence_id
    , latest_source_landing_run_id as source_landing_run_id
    , candidate_id
    , evidence_direction
    , matched_country_code
    , matched_country_group_code
    , match_source
from (
    select
        match_id
        , any(evidence_id) as evidence_id
        , max(source_landing_run_id) as latest_source_landing_run_id
        , argMax(candidate_id, source_landing_run_id) as candidate_id
        , argMax(evidence_direction, source_landing_run_id) as evidence_direction
        , any(matched_country_code) as matched_country_code
        , any(matched_country_group_code) as matched_country_group_code
        , any(match_source) as match_source
    from combined_raw
    group by match_id
)
