{{ config(
    materialized='table',
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

with combined as (
    select * from {{ ref('int_wremotely__country_eligibility_atomic_country_matches') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}

    union all

    select * from {{ ref('int_wremotely__country_eligibility_atomic_country_group_matches') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}

    union all

    select * from {{ ref('int_wremotely__country_eligibility_atomic_reviewed_location_matches') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}

    union all

    select * from {{ ref('int_wremotely__country_eligibility_atomic_subdivision_matches') }}
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
    , source_landing_run_id
    , candidate_id
    , evidence_direction
    , matched_country_code
    , matched_country_group_code
    , match_source
from combined
group by
    match_id
    , evidence_id
    , source_landing_run_id
    , candidate_id
    , evidence_direction
    , matched_country_code
    , matched_country_group_code
    , match_source
